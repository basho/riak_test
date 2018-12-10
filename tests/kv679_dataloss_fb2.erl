%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%%% @copyright (C) 2017, Basho Technologies
%%% @doc
%%% riak_test for kv679 lost clock/fallback/handoff flavour.
%%%
%%% issue kv679 is a possible dataloss issue, it's basically caused by
%%% the fact that per key logical clocks can go backwards in time in
%%% certain situations. The situation under test here is as follows:
%%%
%% write at P1 replicates to F1 clock is at [{p1, 1}] at p1, f1
%% write twice more at P1, replciates to F2, clock is at [{p1, 3}] at p1, f2
%% write at p2 replicates to f3, clock is at [{p2, 1}] at p2, f3
%% handoff from f3->p1, during which the local read at p1 fails (disk error, whatever)
%% clock at p1,p2 is [{p2, 1}] f3 deletes after handoff (no more f3)
%% handoff from f1->p2 clock at p2 [{p1, 1}, {p2, 1}]
%% read repair from p2 -> p1, clock at p1 [{p1, 1}, {p2, 1}]
%% write on p1 replicate to p2, clock at [{p1, 2}, {p2, 1}] at p1, p2
%% handoff f2->p2, merge causes last write to be lost since entry {p1, 3} > {p1, 2}
%% read repair p2->p1 and the acked write is silently lost forever.
%%%
%%%
%% NOTE this failure occurs even in riak2.1 with actor-epochs, since
%% it is caused by a local-not-found on a non-coordinating write. EQC
%% found this issue, the counter example is below.
%%%
%%%
%% [{set,{var,1},{call,kv679_eqc,put,[p1,p2,<<"A">>,959494,undefined,false]}},
%%  {set,{var,3},{call,kv679_eqc,put,[p3,p2,<<"A">>,960608,{var,2},false]}},
%%  {set,{var,5},{call,kv679_eqc,put,[p1,f3,<<"A">>,960760,{var,4},false]}},
%%  {set,{var,6},{call,kv679_eqc,put,[p1,f1,<<"A">>,960851,{var,4},false]}},
%%  {set,{var,7},{call,kv679_eqc,forget,[p1,<<"A">>]}},
%%  {set,{var,8},{call,kv679_eqc,replicate,[<<"A">>,p3,p1]}},
%%  {set,{var,9},{call,kv679_eqc,replicate,[<<"A">>,f3,p1]}},
%%  {set,{var,11},{call,kv679_eqc,put,[p1,p2,<<"A">>,962942,{var,10},false]}}]
%%
%%%
%%%
%%% @end

-module(kv679_dataloss_fb2).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).
-import(kv679_dataloss_fb, [primary_and_fallback_counts/1]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"kv679">>).
-define(KEY, <<"test">>).
-define(NVAL, 2).

confirm() ->
    Conf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [
                                                 %% note reduced n_val
                                                 %% is to reduce
                                                 %% number of nodes
                                                 %% required for test
                                                 %% case and to
                                                 %% simplify case.
                                                 {n_val, ?NVAL},
                                                 {allow_mult, true},
                                                 {dvv_enabled, true},
                                                 {ring_creation_size, 8},
                                                 {vnode_management_timer, 1000},
                                                 {handoff_concurrency, 100},
                                                 {vnode_inactivity_timeout, 1000}]}]},
            {bitcask, [{sync_strategy, o_sync}, {io_mode, nif}]},
            {leveled, [{sync_strategy, riak_sync}]},
            {leveldb, [{sync_on_write, on}]}],

    KVBackend = proplists:get_value(backend, riak_test_runner:metadata()),

    Nodes = rt:build_cluster(5, Conf),
    Clients =  kv679_tombstone:create_pb_clients(Nodes),
    {_, TempPBC} = hd(Clients),
    rt:pbc_set_bucket_prop(TempPBC, ?BUCKET, [{n_val, ?NVAL}]),
    rt:wait_until_bucket_props(Nodes, ?BUCKET, [{n_val, ?NVAL}]),

    %% Get preflist for key
    PL = kv679_tombstone:get_preflist(hd(Nodes), ?NVAL),
    ?assert(kv679_tombstone2:perfect_preflist(PL, ?NVAL)),

    lager:info("Got preflist ~p", [PL]),

    {CoordNode, _}=CoordClient = kv679_tombstone:coordinating_client(Clients, PL),
    OtherPrimaries = [Node || {{_Idx, Node}, Type} <- PL,
                              Type == primary,
                              Node /= CoordNode],
    Fallbacks = [FB || FB <- Nodes, FB /= CoordNode andalso not lists:member(FB, OtherPrimaries)],

    ?assert(length(Fallbacks) == 3),

    lager:info("fallbacks ~p, primaries ~p~n", [Fallbacks, [CoordNode] ++ OtherPrimaries]),

    %% Partition the other primary and one non-preflist node
    {_, _, _P1, P2} = PartInfo = rt:partition([CoordNode] ++ tl(Fallbacks), OtherPrimaries ++ [hd(Fallbacks)]),
    lager:info("Partitioned ~p~n", [PartInfo]),

    rt:wait_until(fun() ->
                          NewPL = kv679_tombstone:get_preflist(CoordNode, ?NVAL),
                          lager:info("new PL ~p~n", [NewPL]),
                          primary_and_fallback_counts(NewPL) == {1, 1}
                  end),

    FBPL = kv679_tombstone:get_preflist(CoordNode, ?NVAL),
    lager:info("Got a preflist with coord and 1 fb ~p~n", [FBPL]),

    %% Write key once at coordinating primary
    kv679_tombstone:write_key(CoordClient, [<<"alice">>]),
    kv679_tombstone2:dump_clock(CoordClient),
    lager:info("Clock at fallback"),

    %% Kill the fallback before it can handoff
    [FB1] = [Node || {{_Idx, Node}, Type} <- FBPL,
                     Type == fallback],
    rt:brutal_kill(FB1),

    %% get a new preflist with a different fallback
    rt:wait_until(fun() ->
                          NewPL = kv679_tombstone:get_preflist(CoordNode, ?NVAL),
                          primary_and_fallback_counts(NewPL) == {1, 1} andalso NewPL /= FBPL
                  end),
    FBPL2 = kv679_tombstone:get_preflist(CoordNode, ?NVAL),
    lager:info("Got a preflist with coord and 1 fb ~p~n", [FBPL2]),
    ?assert(FBPL2 /= FBPL),

    %% do two more writes so that there exists out there a clock of
    %% {Primary1, 1} on the first fallback node and {Primary1, 3} on
    %% the second
    kv679_tombstone:write_key(CoordClient, [<<"bob">>, <<"charlie">>]),
    kv679_tombstone2:dump_clock(CoordClient),
    lager:info("Clock at fallback2"),

    %% Kill the fallback before it can handoff
    [FB2] = [Node || {{_Idx, Node}, Type} <- FBPL2,
                     Type == fallback],
    rt:brutal_kill(FB2),

    %% meanwhile, in the other partition, let's write some data
    P2PL = kv679_tombstone:get_preflist(hd(P2), ?NVAL),
    lager:info("partition 2 PL ~p", [P2PL]),
    [P2FB] = [Node || {{_Idx, Node}, Type} <- P2PL,
                      Type == fallback],
    [P2P] = [Node || {{_Idx, Node}, Type} <- P2PL,
                     Type == primary],
    P2Client = kv679_tombstone:coordinating_client(Clients, P2PL),
    kv679_tombstone:write_key(P2Client, [<<"dave">>]),
    kv679_tombstone2:dump_clock(P2Client),
    lager:info("Clock in Partition 2"),

    %% set up a local read error on Primary 1 (this is any disk error,
    %% delete the datadir, an intercept for a local crc failure or
    %% deserialisation error)
    add_intercept(CoordNode, KVBackend),

    %% heal the partition, remember the Partition 1 fallbacks aren't
    %% handing off yet (we killed them, but it could be any delay in
    %% handoff, not a failure, killing them is just a way to control
    %% timing (maybe should've used intercepts??))
    {ok, DownNodes} = rt:heal_upnodes(PartInfo),
    lager:info("These nodes still need healing ~p", [DownNodes]),

    %% wait for the partition 2 fallback to hand off
    rpc:call(P2FB, riak_core_vnode_manager, force_handoffs, []),
    rt:wait_until_node_handoffs_complete(P2FB),

    %% what's the clock on primary 1 now?  NOTE: this extra read was
    %% the only way I could ensure that the handoff had occured before
    %% cleaning out the intercept. A sleep also worked.
    kv679_tombstone2:dump_clock(CoordClient),
    clean_intercept(CoordNode, KVBackend),
    kv679_tombstone2:dump_clock(CoordClient),

    %% restart fallback one and wait for it to handoff
    rt:start_and_wait(FB1),
    lager:info("started fallback 1 back up"),
    rpc:call(FB1, riak_core_vnode_manager, force_handoffs, []),
    rt:wait_until_node_handoffs_complete(FB1),

    %% kill dev5 again (there is something very weird here, unless I
    %% do this dev3 _never_ hands off (and since n=2 it is never used
    %% in a preflist for read repair)) So the only way to get it's
    %% data onto CoordNode is to ensure a preflist of FB1 and
    %% CoordNode for a read! To be fair this rather ruins the test
    %% case. @TODO investigate this handoff problem
    rt:stop_and_wait(P2P),
    rt:stop_and_wait(P2FB),
    rt:wait_until(fun() ->
                          NewPL = kv679_tombstone:get_preflist(CoordNode, ?NVAL),
                          lager:info("new PL ~p~n", [NewPL]),
                          primary_and_fallback_counts(NewPL) == {1, 1} andalso
                              different_nodes(NewPL)
                  end),

    %% %% what's the clock on primary 1 now?
    kv679_tombstone2:dump_clock(CoordClient),

    %% restart P2P and P2FB
    rt:start_and_wait(P2P),
    rt:start_and_wait(P2FB),

    %% get a primary PL
    rt:wait_until(fun() ->
                          NewPL = kv679_tombstone:get_preflist(CoordNode, ?NVAL),
                          lager:info("new PL ~p~n", [NewPL]),
                          primary_and_fallback_counts(NewPL) == {2, 0} andalso
                              different_nodes(NewPL)
                  end),

    %% write a new value
    kv679_tombstone:write_key(CoordClient, [<<"emma">>]),
    kv679_tombstone2:dump_clock(CoordClient),

    %% finally start the last offline fallback, await all transfers,
    %% and do a read, the last write, `emma' should be present
    rt:start_and_wait(FB2),
    rpc:call(FB2, riak_core_vnode_manager, force_handoffs, []),
    rt:wait_until_transfers_complete(Nodes),

    lager:info("final get"),
    Res = kv679_tombstone:read_key(CoordClient),
    ?assertMatch({ok, _}, Res),
    {ok, O} = Res,

    %% A nice riak would have somehow managed to make a sibling of the
    %% last acked write, even with all the craziness
    ?assertEqual([<<"charlie">>, <<"emma">>], lists:sort(riakc_obj:get_values(O))),
    lager:info("Final Object ~p~n", [O]),
    pass.

different_nodes(PL) ->
    Nodes = [Node || {{_, Node}, _} <- PL],
    length(lists:usort(Nodes)) == length(PL).


add_intercept(Node, undefined) ->
    add_intercept(Node, bitcask);
add_intercept(Node, bitcask) ->
    rt_intercept:add(Node, {riak_kv_bitcask_backend,
                            [{{get, 3}, always_corrupt_get}]});
add_intercept(Node, leveled) ->
    rt_intercept:add(Node, {riak_kv_leveled_backend,
                            [{{get, 3}, always_corrupt_get}]});
add_intercept(Node, eleveldb) ->
    rt_intercept:add(Node, {riak_kv_eleveldb_backend,
                            [{{get, 3}, always_corrupt_get}]}).


clean_intercept(Node, undefined) ->
    clean_intercept(Node, bitcask);
clean_intercept(Node, bitcask) ->
    rt_intercept:clean(Node, riak_kv_bitcask_backend);
clean_intercept(Node, leveled) ->
    rt_intercept:clean(Node, riak_kv_leveled_backend);
clean_intercept(Node, eleveldb) ->
    rt_intercept:clean(Node, riak_kv_eleveldb_backend).