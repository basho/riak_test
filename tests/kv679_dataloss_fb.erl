%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
%%% @copyright (C) 2014, Basho Technologies
%%% @doc
%%% riak_test for kv679 lost clock flavour.
%%%
%%% issue kv679 is a possible dataloss issue, it's basically caused by
%%% the fact that per key logical clocks can go backwards in time in
%%% certain situations. The situation under test here is as follows:
%%%
%% A coords a write to K [{a, 1}] and replicates to fallbacks D, E
%% A coords a write to K [{a, 2}] and replicates to primaries B, C
%% A coords a write K [{a, 3}] and replicates to primaries B, C
%% A loses it's clock for K (so far this is like the lost clock case above)
%% Read of A, D, E read repairs A with K=[{a, 1}]
%% A coords a write, issues [{a, 2}] again
%% Acked write is lost
%%%
%%%
%%% @end

-module(kv679_dataloss_fb).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"kv679">>).
-define(KEY, <<"test">>).

confirm() ->
    Conf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [{allow_mult, true},
                                                 {dvv_enabled, true},
                                                 {ring_creation_size, 8},
                                                 {vnode_management_timer, 1000},
                                                 {handoff_concurrency, 100},
                                                 {vnode_inactivity_timeout, 1000}]}]},
            {bitcask, [{sync_strategy, o_sync}, {io_mode, nif}]}],

    %% 5 'cos I want a perfect preflist when 2 primaries are down.
    %% i.e. I want to kill the fallbacks before they can handoff
    %% without effecting the primaries
    Nodes = rt:build_cluster(6, Conf),

    Clients =  kv679_tombstone:create_pb_clients(Nodes),

    %% Get preflist for key
    PL = kv679_tombstone:get_preflist(hd(Nodes)),

    ?assert(kv679_tombstone2:perfect_preflist(PL)),

    lager:info("Got preflist"),

    {CoordNode, _}=CoordClient = kv679_tombstone:coordinating_client(Clients, PL),

    OtherPrimaries = [Node || {{_Idx, Node}, Type} <- PL,
                              Type == primary,
                              Node /= CoordNode],

    [rt:stop_and_wait(N) || N <- OtherPrimaries],

    lager:info("Killed 2 primaries"),

    rt:wait_until(fun() ->
                          NewPL = kv679_tombstone:get_preflist(CoordNode),
                          two_fallbacks_one_primary(NewPL) == {1, 2}
                  end),

    FBPL = kv679_tombstone:get_preflist(CoordNode),

    lager:info("Got a preflist with coord and 2 fbs ~p~n", [FBPL]),

    %% Write key twice at C1
    kv679_tombstone:write_key(CoordClient, [<<"bob">>, <<"jim">>]),

    kv679_tombstone2:dump_clock(CoordClient),


    lager:info("Clock at 2 fallbacks"),

    %% Kill the fallbacks before they can handoff
    Fallbacks = [Node || {{_Idx, Node}, Type} <- FBPL,
                         Type == fallback],

    [rt:brutal_kill(FB) || FB <- Fallbacks],

    %% Bring back the primaries and do some more writes
    [rt:start_and_wait(P) || P <- OtherPrimaries],

    lager:info("started primaries back up"),

    rt:wait_until(fun() ->
                          NewPL = kv679_tombstone:get_preflist(CoordNode),
                          NewPL == PL
                  end),

    kv679_tombstone:write_key(CoordClient, [<<"jon">>, <<"joe">>]),

    kv679_tombstone2:dump_clock(CoordClient),

    %% Kill those primaries with there frontier clocks
    [rt:brutal_kill(P) || P <- OtherPrimaries],

    lager:info("killed primaries again"),

    %% delete the local data at the coordinator Key
    kv679_dataloss:delete_datadir(hd(PL)),

    %% Start up those fallbacks
    [rt:start_and_wait(F) || F <- Fallbacks],

    lager:info("restart fallbacks"),

    %% Wait for the fallback prefist
    rt:wait_until(fun() ->
                          NewPL = kv679_tombstone:get_preflist(CoordNode),
                          NewPL == FBPL
                  end),

    %% Read the key, read repair will mean that the data deleted vnode
    %% will have an old clock (gone back in time!)

    kv679_tombstone2:dump_clock(CoordClient),

    %% write a new value, this _should_ be a sibling of what is on
    %% crashed primaries
    kv679_tombstone:write_key(CoordClient, <<"anne">>),

    kv679_tombstone2:dump_clock(CoordClient),

    %% Time start up those primaries, let handoff happen, and see what
    %% happens to that last write

    [rt:start_and_wait(P) || P <- OtherPrimaries],

    lager:info("restart primaries _again_"),

     rt:wait_until(fun() ->
                          NewPL = kv679_tombstone:get_preflist(CoordNode),
                          NewPL == PL
                  end),

    lager:info("wait for handoffs"),

    [begin
         rpc:call(FB, riak_core_vnode_manager, force_handoffs, []),
         rt:wait_until_transfers_complete([FB])
     end || FB <- Fallbacks],

    lager:info("final get"),

    Res = kv679_tombstone:read_key(CoordClient),

    ?assertMatch({ok, _}, Res),
    {ok, O} = Res,

    ?assertEqual([<<"anne">>, <<"joe">>], riakc_obj:get_values(O)),

    pass.

two_fallbacks_one_primary(PL) ->
    lists:foldl(fun({{_, _}, primary}, {P, F}) ->
                        {P+1, F};
                   ({{_, _}, fallback}, {P, F}) ->
                        {P, F+1}
                end,
                {0, 0},
                PL).
