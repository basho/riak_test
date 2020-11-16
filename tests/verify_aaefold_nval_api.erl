%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
%% @doc Verification of AAE fold based on n_val (with cached trees)
%%
%% Confirm that trees are returned that vary along with the data in the
%% store

-module(verify_aaefold_nval_api).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 8).
-define(REBUILD_TICK, 30 * 1000).
-define(CFG_NOREBUILD,
        [{riak_kv,
          [
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 3600},
           {tictacaae_exchangetick, 5 * 1000}, % 5 seconds
           {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(CFG_REBUILD,
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 0},
           {tictacaae_rebuilddelay, 60},
           {tictacaae_exchangetick, 5 * 1000}, % 5 seconds
           {tictacaae_rebuildtick, ?REBUILD_TICK} % Check for rebuilds!
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(NUM_NODES, 3).
-define(NUM_KEYS_PERNODE, 10000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(DELTA_COUNT, 10).

confirm() ->
    lager:info("Testing without rebuilds - using http api"),
    Nodes0 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD),
    ClientHeadHTTP = rt:httpc(hd(Nodes0)),
    ClientTailHTTP = rt:httpc(lists:last(Nodes0)),
    ok = verify_aae_fold(Nodes0, rhc, ClientHeadHTTP, ClientTailHTTP),

    rt:clean_cluster(Nodes0),

    Nodes1 = rt:build_cluster(?NUM_NODES, ?CFG_REBUILD),
    lager:info("Sleeping for twice rebuild tick - testing with rebuilds ongoing"),
    lager:info("Testing this time with PB API"),
    timer:sleep(2 * ?REBUILD_TICK),
    ClientHeadPB = rt:pbc(hd(Nodes1)),
    ClientTailPB = rt:pbc(lists:last(Nodes1)),
    ok = verify_aae_fold(Nodes1, riakc_pb_socket, ClientHeadPB, ClientTailPB),

    pass.


verify_aae_fold(Nodes, Mod, CH, CT) ->

    lager:info("Fold for empty root"),
    {ok, {root, RH0}} = Mod:aae_merge_root(CH, ?N_VAL),
    {ok, {root, RT0}} = Mod:aae_merge_root(CT, ?N_VAL),

    lager:info("Commencing object load"),
    KeyLoadFun =
        fun(Node, KeyCount) ->
            KVs = test_data(KeyCount + 1,
                                KeyCount + ?NUM_KEYS_PERNODE,
                                list_to_binary("U1")),
            ok = write_data(Node, KVs),
            KeyCount + ?NUM_KEYS_PERNODE
        end,

    lists:foldl(KeyLoadFun, 1, Nodes),
    lager:info("Loaded ~w objects", [?NUM_KEYS_PERNODE * length(Nodes)]),
    wait_until_root_stable(Mod, CH),

    lager:info("Fold for busy root"),
    {ok, {root, RH1}} = Mod:aae_merge_root(CH, ?N_VAL),
    {ok, {root, RT1}} = Mod:aae_merge_root(CT, ?N_VAL),

    lager:info("Dirty segments ~w",
        [leveled_tictac:find_dirtysegments(RH1, RT1)]),
    
    ?assertMatch(true, RH1 == RT1),
    ?assertMatch(true, RH0 == RT0),
    ?assertMatch(false, RH0 == RH1),

    lager:info("Make ~w changes", [?DELTA_COUNT]),
    Changes2 = test_data(1, ?DELTA_COUNT, list_to_binary("U2")),
    ok = write_data(hd(Nodes), Changes2),
    {ok, {root, RH2}} = Mod:aae_merge_root(CH, ?N_VAL),

    DirtyBranches2 = aae_exchange:compare_roots(RH1, RH2),

    lager:info("Found branch deltas ~w", [DirtyBranches2]),
    
    ?assertMatch(true, length(DirtyBranches2) > 0),
    ?assertMatch(true, length(DirtyBranches2) =< ?DELTA_COUNT),

    {ok, {branches, BH2}} =
        Mod:aae_merge_branches(CH, ?N_VAL, DirtyBranches2),

    lager:info("Make ~w changes to same keys", [?DELTA_COUNT]),
    Changes3 = test_data(1, ?DELTA_COUNT, list_to_binary("U3")),
    ok = write_data(hd(Nodes), Changes3),
    {ok, {root, RH3}} = Mod:aae_merge_root(CH, ?N_VAL),

    DirtyBranches3 = aae_exchange:compare_roots(RH2, RH3),

    lager:info("Found ~w branch deltas", [length(DirtyBranches3)]),
    ?assertMatch(true, DirtyBranches2 == DirtyBranches3),

    {ok, {branches, BH3}} =
        Mod:aae_merge_branches(CH, ?N_VAL, DirtyBranches3),

    DirtySegments1 = aae_exchange:compare_branches(BH2, BH3),
    lager:info("Found ~w mismatched segments", [length(DirtySegments1)]),
    ?assertMatch(true, length(DirtySegments1) > 0),
    ?assertMatch(true, length(DirtySegments1) =< ?DELTA_COUNT),

    {ok, {keysclocks, KCL1}} =
        Mod:aae_fetch_clocks(CH, ?N_VAL, DirtySegments1),

    lager:info("Found ~w mismatched keys", [length(KCL1)]),

    ?assertMatch(true, length(KCL1) >= ?DELTA_COUNT),

    lager:info("Checking all mismatched keys in result"),
    MatchFun =
        fun(I) ->
                K = to_key(I),
                BK = {?BUCKET, K},
                InFetchClocks = lists:keyfind(BK, 1, KCL1),
                ?assertMatch({BK, Clock} when is_binary(Clock), InFetchClocks)
        end,
    lists:foreach(MatchFun, lists:seq(1, ?DELTA_COUNT)),

    lager:info("Stopping a node - query results should be unchanged"),
    rt:stop_and_wait(hd(tl(Nodes))),
    {ok, {branches, BH4}} =
        Mod:aae_merge_branches(CH, ?N_VAL, DirtyBranches3),

    ?assertMatch(true, BH3 == BH4),
    {ok, {keysclocks, KCL2}} =
        Mod:aae_fetch_clocks(CH, ?N_VAL, DirtySegments1),
    ?assertMatch(true, lists:sort(KCL1) == lists:sort(KCL2)),

    % Need to re-start or clean will fail
    rt:start_and_wait(hd(tl(Nodes))).


to_key(N) ->
    list_to_binary(io_lib:format("K~6..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].

write_data(Node, KVs) ->
    write_data(Node, KVs, []).

write_data(Node, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
         O =
         case riakc_pb_socket:get(PB, ?BUCKET, K) of
             {ok, Prev} ->
                 riakc_obj:update_value(Prev, V);
             _ ->
                 riakc_obj:new(?BUCKET, K, V)
         end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    timer:sleep(5000),
    ok.

wait_until_root_stable(Mod, Client) ->
    {ok, {root, RH0}} = Mod:aae_merge_root(Client, ?N_VAL),
    timer:sleep(2000),
    {ok, {root, RH1}} = Mod:aae_merge_root(Client, ?N_VAL),
    case aae_exchange:compare_roots(RH0, RH1) of
        [] ->
            lager:info("Root appears stable matched");
        [L] ->
            Pre = L * 4,
            <<_B0:Pre/binary, V0:32/integer, _Post0/binary>> = RH0,
            <<_B1:Pre/binary, V1:32/integer, _Post1/binary>> = RH1,
            lager:info("Root not stable: branch ~w compares ~w with ~w",
                        [L, V0, V1]),
            wait_until_root_stable(Mod, Client)
    end.