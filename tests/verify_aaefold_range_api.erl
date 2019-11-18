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
%% @doc Verification of AAE fold based on range (dynamic fold-based trees)
%%
%% Confirm that trees are returned that vary along with the data in the
%% store

-module(verify_aaefold_range_api).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 8).
-define(CFG_NOREBUILD,
        [{riak_kv,
          [
           % Speedy AAE configuration
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
    
    lager:info("Testing without rebuilds - using pb api"),
    Nodes1 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD),
    ClientHeadPB = rt:pbc(hd(Nodes1)),
    ClientTailPB = rt:pbc(lists:last(Nodes1)),

    ok = verify_aae_fold(Nodes1, riakc_pb_socket, ClientHeadPB, ClientTailPB),

    pass.


verify_aae_fold(Nodes, Mod, CH, CT) ->

    lager:info("Fold for empty tree range"),

    {ok, {tree, RH0Mochi}} =
        Mod:aae_range_tree(CH, ?BUCKET, all, small, all, all, pre_hash),
    {ok, {tree, RT0Mochi}} =
        Mod:aae_range_tree(CT, ?BUCKET, all, small, all, all, pre_hash),

    RH0 = leveled_tictac:import_tree(RH0Mochi),
    RT0 = leveled_tictac:import_tree(RT0Mochi),

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

    lager:info("Fold for busy tree"),
    {ok, {tree, RH1Mochi}} =
        Mod:aae_range_tree(CH, ?BUCKET, all, small, all, all, pre_hash),
    {ok, {tree, RT1Mochi}} =
        Mod:aae_range_tree(CT, ?BUCKET, all, small, all, all, pre_hash),

    RH1 = leveled_tictac:import_tree(RH1Mochi),
    RT1 = leveled_tictac:import_tree(RT1Mochi),

    ?assertEqual(RH1, RT1),
    ?assertEqual(RH0, RT0),
    ?assertNotEqual(RH0, RH1),

    ?assertEqual([], aae_exchange:compare_trees(RH1, RT1)),

    lager:info("Make ~w changes", [?DELTA_COUNT]),
    Changes2 = test_data(1, ?DELTA_COUNT, list_to_binary("U2")),
    ok = write_data(hd(Nodes), Changes2),

    {ok, {tree, RH2Mochi}} =
        Mod:aae_range_tree(CH, ?BUCKET, all, small, all, all, pre_hash),
    RH2 = leveled_tictac:import_tree(RH2Mochi),

    DirtySegments1 = aae_exchange:compare_trees(RH1, RH2),

    lager:info("Found ~w mismatched segments", [length(DirtySegments1)]),
    ?assertMatch(N when N >= ?DELTA_COUNT, length(DirtySegments1)),

    {ok, {keysclocks, KCL1}} =
        Mod:aae_range_clocks(CH, ?BUCKET, all, {DirtySegments1, small}, all),

    lager:info("Found ~w mismatched keys", [length(KCL1)]),

    ?assertMatch(N when N >= ?DELTA_COUNT, length(KCL1)),
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

    {ok, {keysclocks, KCL2}} =
        Mod:aae_range_clocks(CH, ?BUCKET, all, {DirtySegments1, small}, all),
    ?assertMatch(true, lists:sort(KCL1) == lists:sort(KCL2)),
    
    rt:start(hd(tl(Nodes))).


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
    ok.
