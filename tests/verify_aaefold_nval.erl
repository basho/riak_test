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

-module(verify_aaefold_nval).
-export([confirm/0, verify_aae_fold/1]).
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
    Nodes0 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD),
    ok = verify_aae_fold(Nodes0),
    pass.


verify_aae_fold(Nodes) ->
    
    {ok, CH} = riak:client_connect(hd(Nodes)),
    {ok, CT} = riak:client_connect(lists:last(Nodes)),

    lager:info("Fold for empty root"),
    {ok, RH0} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    {ok, RT0} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CT),

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

    lager:info("Fold for busy root"),
    {ok, RH1} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    {ok, RT1} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CT),
    
    ?assertMatch(true, RH1 == RT1),
    ?assertMatch(true, RH0 == RT0),
    ?assertMatch(false, RH0 == RH1),
    
    lager:info("Make ~w changes", [?DELTA_COUNT]),
    Changes2 = test_data(1, ?DELTA_COUNT, list_to_binary("U2")),
    ok = write_data(hd(Nodes), Changes2),
    {ok, RH2} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    DirtyBranches2 = aae_exchange:compare_roots(RH1, RH2),

    lager:info("Found ~w branch deltas", [length(DirtyBranches2)]),
    ?assertMatch(true, length(DirtyBranches2) > 0),
    ?assertMatch(true, length(DirtyBranches2) =< ?DELTA_COUNT),

    {ok, BH2} =
        riak_client:aae_fold({merge_branch_nval, ?N_VAL, DirtyBranches2}, CH),

    lager:info("Make ~w changes to same keys", [?DELTA_COUNT]),
    Changes3 = test_data(1, ?DELTA_COUNT, list_to_binary("U3")),
    ok = write_data(hd(Nodes), Changes3),
    {ok, RH3} = riak_client:aae_fold({merge_root_nval, ?N_VAL}, CH),
    DirtyBranches3 = aae_exchange:compare_roots(RH2, RH3),

    lager:info("Found ~w branch deltas", [length(DirtyBranches3)]),
    ?assertMatch(true, DirtyBranches2 == DirtyBranches3),

    {ok, BH3} =
        riak_client:aae_fold({merge_branch_nval, ?N_VAL, DirtyBranches3}, CH),
    
    DirtySegments1 = aae_exchange:compare_branches(BH2, BH3),
    lager:info("Found ~w mismatched segments", [length(DirtySegments1)]),
    ?assertMatch(true, length(DirtySegments1) > 0),
    ?assertMatch(true, length(DirtySegments1) =< ?DELTA_COUNT),

    {ok, KCL1} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),

    lager:info("Found ~w mismatched keys", [length(KCL1)]),

    ?assertMatch(true, length(KCL1) >= ?DELTA_COUNT),
    MappedKCL1 = lists:map(fun({B, K, VC}) -> {{B, K}, VC} end, KCL1),

    lager:info("Checking all mismatched keys in result"),
    MatchFun = 
        fun(I) ->
            K = to_key(I),
            InFetchClocks = lists:keyfind({?BUCKET, K}, 1, MappedKCL1),
            ?assertMatch(true, {?BUCKET, K} == element(1, InFetchClocks))
        end,
    lists:foreach(MatchFun, lists:seq(1, ?DELTA_COUNT)),
    
    lager:info("Stopping a node - query results should be unchanged"),
    rt:stop_and_wait(hd(tl(Nodes))),
    {ok, BH4} =
        riak_client:aae_fold({merge_branch_nval, ?N_VAL, DirtyBranches3}, CH),
    ?assertMatch(true, BH3 == BH4),
    {ok, KCL2} =
        riak_client:aae_fold({fetch_clocks_nval, ?N_VAL, DirtySegments1}, CH),
    ?assertMatch(true, lists:sort(KCL1) == lists:sort(KCL2)).


to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

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

