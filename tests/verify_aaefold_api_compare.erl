%% -------------------------------------------------------------------
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
%% @doc This overlaps with other aae_fold tests, but is intended as a
%% systematic change thta the HTTP and PB API return the same output.  This
%% characteristic is the useful when building replication features on the fold
%% API - making PB and HTTP inter-changeable

-module(verify_aaefold_api_compare).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 16).

-define(NUM_NODES, 4).
-define(NUM_KEYS_PERBATCH, 10000).
-define(BUCKET, <<"test_bucket">>).
-define(TYPE, <<"test">>).
-define(TYPED_BUCKET, <<"typed_bucket">>).
-define(N_VAL, 3).
-define(KV_CONFIG, {riak_kv,
                        [
                        % Speedy AAE configuration
                        {anti_entropy, {off, []}},
                        {tictacaae_active, active},
                        {tictacaae_parallelstore, leveled_ko},
                                % if backend not leveled will use parallel key-ordered
                                % store
                        {tictacaae_rebuildwait, 4},
                        {tictacaae_rebuilddelay, 3600},
                        {tictacaae_exchangetick, 30 * 1000}, % 5 seconds
                        {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
                        ]}).
-define(CORE_CONFIG, {riak_core,
                        [{default_bucket_props,
                            [
                                {n_val, ?N_VAL},
                                {allow_mult, true},
                                {dvv_enabled, true}
                            ]}]}).

confirm() ->
    TS0 = take_timestamp(),
    lager:info("Deploy some nodes"),
    Nodes = rt:build_cluster(?NUM_NODES, [], [?KV_CONFIG, ?CORE_CONFIG]),
    ClientH = rt:httpc(hd(Nodes)),
    ClientP = rt:pbc(hd(Nodes)),
    
    lager:info("Create a new typed bucket with allow_mult=false"),
    ok =
        rt:create_activate_and_wait_for_bucket_type(Nodes,
                                                    ?TYPE,
                                                    [{allow_mult, false}]),

    TS1 = take_timestamp(),
    lager:info("Compare on empty databases"),
    ok = verify_aae_compare({rhc, ClientH},
                            {riakc_pb_socket, ClientP},
                            convert_to_modified_range(TS0, TS1)),

    lager:info("Generate initial data"),
    KVL1 = test_data(1, ?NUM_KEYS_PERBATCH, <<"InitialV">>),

    lager:info("Load initial data"),
    ok = write_data(hd(Nodes), ?BUCKET, KVL1),
    ok = write_data(hd(Nodes), {?TYPE, ?TYPED_BUCKET}, KVL1),

    TS2 = take_timestamp(),
    lager:info("Compare on initial database"),
    ok = verify_aae_compare({rhc, ClientH},
                            {riakc_pb_socket, ClientP},
                            convert_to_modified_range(TS1, TS2)),

    pass.


verify_aae_compare({ModH, ClientH}, {ModP, ClientP}, {TSA, TSB}) ->

    lager:info("Find root"),
    {ok, RootH0} = ModH:aae_merge_root(ClientH, ?N_VAL),
    {ok, RootP0} = ModP:aae_merge_root(ClientP, ?N_VAL),
    ?assertMatch(RootH0, RootP0),

    lager:info("Find branches"),
    {ok, BranchesH0} =
        ModH:aae_merge_branches(ClientH, ?N_VAL, lists:seq(100, 128)),
    {ok, BranchesP0} =
        ModP:aae_merge_branches(ClientP, ?N_VAL, lists:seq(100, 128)),
    ?assertMatch(BranchesH0, BranchesP0),

    lager:info("Find clocks by segment"),
    {ok, SegsH0} =
        ModH:aae_fetch_clocks(ClientH, ?N_VAL, lists:seq(200, 300)),
    {ok, SegsP0} =
        ModP:aae_fetch_clocks(ClientP, ?N_VAL, lists:seq(200, 300)),
    ?assertMatch(SegsH0, SegsP0),

    lager:info("Compare range-based trees"),
    {ok, {tree, RawTreeH1}} =
        ModH:aae_range_tree(ClientH, ?BUCKET, all, small, all, all, pre_hash),
    {ok, {tree, RawTreeP1}} =
        ModP:aae_range_tree(ClientP, ?BUCKET, all, small, all, all, pre_hash),
    TreeH1 = leveled_tictac:import_tree(RawTreeH1),
    TreeP1 = leveled_tictac:import_tree(RawTreeP1),
    HPL = length(leveled_tictac:find_dirtyleaves(TreeH1, TreeP1)),
    
    ?assertMatch(0, HPL),

    {ok, {tree, RawTreeH2}} =
        ModH:aae_range_tree(ClientH,
                            {?TYPE, ?TYPED_BUCKET}, 
                            {to_key(?NUM_KEYS_PERBATCH div 5),
                                to_key(?NUM_KEYS_PERBATCH div 2)}, 
                            small,
                            {lists:seq(32, 128), xsmall},
                            {TSA, TSB}, 
                            {rehash, 42}),
    {ok, {tree, RawTreeP2}} =
        ModP:aae_range_tree(ClientP,
                            {?TYPE, ?TYPED_BUCKET}, 
                            {to_key(?NUM_KEYS_PERBATCH div 5),
                                to_key(?NUM_KEYS_PERBATCH div 2)}, 
                            small,
                            {lists:seq(32, 128), xsmall},
                            {TSA, TSB}, 
                            {rehash, 42}),
    TreeH2 = leveled_tictac:import_tree(RawTreeH2),
    TreeP2 = leveled_tictac:import_tree(RawTreeP2),
    ?assertMatch([], leveled_tictac:find_dirtyleaves(TreeH2, TreeP2)),

    lager:info("Compare clocks from range-based comparison"),
    {ok, ClockRangeH1} =
        ModH:aae_range_clocks(ClientH, ?BUCKET, all, all, all),
    {ok, ClockRangeP1} = 
        ModP:aae_range_clocks(ClientP, ?BUCKET, all, all, all),
    ?assertMatch(ClockRangeH1, ClockRangeP1),

    {ok, ClockRangeH2} =
        ModH:aae_range_clocks(ClientH,
                                {?TYPE, ?TYPED_BUCKET},
                                {to_key(?NUM_KEYS_PERBATCH div 5),
                                    to_key(?NUM_KEYS_PERBATCH div 2)}, 
                                {lists:seq(32, 128), xsmall},
                                {TSA, TSB}),
    {ok, ClockRangeP2} = 
        ModP:aae_range_clocks(ClientP,
                                {?TYPE, ?TYPED_BUCKET},
                                {to_key(?NUM_KEYS_PERBATCH div 5),
                                    to_key(?NUM_KEYS_PERBATCH div 2)}, 
                                {lists:seq(32, 128), xsmall},
                                {TSA, TSB}),
    ?assertMatch(ClockRangeH2, ClockRangeP2),

    lager:info("Find Keys"),
    {ok, {keys, SiblingCntsH0}} =
        ModH:aae_find_keys(ClientH, ?BUCKET, all, all,
                            {sibling_count, 1}),
    {ok, {keys, SiblingCntsP0}} =
        ModP:aae_find_keys(ClientP, ?BUCKET, all, all,
                            {sibling_count, 1}),
    ?assertMatch(SiblingCntsH0, SiblingCntsP0),

    {ok, {keys, SiblingCntsH1}} =
        ModH:aae_find_keys(ClientH,
                            {?TYPE, ?TYPED_BUCKET},
                            {to_key(?NUM_KEYS_PERBATCH div 5),
                                to_key(?NUM_KEYS_PERBATCH div 2)},
                            {TSA, TSB},
                            {sibling_count, 1}),
    {ok, {keys, SiblingCntsP1}} =
        ModP:aae_find_keys(ClientP,
                            {?TYPE, ?TYPED_BUCKET},
                            {to_key(?NUM_KEYS_PERBATCH div 5),
                                to_key(?NUM_KEYS_PERBATCH div 2)},
                            {TSA, TSB},
                            {sibling_count, 1}),
    ?assertMatch(SiblingCntsH1, SiblingCntsP1),

    lager:info("Object stats"),
    CompareFun =
        fun(Stats) ->
            fun({K, V}) ->
                R = 
                    case lists:keyfind(K, 1, Stats) of
                        {K, V} ->
                            true;
                        false ->
                            lager:info("Missing Key ~p - OK if empty list",
                                        [K]),
                            V == []
                    end,
                ?assertMatch(true, R)
            end
        end,
    
    {ok, {stats, ObjectStatsH0}} =
        ModH:aae_object_stats(ClientH, ?BUCKET, all, all),
    {ok, {stats, ObjectStatsP0}} =
        ModP:aae_object_stats(ClientP, ?BUCKET, all, all),
    lager:info("Object stats ~p", [ObjectStatsH0]),
    lists:foreach(CompareFun(ObjectStatsP0), ObjectStatsH0),

    {ok, {stats, ObjectStatsH1}} =
        ModH:aae_object_stats(ClientH, 
                                {?TYPE, ?TYPED_BUCKET},
                                {to_key(?NUM_KEYS_PERBATCH div 5),
                                    to_key(?NUM_KEYS_PERBATCH div 2)},
                                {TSA, TSB}),
    {ok, {stats, ObjectStatsP1}} =
        ModP:aae_object_stats(ClientP,
                                {?TYPE, ?TYPED_BUCKET},
                                {to_key(?NUM_KEYS_PERBATCH div 5),
                                    to_key(?NUM_KEYS_PERBATCH div 2)},
                                {TSA, TSB}),
    lists:foreach(CompareFun(ObjectStatsP1), ObjectStatsH1),

    ok.

convert_to_modified_range({StartMega, StartS, _}, {EndMega, EndS, _}) ->
    {StartMega * 1000000 + StartS, EndMega * 1000000 + EndS}.


to_key(N) ->
    list_to_binary(io_lib:format("K~6..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].

write_data(Node, Bucket, KVs) ->
    write_data(Node, Bucket, KVs, []).

write_data(Node, Bucket, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
         O =
             case riakc_pb_socket:get(PB, Bucket, K) of
                 {ok, Prev} ->
                     riakc_obj:update_value(Prev, V);
                 _ ->
                     riakc_obj:new(Bucket, K, V)
             end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    ok.

take_timestamp() ->
    lager:info("Taking timestamp"),
    TS = os:timestamp(),
    timer:sleep(1000),
    TS.