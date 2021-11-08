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
%% @doc Verification of AAE fold's find_keys and object stats
%% operational fold features

-module(verify_tictacrebuild_via_statsfold).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 64).
-define(CFG_TICTACAAE(ExchangeTick, RebuildTick, PoolStrategy),
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 60},
           {tictacaae_exchangetick, ExchangeTick}, 
           {tictacaae_rebuildtick, RebuildTick},
           {worker_pool_strategy, PoolStrategy}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE},
           {default_bucket_props, [{allow_mult, true}]}
          ]}]
       ).
-define(CFG_NOAAE(PoolStrategy), 
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {off, []}},
           {tictacaae_active, passive},
           {worker_pool_strategy, PoolStrategy}
          ]},
          {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE},
           {default_bucket_props, [{allow_mult, true}]}
          ]}]).

-define(NUM_KEYS_PERNODE, 5000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(DELTA_COUNT, 10).

confirm() ->

    Cluster =
        rt:deploy_nodes(5, ?CFG_TICTACAAE(60 * 60 * 1000,
                                            60 * 60 * 1000,
                                            dscp)),
        % Build a cluster with AAE - but don't have the AAE do exchanages
    [Node1, Node2, Node3, _Node4, _Node5] = Cluster,
    rt:set_advanced_conf(Node1, ?CFG_NOAAE(dscp)),
    rt:set_advanced_conf(Node2, ?CFG_NOAAE(single)),
    rt:set_advanced_conf(Node3, ?CFG_NOAAE(none)),
        % Change 1 node to not use Tictac AAE
    rt:join_cluster(Cluster),
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(Cluster),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end, Cluster),

    lager:info("Cluster started with one node missing AAE config"),

    HttpCH = rt:httpc(Node1),
    lager:info("Find Keys for no data "),
    {ok, {keys, ObjSize}} = rhc:aae_find_keys(HttpCH, ?BUCKET, all, all, {object_size, 1}),
    ?assertEqual([], ObjSize),

    lager:info("Commencing object load"),
    KeyLoadFun =
        fun(KeysPerNode) ->
            fun(Node, KeyCount) ->
                    KVs = test_data(KeyCount + 1,
                                    KeyCount + KeysPerNode,
                                    list_to_binary("V1")),
                    ok = write_data(Node, KVs),
                    KeyCount + KeysPerNode
            end
        end,

    TotalKeys = lists:foldl(KeyLoadFun(?NUM_KEYS_PERNODE), 0, Cluster),
    lager:info("Loaded ~w objects", [?NUM_KEYS_PERNODE * length(Cluster)]),
    
    lager:info("get stats"),
    {ok, {stats, Stats}} = rhc:aae_object_stats(HttpCH, ?BUCKET, all, all),
    FoldKeyCount = proplists:get_value(<<"total_count">>, Stats),

    lager:info("FoldKeyCount=~w TotalKeys=~w", [FoldKeyCount, TotalKeys]),
    ?assertMatch(true, FoldKeyCount < TotalKeys),

    lager:info("Changing config on Node 1 back to using AAE"),
    lager:info("Large exchange tick - we don't want to repair via exchange"),
    lager:info("Short rebuild tick - we do want to resolve via rebuild"),
    rt:set_advanced_conf(Node1,
                            ?CFG_TICTACAAE(60 * 60 * 1000, 60 * 1000, dscp)),
    rt:wait_for_service(Node1, riak_kv),
    rt:set_advanced_conf(Node2,
                            ?CFG_TICTACAAE(60 * 60 * 1000, 60 * 1000, single)),
    rt:wait_for_service(Node2, riak_kv),
    rt:set_advanced_conf(Node3,
                            ?CFG_TICTACAAE(60 * 60 * 1000, 60 * 1000, none)),
    rt:wait_for_service(Node3, riak_kv),
    lager:info("Wait until rebuild has caused correct result"),
    lager:info("This should be immediate for native backend"),

    RebuildCompleteFun = 
        fun() ->
            {ok, {stats, RebuildStats}} =
                rhc:aae_object_stats(HttpCH, ?BUCKET, all, all),
            RebuildFoldKeyCount =
                proplists:get_value(<<"total_count">>, RebuildStats),
            lager:info("RebuildFoldKeyCount=~w TotalKeys=~w",
                        [RebuildFoldKeyCount, TotalKeys]),
            RebuildFoldKeyCount == TotalKeys
        end,
    ok = rt:wait_until(RebuildCompleteFun, 8, 30000),

    {MegaB4, SecsB4, _} = os:timestamp(),
    SWbefore = MegaB4 * 1000000 + SecsB4,
    timer:sleep(1000),
    TotalModifiedKeys = lists:foldl(KeyLoadFun(1000), 0, Cluster),
    timer:sleep(1000),
    {MegaAft, SecsAft, _} = os:timestamp(),
    SWafter = MegaAft * 1000000 + SecsAft,

    {ok, {stats, ModifiedStats}} =
        rhc:aae_object_stats(HttpCH, ?BUCKET, all, {SWbefore, SWafter}),
    ModifiedKeyCount = proplists:get_value(<<"total_count">>, ModifiedStats),
    lager:info("ModifiedKeyCount=~w TotalModifiedKeys=~w",
                [ModifiedKeyCount, TotalModifiedKeys]),

    N1_AF4 = fetch_stats(af4pool_stats(), Node1),
    N2_AF4 = fetch_stats(af4pool_stats(), Node2),
    N3_AF4 = fetch_stats(af4pool_stats(), Node3),
    
    ?assertNotEqual(0, lists:min(N1_AF4)),
    ?assertEqual(0, lists:max(N2_AF4)),
    ?assertEqual(0, lists:max(N3_AF4)),

    N1_NWP = fetch_stats(nwpool_stats(), Node1),
    N2_NWP = fetch_stats(nwpool_stats(), Node2),
    N3_NWP = fetch_stats(nwpool_stats(), Node3),

    ?assertEqual(0, lists:max(N1_NWP)),
    ?assertNotEqual(0, lists:min(N2_NWP)),
    ?assertEqual(0, lists:max(N3_NWP)),

    ?assertMatch(TotalModifiedKeys, ModifiedKeyCount),

    pass.


fetch_stats(StatList, Node) ->
    Stats = verify_riak_stats:get_stats(Node),
    SL = lists:map(fun(S) -> proplists:get_value(S, Stats) end, StatList),
    lager:info("Stats pulled for ~p ~w - ~p", [StatList, Node, SL]),
    SL.

af4pool_stats() ->
    [<<"worker_af4_pool_queuetime_mean">>,
        <<"worker_af4_pool_queuetime_100">>,
        <<"worker_af4_pool_worktime_mean">>,
        <<"worker_af4_pool_worktime_100">>].

nwpool_stats() ->
    [<<"worker_node_worker_pool_queuetime_mean">>,
        <<"worker_node_worker_pool_queuetime_100">>,
        <<"worker_node_worker_pool_worktime_mean">>,
        <<"worker_node_worker_pool_worktime_100">>].

to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

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
