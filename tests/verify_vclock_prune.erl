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
%% @doc Verification aae folds and replication with pruning vclocks
%%

-module(verify_vclock_prune).
-export([confirm/0]).
-import(location, [plan_and_wait/2]).

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(SNK_WORKERS, 4).
-define(MAX_RESULTS, 512).

-define(CFG_REPL(SrcQueueDefns, NVal),
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
            {tictacaae_rebuildtick, 3600000},
            {replrtq_enablesrc, true},
            {replrtq_srcqueue, SrcQueueDefns},
            {log_readrepair, true},
            {read_repair_log, true}
          ]},
         {riak_core,
          [
            {ring_creation_size, ?DEFAULT_RING_SIZE},
            {vnode_management_timer, 2000},
            {vnode_inactivity_timeout, 4000},
            {handoff_concurrency, 16},
            {default_bucket_props,
                    [
                    {n_val, NVal},
                    {allow_mult, true},
                    {dvv_enabled, true}
                    ]}
          ]}]
       ).

-define(SNK_CONFIG(ClusterName, PeerList), 
       [{riak_kv, 
           [{replrtq_enablesink, true},
               {replrtq_sinkqueue, ClusterName},
               {replrtq_sinkpeers, PeerList},
               {replrtq_sinkworkers, ?SNK_WORKERS}]}]).

-define(FS_CONFIG(PeerIP, PeerPort, LocalClusterName, RemoteClusterName),
        [{riak_kv,
            [{ttaaefs_scope, all},
            {ttaaefs_localnval, 3},
            {ttaaefs_remotenval, 3},
            {ttaaefs_peerip, PeerIP},
            {ttaaefs_peerport, PeerPort},
            {ttaaefs_peerprotocol, pb},
            {ttaaefs_allcheck, 0},
            {ttaaefs_autocheck, 0},
            {ttaaefs_daycheck, 0},
            {ttaaefs_hourcheck, 0},
            {ttaaefs_nocheck, 24},
            {ttaaefs_maxresults, ?MAX_RESULTS},
            {ttaaefs_queuename, LocalClusterName},
            {ttaaefs_queuename_peer, RemoteClusterName},
            {ttaaefs_logrepairs, true}]}]).

-define(NUM_KEYS_PERNODE, 1000).
-define(BUCKET, {<<"test_type">>, <<"test_bucket">>}).

confirm() ->
    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {4, ?CFG_REPL("cluster_b:any", 3)},
            {2, ?CFG_REPL("cluster_a:any", 3)}]),

    lager:info("Discover Peer IP/ports and restart with peer config"),
    reset_peer_config(ClusterA, ClusterB),

    lists:foreach(
        fun(N) -> rt:wait_until_ready(N), rt:wait_until_pingable(N) end,
        ClusterA ++ ClusterB
    ),
    
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    
    ok = verify_clock_pruning(ClusterA, ClusterB),
    pass.


verify_clock_pruning(Nodes, ClusterB) ->
    
    {ok, CH} = riak:client_connect(hd(Nodes)),
    {ok, CR} = riak:client_connect(hd(ClusterB)),

    lager:info("Creating bucket types"),
    rt:create_and_activate_bucket_type(
        hd(Nodes),
        element(1, ?BUCKET), 
        [{young_vclock, 20}, {old_vclock, 30}, {small_vclock, 10}, {big_vclock, 10}]),
    rt:create_and_activate_bucket_type(
        hd(ClusterB),
        element(1, ?BUCKET), 
        [{young_vclock, 20}, {old_vclock, 30}, {small_vclock, 10}, {big_vclock, 10}]),

    lager:info("Commencing object load"),
    KeyLoadFun =
        fun(V) ->
            fun(Node, KeyCount) ->
                lager:info("Loading from key ~w on node ~w", [KeyCount, Node]),
                KVs = 
                    test_data(
                        KeyCount + 1, KeyCount + ?NUM_KEYS_PERNODE, V),
                ok = write_data(Node, KVs),
                KeyCount + ?NUM_KEYS_PERNODE
            end
        end,

    lists:foldl(KeyLoadFun(list_to_binary("U1")), 1, Nodes),
    lager:info("Loaded ~w objects", [?NUM_KEYS_PERNODE * length(Nodes)]),
    wait_for_queues_to_drain(Nodes, cluster_b),

    FetchClocksQuery = {fetch_clocks_range, ?BUCKET, all, all, all},

    rt:wait_until(
        fun() ->
            {ok, KCL} = riak_client:aae_fold(FetchClocksQuery, CR),
            length(KCL) == (?NUM_KEYS_PERNODE * length(Nodes))
        end
    ),
    
    lager:info("Stopping a node - query results should be unchanged"),
    FiddlingNode = hd(tl(Nodes)),
    RestNodes = Nodes -- [FiddlingNode],
    rt:stop_and_wait(FiddlingNode),
    
    {ok, KCL2} = riak_client:aae_fold(FetchClocksQuery, CH),
    
    rt:start_and_wait(FiddlingNode),
    
    {ok, KCL3} = riak_client:aae_fold(FetchClocksQuery, CH),
    ?assertMatch(true, lists:sort(KCL2) == lists:sort(KCL3)),

    lager:info("**************************"),
    lager:info("Testing clock pruning"),
    lager:info("Bucket type has a lower than standard big_clock/small_clock"),
    lager:info("This should mean that clocks will reach pruning limit"),
    lager:info("Given enough updates and cluster changes"),
    lager:info("**************************"),

    lager:info("Update keys 4 times"),

    lists:foldl(KeyLoadFun(list_to_binary("U2")), 1, RestNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U3")), 1, RestNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U4")), 1, RestNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U5")), 1, RestNodes),
    wait_for_queues_to_drain(RestNodes, cluster_b),

    lager:info("Leave node ~w", [FiddlingNode]),
    
    ok = rt:staged_leave(FiddlingNode),
    rt:wait_until_ring_converged(Nodes),
    ok = plan_and_wait(hd(RestNodes), RestNodes),
    rt:wait_until_unpingable(FiddlingNode),

    lager:info("Update keys 3 times"),

    lists:foldl(KeyLoadFun(list_to_binary("U6")), 1, RestNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U7")), 1, RestNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U8")), 1, RestNodes),
    wait_for_queues_to_drain(RestNodes, cluster_b),
    
    lager:info("Rejoin Node ~w", [FiddlingNode]),
    rt:start(FiddlingNode),
    rt:wait_until_ready(FiddlingNode),
    rt:wait_until_pingable(FiddlingNode),
    rt:staged_join(FiddlingNode, hd(Nodes)),
    plan_and_wait(hd(Nodes), Nodes),

    lager:info("Update keys 3 times"),

    lists:foldl(KeyLoadFun(list_to_binary("U9")), 1, RestNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U10")), 1, RestNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U11")), 1, RestNodes),
    wait_for_queues_to_drain(RestNodes, cluster_b),

    _CL1D = return_clock_lengths(CH),
    
    EndNode = lists:last(Nodes),
    FrontNodes = Nodes -- [EndNode],

    lager:info("Leave node ~w", [EndNode]),
    
    ok = rt:staged_leave(EndNode),
    rt:wait_until_ring_converged(Nodes),
    ok = plan_and_wait(hd(Nodes), FrontNodes),
    rt:wait_until_unpingable(EndNode),

    lager:info("Update keys 3 times"),

    lists:foldl(KeyLoadFun(list_to_binary("U12")), 1, FrontNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U13")), 1, FrontNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U14")), 1, FrontNodes),
    wait_for_queues_to_drain(FrontNodes, cluster_b),
    
    lager:info("Rejoin Node ~w", [EndNode]),
    rt:start(EndNode),
    rt:wait_until_ready(EndNode),
    rt:wait_until_pingable(EndNode),
    rt:staged_join(EndNode, hd(Nodes)),
    plan_and_wait(hd(Nodes), Nodes),

    lists:foreach(fun(N) -> reset_sink(cluster_b, N) end, ClusterB),

    lager:info("Update keys 3 times"),

    lists:foldl(KeyLoadFun(list_to_binary("U15")), 1, FrontNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U16")), 1, FrontNodes),
    lists:foldl(KeyLoadFun(list_to_binary("U17")), 1, FrontNodes),
    wait_for_queues_to_drain(FrontNodes, cluster_b),

    CL2D = return_clock_lengths(CH),

    ?assert(element(2, lists:keyfind(10, 1, CL2D)) > 0),
    ?assert(element(1, lists:last(CL2D)) == 10),
    BigClocksNow = element(2, lists:keyfind(8, 1, CL2D)),
    lager:info("there are prunable clocks - but none bigger"),

    lager:info("Confirm match of clock lengths between active and passive"),
    ?assert(compare_clock_lengths(CH, CR, 5)),
    lager:info("Change small_clock pruning limit on bucket properties"),
    ok =
        rpc:call(
            hd(Nodes),
            riak_core_bucket_type,
            update,
            [element(1, ?BUCKET), [{small_vclock, 8}]]),
    ok =
        rpc:call(
            hd(ClusterB),
            riak_core_bucket_type,
            update,
            [element(1, ?BUCKET), [{small_vclock, 8}]]),

    CheckSmallClockChange =
        fun(Node, V) ->
            fun() ->
                PB = rt:pbc(Node),
                {ok, BProps} = riakc_pb_socket:get_bucket(PB, ?BUCKET),
                SmallClock = proplists:get_value(small_vclock, BProps),
                lager:info("Small clock is Set to ~w", [SmallClock]),
                V == SmallClock
            end
        end,
    lists:foreach(
        fun(N) -> rt:wait_until(CheckSmallClockChange(N, 8)) end,
        Nodes ++ ClusterB),

    lager:info("Touch all prunable clocks"),
    TouchFun =
        fun({B, K, _C}) ->
            lager:info("Touching ~s", [K]),
            {ok, Obj} = riak_client:get(B, K, CH),
            ok = riak_client:put(Obj, CH)
        end,
    QueriesFun =
        fun(Bucket) ->
            lists:map(
                fun(I) ->
                    {fetch_clocks_range,
                        Bucket,
                        all,
                        {segments,
                            lists:seq((128 * I) + 1, 128 *  (I + 1)),
                            small},
                        all}
                end,
                lists:seq(0, 511)
            )
        end,
    TouchGreaterThan =
        fun(B, M)  ->
            lists:foreach(
                fun(Q) ->
                    lists:foreach(
                        TouchFun,
                        lists:filter(
                            fun({_B0, _K0, C0}) -> length(C0) > M end,
                            element(2, riak_client:aae_fold(Q, CH))))
                end,
                QueriesFun(B))
        end,
    TouchGreaterThan(?BUCKET, 9),
    
    CL4D = return_clock_lengths(CH),                    
    ?assertMatch(false, lists:keyfind(10, 1, CL4D)),
    BigClocksPostTouch = element(2, lists:keyfind(8, 1, CL4D)),
    ?assert(BigClocksPostTouch > BigClocksNow),

    {ok, KCLA} =
        riak_client:aae_fold({fetch_clocks_range, ?BUCKET, all, all, all}, CH),
    {ok, KCLB} =
        riak_client:aae_fold({fetch_clocks_range, ?BUCKET, all, all, all}, CR),
    lists:foreach(
        fun({_B, K, _C}) -> lager:info("Missing Key ~s", [K]) end,
        lists:sort(lists:subtract(KCLA, KCLB))),

    lager:info("Confirm match of clock lengths between active and passive"),
    ?assert(compare_clock_lengths(CH, CR, 5)),

    lager:info("Update keys 3 times on passive cluster"),

    lists:foldl(KeyLoadFun(list_to_binary("U18")), 1, ClusterB),
    lists:foldl(KeyLoadFun(list_to_binary("U19")), 1, ClusterB),
    lists:foldl(KeyLoadFun(list_to_binary("U20")), 1, ClusterB),
    wait_for_queues_to_drain(ClusterB, cluster_a),

    lager:info("Confirm match of clock lengths between active and passive"),
    ?assert(compare_clock_lengths(CH, CR, 5)),

    lager:info("Full-sync setup - and show no deltas"),
    ok = setup_fullsync_peer(Nodes, hd(ClusterB)),
    R = rpc:call(hd(Nodes), riak_client, ttaaefs_fullsync, [all_check, 60]),
    lager:info("Full sync A -> B ~p", [R]),
    ?assertMatch({root_compare, 0}, R),

    lager:info("Temporary change to small_clock pruning limit"),
    ok =
        rpc:call(
            hd(Nodes),
            riak_core_bucket_type,
            update,
            [element(1, ?BUCKET), [{small_vclock, 9}]]),
    lists:foreach(
        fun(N) -> rt:wait_until(CheckSmallClockChange(N, 9)) end,
        Nodes),

    lager:info("Update keys once"),
    lists:foldl(KeyLoadFun(list_to_binary("U21")), 1, Nodes),
    wait_for_queues_to_drain(Nodes, cluster_b),

    lager:info("Revert temporary change to small_clock pruning limit"),
    ok =
        rpc:call(
            hd(Nodes),
            riak_core_bucket_type,
            update,
            [element(1, ?BUCKET), [{small_vclock, 8}]]),
    lists:foreach(
        fun(N) -> rt:wait_until(CheckSmallClockChange(N, 8)) end,
        Nodes),
    
    CL3D = return_clock_lengths(CH),
    _ = return_clock_lengths(CR),
    BigClocks = element(2, lists:keyfind(9, 1, CL3D)),
    lager:info("Big clocks on Cluster A ~w", [BigClocks]),

    return_clock_lengths(CH, CR, 10),

    lager:info("Full-sync should show no deltas"),
    ACR1 = rpc:call(hd(Nodes), riak_client, ttaaefs_fullsync, [all_check, 60]),
    lager:info("Full sync A -> B ~p", [ACR1]),
    ?assertMatch({root_compare, 0}, ACR1),

    KeyLoadN1Fun =
        fun(V) ->
            fun(Node, KeyCount) ->
                lager:info("Loading from key ~w on node ~w", [KeyCount, Node]),
                KVs = 
                    test_data(
                        KeyCount + 1, KeyCount + ?NUM_KEYS_PERNODE, V),
                ok = write_data(Node, KVs, [{n_val, 1}]),
                KeyCount + ?NUM_KEYS_PERNODE
            end
        end,
    lager:info("Update keys once - with n_val of 1"),
    lists:foldl(KeyLoadN1Fun(list_to_binary("U22")), 1, Nodes),

    lager:info("Read repair all keys twice"),
    ExpectedKeys = length(Nodes) * ?NUM_KEYS_PERNODE,
    {ok,{[], ExpectedKeys, all, _BS}} =
        riak_client:aae_fold({repair_keys_range, ?BUCKET, all, all, all}, CH),
    empty_read_queues(Nodes),
    _ = return_clock_lengths(CH),
    _ = return_clock_lengths(CR),
    {ok,{[], ExpectedKeys, all, _BS}} =
        riak_client:aae_fold({repair_keys_range, ?BUCKET, all, all, all}, CH),
    empty_read_queues(Nodes),
    _ = return_clock_lengths(CH),
    _ = return_clock_lengths(CR),

    lager:info("Full-sync should show deltas"),
    ACR2 = rpc:call(hd(Nodes), riak_client, ttaaefs_fullsync, [all_check, 60]),
    lager:info("Full sync A -> B ~p", [ACR2]),

    wait_for_queues_to_drain(Nodes, cluster_b),

    _ = return_clock_lengths(CH),
    _ = return_clock_lengths(CR),

    lager:info("Full-sync should show no deltas"),
    ACR3 = rpc:call(hd(Nodes), riak_client, ttaaefs_fullsync, [all_check, 60]),
    lager:info("Full sync A -> B ~p", [ACR3]),
    ?assertMatch({root_compare, 0}, ACR3)

    .


return_clock_lengths(RiakClient) ->
    ClockLengthFun =
        fun(B) ->
            {ok, KCL} =
                riak_client:aae_fold(
                    {fetch_clocks_range, B, all, all, all}, RiakClient),
            lists:map(fun({_B0, _K0, C0}) -> length(C0) end, KCL)
        end,
    ClockLengths =
        lists:sort(
            dict:to_list(
                lists:foldl(
                    fun(L, D) -> dict:update_counter(L, 1, D) end,
                    dict:new(),
                    ClockLengthFun(?BUCKET))
            )
        ),
    lager:info("Client ~w sees clock lengths: ~w", [RiakClient, ClockLengths]),
    ClockLengths.

compare_clock_lengths(CL, CR, 0) ->
    {ok, RawKCL} = 
        riak_client:aae_fold(
            {fetch_clocks_range, ?BUCKET, all, all, all}, CL),
    {ok, RawKCR} =
        riak_client:aae_fold(
            {fetch_clocks_range, ?BUCKET, all, all, all}, CR),
    lager:info("Differences A to B:"),
    lists:foreach(
        fun({_B, K, C}) -> lager:info("K ~s C ~w", [K, C]) end,
        lists:sort(lists:subtract(RawKCL, RawKCR))
    ),
    lager:info("Differences B to A:"),
    lists:foreach(
        fun({_B, K, C}) -> lager:info("K ~s C ~w", [K, C]) end,
        lists:sort(lists:subtract(RawKCR, RawKCL))
    ),
    false;
compare_clock_lengths(CL, CR, N) ->
    lager:info("Confirm match of clock lengths between active and passive"),
    KCL = return_clock_lengths(CL),
    KCR = return_clock_lengths(CR),
    case KCL of
        KCR ->
            true;
        _ ->
            timer:sleep(10000),
            compare_clock_lengths(CL, CR, N - 1)
    end.

to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].

write_data(Node, KVs) ->
    write_data(Node, KVs, [{w, 3}]).

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

reset_peer_config(ClusterA, ClusterB) ->
    FoldToPeerConfigPB = 
        fun(Node, Acc) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port) ++ ":pb"
        end,
    ClusterASnkPL = lists:foldl(FoldToPeerConfigPB, "", ClusterB),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfigPB, "", ClusterA),
    ClusterASNkCfg = ?SNK_CONFIG(cluster_a, ClusterASnkPL),
    ClusterBSNkCfg = ?SNK_CONFIG(cluster_b, ClusterBSnkPL),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterASNkCfg) end, ClusterA),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterBSNkCfg) end, ClusterB),           

    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB).

setup_fullsync_peer(ClusterA, NodeB) ->
    {pb, {IP, Port}} = lists:keyfind(pb, 1, rt:connection_info(NodeB)),
    ClusterACfg = ?FS_CONFIG(IP, Port, cluster_b, cluster_a),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterACfg) end, ClusterA),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end, ClusterA).


reset_sink(QueueName, Node) ->
    rpc:call(Node, riak_kv_replrtq_snk, suspend_snkqueue, [QueueName]),
    rpc:call(Node, riak_kv_replrtq_snk, resume_snkqueue, [QueueName]).

return_clock_lengths(_CH, _CR, 0) ->
    ok;
return_clock_lengths(CH, CR, N) ->
    return_clock_lengths(CH),
    return_clock_lengths(CR),
    timer:sleep(1000),
    return_clock_lengths(CH, CR, N - 1).

wait_for_queues_to_drain([], QueueName) ->
    lager:info("Queue ~w drained on nodes", [QueueName]);
wait_for_queues_to_drain([N|Rest], QueueName) ->
    rt:wait_until(
        fun() ->
            {QueueName, {0, 0, 0}} ==
                rpc:call(N, riak_kv_replrtq_src, length_rtq, [QueueName])
        end
    ),
    wait_for_queues_to_drain(Rest, QueueName).

empty_read_queues(Nodes) ->
    lists:foreach(
        fun(N) -> true = get_read_stats(N, 10) end,
        Nodes
    ).

get_read_stats(_Node, 0) ->
    false;
get_read_stats(Node, N) ->
    RSL = rpc:call(Node, riak_kv_reader, read_stats, []),
    {mqueue_lengths, MQL} = lists:keyfind(mqueue_lengths, 1, RSL),
    case lists:filter(fun({_P, QL}) -> QL > 0 end, MQL) of
        [] ->
            true;
        NonEmptyQueues ->
            lager:info(
                "Non-empty queues ~w on Node ~w",
                [NonEmptyQueues, Node]),
            timer:sleep(10000),
            get_read_stats(Node, N - 1)
    end.
        