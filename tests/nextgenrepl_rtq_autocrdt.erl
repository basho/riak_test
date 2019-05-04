%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(nextgenrepl_rtq_autocrdt).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(A_RING, 16).
-define(B_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 1).

-define(CTYPE, <<"_counters">>).
-define(STYPE, <<"_sets">>).
-define(MTYPE, <<"_maps">>).
-define(HTYPE, <<"_hlls">>).
-define(TYPES, [{?CTYPE, counter},
                {?STYPE, set},
                {?MTYPE, map},
                {?HTYPE, hll}]).


-define(SNK_WORKERS, 2).

-define(REPL_SLEEP, 512). 
    % May need to wait for 2 x the 256ms max sleep time of a snk worker
-define(WAIT_LOOPS, 12).

-define(CONFIG(RingSize, NVal, SrcQueueDefns), [
        {riak_core,
            [
             {ring_creation_size, RingSize},
             {default_bucket_props,
                 [
                     {n_val, NVal},
                     {allow_mult, true},
                     {dvv_enabled, true}
                 ]}
            ]
        },
        {riak_kv,
          [
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 3600},
           {tictacaae_exchangetick, 120 * 1000},
           {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
           {delete_mode, keep},
           {enable_repl_cache, true},
           {replrtq_srcqueue, SrcQueueDefns}
          ]}
        ]).

-define(SNK_CONFIG(ClusterName, PeerList), 
        [{riak_kv, 
            [{replrtq_enablesink, true},
                {replrtq_sink1queue, ClusterName},
                {replrtq_sink1peers, PeerList},
                {replrtq_sink1workers, ?SNK_WORKERS}]}]).

confirm() ->
    ClusterASrcQ = "cluster_b:buckettype._maps",
    ClusterBSrcQ = "cluster_a:buckettype._maps",

    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {3, ?CONFIG(?A_RING, ?A_NVAL, ClusterASrcQ)},
            {3, ?CONFIG(?B_RING, ?B_NVAL, ClusterBSrcQ)}]),
    
    lager:info("Discover Peer IP/ports and restart with peer config"),
    FoldToPeerConfig = 
        fun(Node, Acc) ->
            {http, {IP, Port}} =
                lists:keyfind(http, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port)
        end,
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA),
    ClusterASNkCfg = ?SNK_CONFIG(cluster_a, ClusterASnkPL),
    ClusterBSNkCfg = ?SNK_CONFIG(cluster_b, ClusterBSnkPL),
    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ClusterASNkCfg) end,
                    ClusterA),
    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ClusterBSNkCfg) end,
                    ClusterB),

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB),
    
    lager:info("Creating bucket types"),
    rt:create_and_activate_bucket_type(hd(ClusterA),
                                        <<"_maps">>, 
                                        [{datatype, map}, {allow_mult, true}]),
    rt:create_and_activate_bucket_type(hd(ClusterB),
                                        <<"_maps">>, 
                                        [{datatype, map}, {allow_mult, true}]),

    lager:info("Ready for test."),
    test_rtqrepl_between_clusters(ClusterA, ClusterB).

test_rtqrepl_between_clusters(ClusterA, ClusterB) ->

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    
    lager:info("Initiate a map in Cluster A"),
    ClientA = rt:pbc(NodeA),
    ClientB = rt:pbc(NodeB),
    riakc_pb_socket:modify_type(
                        ClientA,
                        fun(M) ->
                            M1 = riakc_map:update(
                                    {<<"friends">>, set},
                                    fun(S) ->
                                        riakc_set:add_element(<<"Russell">>,
                                                                S)
                                    end,
                                    M),
                            M2 = riakc_map:update(
                                    {<<"followers">>, counter},
                                    fun(C) ->
                                        riakc_counter:increment(10,
                                                                C)
                                    end,
                                    M1),
                            riakc_map:update(
                                    {<<"name">>, register},
                                    fun(R) ->
                                        riakc_register:set(<<"Original">>,
                                                            R)
                                    end,
                                    M2)
                        end,
                        {<<"_maps">>, <<"test_map">>}, 
                        <<"TestKey">>,
                        [create]),
    
    ExpVal1 = 
        [{{<<"followers">>, counter}, 10},
            {{<<"friends">>, set}, [<<"Russell">>]},
            {{<<"name">>, register}, <<"Original">>}],
    lager:info("Read own write in Cluster A"),
    check_value(ClientA,
                    riakc_pb_socket,
                    {<<"_maps">>, <<"test_map">>},
                    <<"TestKey">>,
                    riakc_map,
                    ExpVal1),
    
    lager:info("Read repl'd write eventually in Cluster B"),
    check_value(ClientB,
                    riakc_pb_socket,
                    {<<"_maps">>, <<"test_map">>},
                    <<"TestKey">>,
                    riakc_map,
                    ExpVal1),
    
    lager:info("Make a change to set and counter"),
    riakc_pb_socket:modify_type(
                        ClientA,
                        fun(M) ->
                            M1 = riakc_map:update(
                                    {<<"friends">>, set},
                                    fun(S) ->
                                        riakc_set:add_element(<<"Martin">>,
                                                                S)
                                    end,
                                    M),
                            riakc_map:update(
                                    {<<"followers">>, counter},
                                    fun(C) ->
                                        riakc_counter:increment(10,
                                                                C)
                                    end,
                                    M1)
                        end,
                        {<<"_maps">>, <<"test_map">>}, 
                        <<"TestKey">>,
                        [create]),
    ExpVal2 = 
        [{{<<"followers">>, counter}, 20},
            {{<<"friends">>, set}, [<<"Martin">>, <<"Russell">>]},
            {{<<"name">>, register}, <<"Original">>}],
    check_value(ClientA,
                    riakc_pb_socket,
                    {<<"_maps">>, <<"test_map">>},
                    <<"TestKey">>,
                    riakc_map,
                    ExpVal2),
    
    lager:info("Read repl'd write eventually in Cluster B"),
    check_value(ClientB,
                    riakc_pb_socket,
                    {<<"_maps">>, <<"test_map">>},
                    <<"TestKey">>,
                    riakc_map,
                    ExpVal2),
    
    lager:info("Breaking real-time replication"),
    StopReplFun = 
        fun(SrcNode) ->
            ok = rpc:call(SrcNode,
                            riak_kv_replrtq_src,
                            suspend_rtq,
                            [cluster_b])
        end,
    lists:foreach(StopReplFun, ClusterA),
    lager:info("Make a change to set"),
    riakc_pb_socket:modify_type(
                        ClientA,
                        fun(M) ->
                            riakc_map:update(
                                    {<<"friends">>, set},
                                    fun(S) ->
                                        riakc_set:add_element(<<"Pontus">>,
                                                                S)
                                    end,
                                    M)
                        end,
                        {<<"_maps">>, <<"test_map">>}, 
                        <<"TestKey">>,
                        [create]),
    lager:info("Check local value"),
    ExpVal3 = 
        [{{<<"followers">>, counter}, 20},
            {{<<"friends">>, set},
                [<<"Martin">>, <<"Pontus">>, <<"Russell">>]},
            {{<<"name">>, register}, <<"Original">>}],
    check_value(ClientA,
                    riakc_pb_socket,
                    {<<"_maps">>, <<"test_map">>},
                    <<"TestKey">>,
                    riakc_map,
                    ExpVal3),
    timer:sleep(?REPL_SLEEP),
    lager:info("After a pause replicated value remains unchanged"),
    check_value(ClientB,
                    riakc_pb_socket,
                    {<<"_maps">>, <<"test_map">>},
                    <<"TestKey">>,
                    riakc_map,
                    ExpVal2),
    
    lager:info("Re-enabling real-time replication"),
    RestartReplFun = 
        fun(SrcNode) ->
            ok = rpc:call(SrcNode,
                            riak_kv_replrtq_src,
                            resume_rtq,
                            [cluster_b])
        end,
    lists:foreach(RestartReplFun, ClusterA),
    timer:sleep(?REPL_SLEEP),
    lager:info("After a pause replicated value remains unchanged"),
    lager:info("Resume does not replay any replication"),
    check_value(ClientB,
                    riakc_pb_socket,
                    {<<"_maps">>, <<"test_map">>},
                    <<"TestKey">>,
                    riakc_map,
                    ExpVal2),
    
    lager:info("Update the register"),
    riakc_pb_socket:modify_type(
                        ClientA,
                        fun(M) ->
                            riakc_map:update(
                                    {<<"name">>, register},
                                    fun(R) ->
                                        riakc_register:set(<<"Jaded">>,
                                                            R)
                                    end,
                                    M)
                        end,
                        {<<"_maps">>, <<"test_map">>}, 
                        <<"TestKey">>,
                        [create]),
    
    ExpVal4 = 
        [{{<<"followers">>, counter}, 20},
            {{<<"friends">>, set},
                [<<"Martin">>, <<"Pontus">>, <<"Russell">>]},
            {{<<"name">>, register}, <<"Jaded">>}],
    lager:info("Check local value"),
    check_value(ClientA,
                    riakc_pb_socket,
                    {<<"_maps">>, <<"test_map">>},
                    <<"TestKey">>,
                    riakc_map,
                    ExpVal4),
    lager:info("Check that remote vale has converged with all changes"),
    check_value(ClientB,
                    riakc_pb_socket,
                    {<<"_maps">>, <<"test_map">>},
                    <<"TestKey">>,
                    riakc_map,
                    ExpVal4),

    pass.


check_value(Client, CMod, Bucket, Key, DTMod, Expected) ->
    check_value(Client,CMod,Bucket,Key,DTMod,Expected,
                [{r,2}, {notfound_ok, true}, {timeout, 5000}]).

check_value(Client, CMod, Bucket, Key, DTMod, Expected, Options) ->
    rt:wait_until(fun() ->
                        try
                            Result = CMod:fetch_type(Client, Bucket, Key,
                                                    Options),
                            ?assertMatch({ok, _}, Result),
                            {ok, C} = Result,
                            ?assertEqual(true, DTMod:is_type(C)),
                            ?assertEqual(Expected, DTMod:value(C)),
                            true
                        catch
                            Type:Error ->
                                lager:info("check_value(~p,~p,~p,~p,~p) "
                                            "failed: ~p:~p", [Client, Bucket,
                                                            Key, DTMod,
                                                            Expected, Type,
                                                            Error]),
                                false
                        end
                    end).
