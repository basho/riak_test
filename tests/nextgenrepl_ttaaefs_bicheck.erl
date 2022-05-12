%% @doc
%% This module implements a riak_test to test the auto_check method
%% in nextgenrepl ttaaefs, with bi-directional full-sync replication
%% between the clusters - such that a full-sync in one direction will
%% prompt repairs in both directions

-module(nextgenrepl_ttaaefs_bicheck).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 32).
-define(A_NVAL, 1).
-define(B_NVAL, 3).
-define(STATS_WAIT, 1000).
-define(REPL_WAIT, 5000).
-define(MEGA, 1000000).
-define(SNK_WORKERS, 4).

-define(BASE_CONFIG(RingSize, Nval), [
        {riak_core,
            [
             {ring_creation_size, RingSize},
             {default_bucket_props,
                 [
                     {n_val, Nval},
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
           {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
          ]}]).

-define(REPL_CONFIG(
    RingSize, NVal, PeerIP, PeerPort, PeerProtocol, RemoteNVal, QL, QP),
    [
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
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 3600},
           {tictacaae_exchangetick, 120 * 1000},
           {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
           {ttaaefs_scope, all},
           {ttaaefs_queuename, list_to_atom(QL)},
           {ttaaefs_queuename_peer, QP},
           {ttaaefs_maxresults, 32},
           {ttaaefs_rangeboost, 4},
           {ttaaefs_nocheck, 24},
           {ttaaefs_autocheck, 0},
           {ttaaefs_rangecheck, 0},
           {ttaaefs_allcheck, 0},
           {ttaaefs_localnval, NVal},
           {ttaaefs_remotenval, RemoteNVal},
           {ttaaefs_peerip, PeerIP},
           {ttaaefs_peerport, PeerPort},
           {ttaaefs_peerprotocol, PeerProtocol},
           {delete_mode, immediate},
           {replrtq_enablesrc, true},
           {replrtq_srcqueue, QL ++ ":any"},
           {replrtq_enablesink, true},
           {replrtq_sinkqueue, QP},
           {replrtq_sinkpeers,
                PeerIP ++ ":" ++
                    integer_to_list(PeerPort) ++ ":" ++
                    atom_to_list(Protocol)},
           {replrtq_sinkworkers, ?SNK_WORKERS}
        ]}
        ]).

confirm() ->
    [ClusterA1, ClusterB1] = setup_clusters(),
    update_cluster(
        ClusterA1, ClusterB1,
        ?A_RING, ?A_NVAL, ?B_NVAL, http,
        "cluster_b", cluster_a),
    update_cluster(
        ClusterB1, ClusterA1,
        ?B_RING, ?B_NVAL, ?A_NVAL, pb,
        "cluster_a", cluster_b),
    wait_for_convergence(ClusterA1, ClusterB1),
    
    test_repl_between_clusters(ClusterA1, ClusterB1).

update_cluster(
    [Node1, Node2, Node3],
    [Peer1, Peer2, Peer3],
    RingSize, NVal, RemoteNVal, Protocol,
    LocalQueue, RemoteQueue) ->
    
    lager:info("Setup automatic replication between clusters"),
    {Protocol, {IP1, Port1}} = lists:keyfind(Protocol, 1, rt:connection_info(Peer1)),
    {Protocol, {IP2, Port2}} = lists:keyfind(Protocol, 1, rt:connection_info(Peer2)),
    {Protocol, {IP3, Port3}} = lists:keyfind(Protocol, 1, rt:connection_info(Peer3)),
    rt:set_advanced_conf(
        Node1, 
        ?REPL_CONFIG(
            RingSize,
            NVal,
            IP1,
            Port1,
            Protocol,
            RemoteNVal,
            LocalQueue,
            RemoteQueue)),
    rt:set_advanced_conf(
        Node2, 
        ?REPL_CONFIG(
            RingSize,
            NVal,
            IP2,
            Port2,
            Protocol,
            RemoteNVal,
            LocalQueue,
            RemoteQueue)),
    rt:set_advanced_conf(
        Node3, 
        ?REPL_CONFIG(
            RingSize,
            NVal,
            IP3,
            Port3,
            Protocol,
            RemoteNVal,
            LocalQueue,
            RemoteQueue)),
    timer:sleep(60 * 1000),
    lager:info("Paused for real-time replication to start"),
    ok.


setup_clusters() ->

    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {3, ?BASE_CONFIG(?A_RING, ?A_NVAL)},
            {3, ?BASE_CONFIG(?B_RING, ?B_NVAL)}]),
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    
    wait_for_convergence(ClusterA, ClusterB),
    
    lager:info("Ready for test"),
    [ClusterA, ClusterB].


test_repl_between_clusters(ClusterA, ClusterB) ->

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    
    lager:info("Check empty clusters will compare"),

    {root_compare, 0} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),
    {root_compare, 0} =
        rpc:call(NodeB, riak_client, ttaaefs_fullsync, [auto_check]),

    lager:info("Test 100 key difference and resolve"),
    % Write keys to cluster A, verify B and C have them.
    write_to_cluster(NodeA, 1, 100),
    timer:sleep(?REPL_WAIT),
    ?assertMatch(0, read_from_cluster(NodeA, 1, 100)),
    rt:wait_until(
        fun() ->
            E = read_from_cluster(NodeB, 1, 100),
            lager:info("Errors of E=~w", [E]),
            E == 0
        end,
        6,
        10 * 1000),
    
    lager:info("Check clusters with common data will compare"),

    {root_compare, 0} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),
    {root_compare, 0} =
        rpc:call(NodeB, riak_client, ttaaefs_fullsync, [auto_check]),
    
    SuspendFun =
        fun(QueueName) ->
            fun(N) ->
                ok = rpc:call(N, riak_kv_replrtq_src, suspend_rtq, [QueueName])
            end
        end,
    ResumeFun =
        fun(QueueName) ->
            fun(N) ->
                ok = rpc:call(N, riak_kv_replrtq_src, resume_rtq, [QueueName])
            end
        end,
    
    lager:info("Creating delta whilst queue suspended"),

    lists:foreach(SuspendFun(cluster_b), ClusterA),
    write_to_cluster(NodeA, 101, 200),
    timer:sleep(?REPL_WAIT),
    lists:foreach(ResumeFun(cluster_b), ClusterA),
    ?assertMatch(0, read_from_cluster(NodeA, 101, 200)),
    ?assertMatch(100, read_from_cluster(NodeB, 101, 200)),

    lager:info("Resolving delta by full-sync A-> B"),

    {clock_compare, 100} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),
    ok = 
        rt:wait_until(
            fun() ->
                E = read_from_cluster(NodeB, 101, 200),
                lager:info("Errors of E=~w", [E]),
                E == 0
            end,
            6,
            10 * 1000),
    
    lager:info("Creating new delta whilst queue suspended"),

    lists:foreach(SuspendFun(cluster_b), ClusterA),
    write_to_cluster(NodeA, 201, 300),
    timer:sleep(?REPL_WAIT),
    lists:foreach(ResumeFun(cluster_b), ClusterA),
    ?assertMatch(0, read_from_cluster(NodeA, 201, 300)),
    ?assertMatch(100, read_from_cluster(NodeB, 201, 300)),

    lager:info("Resolving delta by full-sync B -> A"),

    {clock_compare, 100} =
        rpc:call(NodeB, riak_client, ttaaefs_fullsync, [auto_check]),
    ok =
        rt:wait_until(
            fun() ->
                E = read_from_cluster(NodeB, 201, 300),
                lager:info("Errors of E=~w", [E]),
                E == 0
            end,
            6,
            10 * 1000),
    
    {root_compare, 0} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),
    
    lager:info(
        "Creating new delta in opposite direction - changes protocol used"),
    lists:foreach(SuspendFun(cluster_a), ClusterB),
    write_to_cluster(NodeB, 301, 400),
    timer:sleep(?REPL_WAIT),
    lists:foreach(ResumeFun(cluster_a), ClusterB),
    ?assertMatch(0, read_from_cluster(NodeB, 301, 400)),
    ?assertMatch(100, read_from_cluster(NodeA, 301, 400)),

    lager:info("Resolving delta by full-sync A -> B"),
    lager:info("Works as last check was success, so range reset"),
    {clock_compare, 100} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),
    ok =
        rt:wait_until(
            fun() ->
                E = read_from_cluster(NodeA, 301, 400),
                lager:info("Errors of E=~w", [E]),
                E == 0
            end,
            6,
            10 * 1000),
    
    {root_compare, 0} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),

    lager:info(
        "Creating new delta through deletion - this will create false delta"),
    lager:info(
        "Where the last modified date is less recent (due to non-rep of del)"),
    
    lists:foreach(SuspendFun(cluster_b), ClusterA),
    delete_from_cluster(NodeA, 1, 100),
    timer:sleep(?REPL_WAIT),
    lists:foreach(ResumeFun(cluster_b), ClusterA),

    lager:info(
        "Next auto_check will be a range_check, but all out of lmd window"),
    {clock_compare, 0} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),
    lager:info(
        "Next auto_check will be an all_check, but max_results found"),
    lager:info(
        "Will repair from B back to A"),
    {clock_compare, 32} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),
    timer:sleep(?REPL_WAIT),
    ok =
        rt:wait_until(
            fun() ->
                E = read_from_cluster(NodeA, 1, 100),
                lager:info("Errors of E=~w", [E]),
                E == 68
            end,
            6,
            10 * 1000),
    
    lager:info("32 objects resurrected - all will have key amnesia as N=1"),
    lager:info("So 68 unreplicated, and 32 to replicate back"),
    lager:info("Range should be detected, and range_check used, 100 found"),
    {clock_compare, 100} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),
    ok =
        rt:wait_until(
            fun() ->
                E = read_from_cluster(NodeA, 1, 100),
                lager:info("Errors of E=~w", [E]),
                E == 0
            end,
            6,
            10 * 1000),
    lager:info("The last sync sent 32 back, and received the remaining 68"),
    lager:info("The 68 received will have key amnesia and a clock update"),
    lager:info("So they must be replicated one more time"),
    {clock_compare, 68} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),
    timer:sleep(?REPL_WAIT),
    {root_compare, 0} =
        rpc:call(NodeA, riak_client, ttaaefs_fullsync, [auto_check]),
    {root_compare, 0} =
        rpc:call(NodeB, riak_client, ttaaefs_fullsync, [auto_check]),

    pass.

wait_for_convergence(ClusterA, ClusterB) ->
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB).


%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End) ->
    CommonValBin = <<"CommonValueToWriteForAllObjects">>,
    write_to_cluster(Node, Start, End, ?TEST_BUCKET, true, CommonValBin).

write_to_cluster(Node, Start, End, Bucket, NewObj, CVB) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = key(N),
            Obj = 
                case NewObj of
                    true ->
                        riak_object:new(Bucket,
                                        Key,
                                        <<N:32/integer, CVB/binary>>);
                    false ->
                        UPDV = <<N:32/integer, CVB/binary>>,
                        {ok, PrevObj} = riak_client:get(Bucket, Key, C),
                        riak_object:update_value(PrevObj, UPDV)
                end,
            try riak_client:put(Obj, C) of
                ok ->
                    Acc;
                Other ->
                    [{N, Other} | Acc]
            catch
                What:Why ->
                    [{N, {What, Why}} | Acc]
            end
        end,
    Errors = lists:foldl(F, [], lists:seq(Start, End)),
    lager:warning("~p errors while writing: ~p", [length(Errors), Errors]),
    ?assertEqual([], Errors).


%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End) ->
    CommonValBin = <<"CommonValueToWriteForAllObjects">>,
    read_from_cluster(Node, Start, End, ?TEST_BUCKET, CommonValBin).

read_from_cluster(Node, Start, End, Bucket, CommonValBin) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = key(N),
            case  riak_client:get(Bucket, Key, C) of
                {ok, Obj} ->
                    ExpectedVal = <<N:32/integer, CommonValBin/binary>>,
                    case riak_object:get_value(Obj) of
                        ExpectedVal ->
                            Acc;
                        UnexpectedVal ->
                            [{wrong_value, UnexpectedVal}|Acc]
                    end;
                {error, Error} ->
                    [{fetch_error, Error}|Acc]
            end
        end,
    length(lists:foldl(F, [], lists:seq(Start, End))).

key(N) ->
    list_to_binary(io_lib:format("~8..0B~n", [N])).

delete_from_cluster(Node, Start, End) ->
    delete_from_cluster(Node, Start, End, ?TEST_BUCKET).

delete_from_cluster(Node, Start, End, Bucket) ->
    lager:info("Deleting ~p keys from node ~p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = key(N),
            try riak_client:delete(Bucket, Key, C) of
                ok ->
                    Acc;
                Other ->
                    [{N, Other} | Acc]
            catch
                What:Why ->
                    [{N, {What, Why}} | Acc]
            end
        end,
    Errors = lists:foldl(F, [], lists:seq(Start, End)),
    lager:warning("~p errors while deleting: ~p", [length(Errors), Errors]),
    ?assertEqual([], Errors).

% get_stats(Node) ->
%     lager:info("Fetching stats from ~w", [Node]),
%     S = verify_riak_stats:get_stats(Node, ?STATS_WAIT),
%     {_, ACT} = lists:keyfind(<<"ttaaefs_allcheck_total">>, 1, S),
%     {_, DCT} = lists:keyfind(<<"ttaaefs_daycheck_total">>, 1, S),
%     {_, HCT} = lists:keyfind(<<"ttaaefs_hourcheck_total">>, 1, S),
%     {_, RCT} = lists:keyfind(<<"ttaaefs_rangecheck_total">>, 1, S),
%     {_, SST} = lists:keyfind(<<"ttaaefs_sync_total">>, 1, S),
%     {_, NST} = lists:keyfind(<<"ttaaefs_nosync_total">>, 1, S),
%     {_, SrcAT} = lists:keyfind(<<"ttaaefs_src_ahead_total">>, 1, S),
%     {_, SnkAT} = lists:keyfind(<<"ttaaefs_snk_ahead_total">>, 1, S),
%     {_, STimeMax} = lists:keyfind(<<"ttaaefs_sync_time_100">>, 1, S),
%     {_, NSTimeMax} = lists:keyfind(<<"ttaaefs_nosync_time_100">>, 1, S),
%     lager:info(
%         "Stats all_check=~w day_check=~w hour_check=~w range_check=~w",
%         [ACT, DCT, HCT, RCT]),
%     lager:info(
%         "Stats sync=~w nosync=~w src_ahead=~w snk_ahead=~w",
%         [SST, NST, SrcAT, SnkAT]),
%     lager:info(
%         "Stats max_sync_time=~w ms max_nosync_time=~w ms",
%         [STimeMax div 1000, NSTimeMax div 1000]),
%     {ACT, DCT, HCT, RCT, SST, NST, SrcAT, SnkAT}.
