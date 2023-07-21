%% @doc
%% This module implements a riak_test to prove real-time repl
%% works as expected with automated discovery of peers

-module(nextgenrepl_rtq_workercount).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 16).
-define(B_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 1).

-define(SNK_WORKERS, 2).
-define(PEER_LIMIT, 2).
-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(INIT_MAX_DELAY, 60).
-define(STND_MAX_DELAY, 3600).
-define(NUM_KEYS, 100000).
-define(PEER_DISCOVERY_DELAY, 10).

-define(REPL_SLEEP, 2048). 
    % May need to wait for 2 x the 1024ms max sleep time of a snk worker
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
            {ttaaefs_maxresults, 128},
            {delete_mode, keep},
            {replrtq_enablesrc, true},
            {replrtq_srcqueue, SrcQueueDefns},
            {replrtq_peer_discovery, true},
            {replrtq_prompt_min_seconds, ?PEER_DISCOVERY_DELAY}
          ]}
        ]).

-define(SNK_CONFIG(ClusterName, PeerList), 
        [{riak_kv, 
            [{replrtq_enablesink, true},
                {replrtq_prompt_max_seconds, ?INIT_MAX_DELAY},
                {replrtq_sinkqueue, ClusterName},
                {replrtq_sinkpeers, PeerList},
                {replrtq_sinkworkers, ?SNK_WORKERS},
                {replrtq_sinkpeerlimit, ?PEER_LIMIT}]}]).

confirm() ->
    ClusterASrcQ = "cluster_b:any",
    ClusterBSrcQ = "cluster_a:any",

    [ClusterA1, ClusterB1] =
        rt:deploy_clusters([
            {4, ?CONFIG(?A_RING, ?A_NVAL, ClusterASrcQ)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, ClusterBSrcQ)}]),

    cluster_test(ClusterA1, ClusterB1, pb).

cluster_test(ClusterA, ClusterB, Protocol) ->

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),

    lager:info("Discover Peer IP/ports and restart with peer config"),
    lists:foreach(compare_peer_info(4, Protocol), ClusterA),
    lists:foreach(compare_peer_info(2, Protocol), ClusterB),

    [NodeA|_RestA] = ClusterA,
    [NodeB1, NodeB2] = ClusterB,

    PeerConfigFun =
        fun(Node) ->
            {Protocol, {IP, Port}} =
                lists:keyfind(Protocol, 1, rt:connection_info(Node)),
            IP ++ ":" ++ integer_to_list(Port) ++ ":" ++ atom_to_list(Protocol)
        end,
    
    PeerA = PeerConfigFun(NodeA),
    PeerB = PeerConfigFun(NodeB1),

    reset_peer_config(ClusterA, cluster_a, PeerB),
    reset_peer_config(ClusterB, cluster_b, PeerA),
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lager:info("Confirm riak_kv is up on all nodes."),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB),

    lager:info("Ready for test - with ~w client for rtq.", [Protocol]),
    lager:info(
        "Wait for peer discovery delay of ~w seconds",
        [?PEER_DISCOVERY_DELAY]),
    timer:sleep(?PEER_DISCOVERY_DELAY * 1000),

    lager:info("Set worker counts to be lower for B2"),
    ok = rpc:call(NodeB1, riak_kv_replrtq_snk, set_workercount, [cluster_b, 8, 2]),
    ok = rpc:call(NodeB2, riak_kv_replrtq_snk, set_workercount, [cluster_b, 2, 2]),

    lager:info("B1 state before load :~n~p", [get_snk_state(NodeB1)]),
    lager:info("B2 state before load :~n~p", [get_snk_state(NodeB2)]),
    lager:info("Commence data load ..."),
    SW0 = os:timestamp(),
    write_to_cluster(NodeA, 1, ?NUM_KEYS, new_obj),
    lager:info(
        "Write took ~w seconds", 
        [timer:now_diff(os:timestamp(), SW0) div (1000 * 1000)]),
    lager:info("Wait for queues to drain"),
    verify_vclock_prune:wait_for_queues_to_drain(ClusterA, cluster_b),
    
    StatsB1_0 = get_stats([NodeB1]),
    StatsB2_0 = get_stats([NodeB2]),
    lager:info("NodeB1 stats ~w", [StatsB1_0]),
    lager:info("NodeB2 stats ~w", [StatsB2_0]),
    B1_Fetches0 = element(4, StatsB1_0),
    B2_Fetches0 = element(4, StatsB2_0),
    ?assert(B1_Fetches0 > (2 * B2_Fetches0)),

    lager:info("B1 state before change :~n~p", [get_snk_state(NodeB1)]),
    lager:info("B2 state before change :~n~p", [get_snk_state(NodeB2)]),
    lager:info("Set worker counts to be lower for B1"),
    ok = rpc:call(NodeB1, riak_kv_replrtq_snk, set_workercount, [cluster_b, 2, 2]),
    ok = rpc:call(NodeB2, riak_kv_replrtq_snk, set_workercount, [cluster_b, 8, 2]),
    lager:info("B1 state post change :~n~p", [get_snk_state(NodeB1)]),
    lager:info("B2 state post change :~n~p", [get_snk_state(NodeB2)]),

    lager:info("Commence data load ..."),
    SW1 = os:timestamp(),
    write_to_cluster(NodeA, ?NUM_KEYS + 1, 2 * ?NUM_KEYS, new_obj),
    lager:info(
        "Write took ~w seconds", 
        [timer:now_diff(os:timestamp(), SW1) div (1000 * 1000)]),
    lager:info("Wait for queues to drain"),
    verify_vclock_prune:wait_for_queues_to_drain(ClusterA, cluster_b),

    lager:info("B1 state post load :~n~p", [get_snk_state(NodeB1)]),
    lager:info("B2 state post load :~n~p", [get_snk_state(NodeB2)]),
    StatsB1_1 = get_stats([NodeB1]),
    StatsB2_1 = get_stats([NodeB2]),
    lager:info("NodeB1 stats ~w", [StatsB1_1]),
    lager:info("NodeB2 stats ~w", [StatsB2_1]),
    B1_Fetches1 = element(4, StatsB1_1) - B1_Fetches0,
    B2_Fetches1 = element(4, StatsB2_1) - B2_Fetches0,
    ?assert(B2_Fetches1 > (2 * B1_Fetches1)),
    
    pass.

compare_peer_info(ExpectedPeers, Protocol) ->
    fun(Node) ->
        {Protocol, {IP, Port}} =
            lists:keyfind(Protocol, 1, rt:connection_info(Node)),
        MemberList =
            lists:map(
                fun({IPm, Portm}) ->
                    {list_to_binary(IPm), Portm}
                end,
                rpc:call(Node, riak_client, membership_request, [Protocol])),
        {Mod, RiakErlC} = 
            case Protocol of
                pb ->
                    {ok, Pid} = riakc_pb_socket:start(IP, Port),
                    {riakc_pb_socket, Pid};
                http ->
                    {rhc, rhc:create(IP, Port, "riak", [])}
            end,
        {ok, MemberList} = Mod:peer_discovery(RiakErlC),
        lager:info(
            "Discovered Member list (two ways) ~p ~p",
            [MemberList, Protocol]),
        ?assert(lists:member({list_to_binary(IP), Port}, MemberList)),
        ?assertMatch(ExpectedPeers, length(MemberList)),
        case Protocol of
            pb ->
                Mod:stop(RiakErlC);
            _ ->
                ok
        end
    end.

reset_peer_config(SnkCluster, ClusterName, PeerX) ->
    ClusterSNkCfg = ?SNK_CONFIG(ClusterName, PeerX),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterSNkCfg) end, SnkCluster).


get_stats(Cluster) ->
    Stats = {0, 0, 0, 0, 0, 0},
        % {prefetch, tofetch, nofetch, object, error, empty}
    lists:foldl(fun(N, {PFAcc, TFAcc, NFAcc, FOAcc, FErAcc, FEmAcc}) -> 
                        S = verify_riak_stats:get_stats(N, 1000),
                        {<<"ngrfetch_prefetch_total">>, PFT} =
                            lists:keyfind(<<"ngrfetch_prefetch_total">>, 1, S),
                        {<<"ngrfetch_tofetch_total">>, TFT} =
                            lists:keyfind(<<"ngrfetch_tofetch_total">>, 1, S),
                        {<<"ngrfetch_nofetch_total">>, NFT} =
                            lists:keyfind(<<"ngrfetch_nofetch_total">>, 1, S),
                        {<<"ngrrepl_object_total">>, FOT} =
                            lists:keyfind(<<"ngrrepl_object_total">>, 1, S),
                        {<<"ngrrepl_error_total">>, FErT} =
                            lists:keyfind(<<"ngrrepl_error_total">>, 1, S),
                        {<<"ngrrepl_empty_total">>, FEmT} =
                            lists:keyfind(<<"ngrrepl_empty_total">>, 1, S),
                        {PFT + PFAcc, TFT + TFAcc, NFT + NFAcc,
                            FOT + FOAcc, FErT + FErAcc, FEmAcc + FEmT}
                    end,
                    Stats,
                    Cluster).

%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End, CommonValBin) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            Obj = 
                case CommonValBin of
                    new_obj ->
                        CVB = ?COMMMON_VAL_INIT,
                        riak_object:new(?TEST_BUCKET,
                                        Key,
                                        <<N:32/integer, CVB/binary>>);
                    UpdateBin ->
                        UPDV = <<N:32/integer, UpdateBin/binary>>,
                        {ok, PrevObj} = riak_client:get(?TEST_BUCKET, Key, C),
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

get_snk_state(Node) ->
    P = rpc:call(Node, erlang, whereis, [riak_kv_replrtq_snk]),
    [{_QN, _I, SW}] = element(2, rpc:call(Node, sys, get_state, [P])),
    WQL = length(element(3, SW)),
    MQL = element(4, SW),
    DQL = element(5, SW),
    MWC = element(7, SW),
    [{work_queue_length, WQL},
        {minimum_queue_length, MQL},
        {deferred_queue_length, DQL},
        {max_worker_count, MWC}].