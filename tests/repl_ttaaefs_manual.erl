%% @doc
%% This module implements a riak_test to exercise the Active
%% Anti-Entropy Fullsync replication.  It sets up two clusters, runs a
%% fullsync over all partitions, and verifies the missing keys were
%% replicated to the sink cluster.

-module(repl_ttaaefs_manual).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).

-define(CONFIG(RingSize, NVal), [
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
           {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
          ]}
        ]).

confirm() ->
    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {2, ?CONFIG(8, 1)},
            {2, ?CONFIG(16, 1)},
            {2, ?CONFIG(32, 3)}]),
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    rt:join_cluster(ClusterC),

    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    NodeC = hd(ClusterC),

    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),

    lager:info("Test empty clusters don't show any differences"),
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(NodeA)),
    {http, {IPB, PortB}} = lists:keyfind(http, 1, rt:connection_info(NodeB)),
    {http, {IPC, PortC}} = lists:keyfind(http, 1, rt:connection_info(NodeC)),
    lager:info("Cluster A ~s ~w Cluster B ~s ~w Cluster C ~s ~w",
                [IPA, PortA, IPB, PortB, IPC, PortC]),
    
    {root_compare, 0}
        = fullsync_check(NodeA, {IPA, PortA}, {IPB, PortB}, 1, 1),
    {root_compare, 0}
        = fullsync_check(NodeB, {IPB, PortB}, {IPC, PortC}, 1, 3),
    {root_compare, 0}
        = fullsync_check(NodeC, {IPC, PortC}, {IPA, PortA}, 3, 1),

    lager:info("Test 100 key difference and resolve"),
    % Write keys to cluster A, verify B and C do not have them.
    write_to_cluster(NodeA, 1, 100),
    read_from_cluster(NodeB, 1, 100, 100),
    read_from_cluster(NodeC, 1, 100, 100),
    {clock_compare, 100}
        = fullsync_check(NodeA, {IPA, PortA}, {IPB, PortB}, 1, 1),
    {clock_compare, 100}
        = fullsync_check(NodeB, {IPB, PortB}, {IPC, PortC}, 1, 3),
    % Now node 3 should align with node 1 
    {root_compare, 0}
        = fullsync_check(NodeC, {IPC, PortC}, {IPA, PortA}, 3, 1),
    read_from_cluster(NodeA, 1, 100, 0),
    read_from_cluster(NodeB, 1, 100, 0),
    read_from_cluster(NodeC, 1, 100, 0),
    {root_compare, 0}
        = fullsync_check(NodeA, {IPA, PortA}, {IPB, PortB}, 1, 1),
    {root_compare, 0}
        = fullsync_check(NodeB, {IPB, PortB}, {IPC, PortC}, 1, 3),

    lager:info("Test 1000 key difference and resolve"),
    write_to_cluster(NodeA, 101, 1100),
    read_from_cluster(NodeB, 101, 1100, 1000),
    read_from_cluster(NodeC, 101, 1100, 1000),

    {clock_compare, N1}
        = fullsync_check(NodeA, {IPA, PortA}, {IPB, PortB}, 1, 1),
    lager:info("First comparison found ~w differences", [N1]),
    ?assertEqual(true, N1 > 100),
    ?assertEqual(true, N1 < 1000),

    lager:info("Further eight loops should complete repair"),
    LoopRepairFun =
        fun(Node, Source, Sink, SrcNVal, SinkNVal) ->
            fun(_I) ->
                fullsync_check(Node, Source, Sink, SrcNVal, SinkNVal)
            end
        end,
    lists:foreach(LoopRepairFun(NodeA, {IPA, PortA}, {IPB, PortB}, 1, 1),
                    lists:seq(1, 8)),
    {root_compare, 0} =
        fullsync_check(NodeA, {IPA, PortA}, {IPB, PortB}, 1, 1),
    {root_compare, 0} =
        fullsync_check(NodeB, {IPB, PortB}, {IPA, PortA}, 1, 1),
    lager:info("NodeA and NodeB has been re-sync'd"),
    read_from_cluster(NodeB, 1, 1100, 0),

    lager:info("Repairing in the wrong direction doesn't repair"),
    {clock_compare, 128} =
        fullsync_check(NodeC, {IPC, PortC}, {IPB, PortB}, 3, 1),
    read_from_cluster(NodeC, 101, 1100, 1000),

    lager:info("Complete repair from different clusters"),
    lists:foreach(LoopRepairFun(NodeA, {IPA, PortA}, {IPC, PortC}, 1, 3),
                    lists:seq(1, 5)),
    lists:foreach(LoopRepairFun(NodeB, {IPB, PortB}, {IPC, PortC}, 1, 3),
                    lists:seq(1, 5)),
    read_from_cluster(NodeC, 1, 1100, 0),

    pass.


fullsync_check(SrcNode, {SrcIP, SrcPort}, {SinkIP, SinkPort}, SrcNVal, SinkNVal) ->
    _ = rpc:call(SrcNode, riak_client, ttaaefs_pause, []),
    ok = rpc:call(SrcNode, riak_client, ttaaefs_setsource, [http, SrcIP, SrcPort]),
    ok = rpc:call(SrcNode, riak_client, ttaaefs_setsink, [http, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, riak_client, ttaaefs_setallsync, [SrcNVal, SinkNVal]),
    rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_sync, 60]).


%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    CommonValBin = <<"CommonValueToWriteForAllObjects">>,
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            Obj = riak_object:new(?TEST_BUCKET,
                                    Key,
                                    <<N:32/integer, CommonValBin/binary>>),
            try C:put(Obj) of
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
read_from_cluster(Node, Start, End, Errors) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
    CommonValBin = <<"CommonValueToWriteForAllObjects">>,
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            case  C:get(?TEST_BUCKET, Key) of
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
    ErrorsFound = lists:foldl(F, [], lists:seq(Start, End)),
    ?assertEqual(Errors, length(ErrorsFound)).
