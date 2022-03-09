%% @doc
%% This module implements a riak_test to test the auto_check method
%% in nextgenrepl ttaaefs

-module(nextgenrepl_ttaaefs_autocheck).
-behavior(riak_test).
-export([confirm/0]).
-export([delete_from_cluster/3]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 8).
-define(B_RING, 32).
-define(C_RING, 16).
-define(A_NVAL, 1).
-define(B_NVAL, 3).
-define(C_NVAL, 2).
-define(STATS_WAIT, 1000).
-define(MEGA, 1000000).

-define(CONFIG(RingSize, NVal, LowHour, HighHour), [
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
           {ttaaefs_maxresults, 32},
           {ttaaefs_rangeboost, 4},
           {ttaaefs_checkwindow, {LowHour, HighHour}},
           {delete_mode, keep}
          ]}
        ]).

confirm() ->
    Now = os:timestamp(),
    [ClusterA1, ClusterB1, ClusterC1] = setup_clusters(Now),
    test_repl_between_clusters(ClusterA1, ClusterB1, ClusterC1, Now).

setup_clusters(Now) ->

    {_Date, {Hour, _Min, _Sec}} = calendar:now_to_datetime(Now),
    LowHour = (Hour + 2) rem 24,
    HighHour = (Hour + 3) rem 24,

    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL, LowHour, HighHour)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, LowHour, HighHour)},
            {2, ?CONFIG(?C_RING, ?C_NVAL, LowHour, HighHour)}]),
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    rt:join_cluster(ClusterC),
    
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB ++ ClusterC),
    
    lager:info("Ready for test."),
    [ClusterA, ClusterB, ClusterC].


test_repl_between_clusters(ClusterA, ClusterB, ClusterC, Now) ->
    
    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    NodeC = hd(ClusterC),

    AutoCheckFun = autosync_checkfun(Now),
    
    ok = setup_replqueues(ClusterA ++ ClusterB ++ ClusterC),

    lager:info("Test empty clusters don't show any differences"),
    {http, {IPA, PortA}} = lists:keyfind(http, 1, rt:connection_info(NodeA)),
    {http, {IPB, PortB}} = lists:keyfind(http, 1, rt:connection_info(NodeB)),
    {http, {IPC, PortC}} = lists:keyfind(http, 1, rt:connection_info(NodeC)),
    lager:info("Cluster A ~s ~w Cluster B ~s ~w Cluster C ~s ~w",
                [IPA, PortA, IPB, PortB, IPC, PortC]),
    
    lager:info("Auto check should root compare"),
    {root_compare, 0}
        = AutoCheckFun({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    {root_compare, 0}
        = AutoCheckFun({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeC, IPC, PortC, ?C_NVAL}),
    {root_compare, 0}
        = AutoCheckFun({NodeC, IPC, PortC, ?C_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL}),
    
    lists:foreach(fun(N) -> ?assertMatch(none, get_range(N)) end,
                    [NodeA, NodeB, NodeC]),


    lager:info("Test 100 key difference and resolve"),
    % Write keys to cluster A, verify B and C do not have them.
    write_to_cluster(NodeA, 1, 100),
    read_from_cluster(NodeB, 1, 100, 100),
    read_from_cluster(NodeC, 1, 100, 100),
    {clock_compare, 0}
        = AutoCheckFun({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),

    PostWriteCheckFun = autosync_checkfun(os:timestamp()),
    {clock_compare,32}
        = PostWriteCheckFun({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),

    ?assertMatch(?TEST_BUCKET, element(1, get_range(NodeA))),
    ?assertMatch(all, element(2, get_range(NodeA))),
    ?assertMatch(none, get_range(NodeB)),
    lager:info("Range check now resolves A -> B"),
    {clock_compare, 68}
        = PostWriteCheckFun({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    ?assertMatch(?TEST_BUCKET, element(1, get_range(NodeA))),
    ?assertMatch(all, element(2, get_range(NodeA))),
    {root_compare, 0}
        = PostWriteCheckFun({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    ?assertMatch(none, get_range(NodeA)),

    {Mega, Sec, MicroSec} = Now,
    Later = Mega * ?MEGA + Sec + 3600 * 2,
    AllCheckFun =
        autosync_checkfun({Later div ?MEGA, Later rem ?MEGA, MicroSec}),
    
    {root_compare, 0}
        = AllCheckFun({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL}),
    {clock_compare, 32}
        = AllCheckFun({NodeB, IPB, PortB, ?B_NVAL},
                            {NodeC, IPC, PortC, ?C_NVAL}),
    {clock_compare, 32}
        = AllCheckFun({NodeC, IPC, PortC, ?C_NVAL},
                            {NodeA, IPA, PortA, ?A_NVAL}),


    ?assertMatch({1, 3, 0, 2, 3, 3, 100, 0}, get_stats(NodeA)),
    ?assertMatch({1, 1, 0, 0, 1, 1, 32, 0}, get_stats(NodeB)),
    ?assertMatch({1, 1, 0, 0, 1, 1, 0, 32}, get_stats(NodeC)),

    pass.


get_range(Node) ->
    rpc:call(Node, riak_kv_ttaaefs_manager, get_range, []).

setup_replqueues([]) ->
    ok;
setup_replqueues([HeadNode|Others]) ->
    false = rpc:call(HeadNode,
                    riak_kv_replrtq_src,
                    register_rtq,
                    [q1_ttaaefs, block_rtq]),
        % false indicates this queue is already defined by default
    setup_replqueues(Others).

autosync_checkfun(Now) ->
    fun({SrcNode, SrcIP, SrcPort, SrcNVal},
            {SinkNode, SinkIP, SinkPort, SinkNVal}) ->
        ModRef = riak_kv_ttaaefs_manager,
        _ = rpc:call(SrcNode, ModRef, pause, []),
        ok = rpc:call(SrcNode, ModRef, set_sink, [http, SinkIP, SinkPort]),
        ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
        AAEResult =
            rpc:call(SrcNode, riak_client, ttaaefs_fullsync,
                [auto_check, 60, Now]),
        SrcHTTPC = rhc:create(SrcIP, SrcPort, "riak", []),
        {ok, SnkC} = riak:client_connect(SinkNode),
        N = drain_queue(SrcHTTPC, SnkC),
        lager:info("Drained queue and pushed ~w objects", [N]),
        AAEResult
    end.

drain_queue(SrcClient, SnkClient) ->
    drain_queue(SrcClient, SnkClient, 0).

drain_queue(SrcClient, SnkClient, N) ->
    case rhc:fetch(SrcClient, q1_ttaaefs) of
        {ok, queue_empty} ->
            N;
        {ok, {deleted, _TombClock, RObj}} ->
            {ok, _LMD} = riak_client:push(RObj, true, [], SnkClient),
            drain_queue(SrcClient, SnkClient, N + 1);
        {ok, RObj} ->
            {ok, _LMD} = riak_client:push(RObj, false, [], SnkClient),
            drain_queue(SrcClient, SnkClient, N + 1)
    end.


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


%% @doc Read from cluster a series of keys, asserting a certain number
%%      of errors.
read_from_cluster(Node, Start, End, Errors) ->
    CommonValBin = <<"CommonValueToWriteForAllObjects">>,
    read_from_cluster(Node, Start, End, Errors, ?TEST_BUCKET, CommonValBin).

read_from_cluster(Node, Start, End, Errors, Bucket, CommonValBin) ->
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
    ErrorsFound = lists:foldl(F, [], lists:seq(Start, End)),
    case Errors of
        undefined ->
            lager:info("Errors Found in read_from_cluster ~w",
                        [length(ErrorsFound)]),
            length(ErrorsFound);
        _ ->
            ?assertEqual(Errors, length(ErrorsFound))
    end.

key(N) ->
    list_to_binary(io_lib:format("~8..0B~n", [N])).

get_stats(Node) ->
    lager:info("Fetching stats from ~w", [Node]),
    S = verify_riak_stats:get_stats(Node, ?STATS_WAIT),
    {_, ACT} = lists:keyfind(<<"ttaaefs_allcheck_total">>, 1, S),
    {_, DCT} = lists:keyfind(<<"ttaaefs_daycheck_total">>, 1, S),
    {_, HCT} = lists:keyfind(<<"ttaaefs_hourcheck_total">>, 1, S),
    {_, RCT} = lists:keyfind(<<"ttaaefs_rangecheck_total">>, 1, S),
    {_, SST} = lists:keyfind(<<"ttaaefs_sync_total">>, 1, S),
    {_, NST} = lists:keyfind(<<"ttaaefs_nosync_total">>, 1, S),
    {_, SrcAT} = lists:keyfind(<<"ttaaefs_src_ahead_total">>, 1, S),
    {_, SnkAT} = lists:keyfind(<<"ttaaefs_snk_ahead_total">>, 1, S),
    {_, STimeMax} = lists:keyfind(<<"ttaaefs_sync_time_100">>, 1, S),
    {_, NSTimeMax} = lists:keyfind(<<"ttaaefs_nosync_time_100">>, 1, S),
    lager:info(
        "Stats all_check=~w day_check=~w hour_check=~w range_check=~w",
        [ACT, DCT, HCT, RCT]),
    lager:info(
        "Stats sync=~w nosync=~w src_ahead=~w snk_ahead=~w",
        [SST, NST, SrcAT, SnkAT]),
    lager:info(
        "Stats max_sync_time=~w ms max_nosync_time=~w ms",
        [STimeMax div 1000, NSTimeMax div 1000]),
    {ACT, DCT, HCT, RCT, SST, NST, SrcAT, SnkAT}.