%% @doc
%% What happens when we run AAE full-sync between clusters with different
%% delete_modes.  The answer is problematic - tombstones differ from nothing
%% (as that is the point of a tombstone), and so we can't expect two clusters
%% that have had the same operations to agree.

-module(nextgenrepl_deletewithfailure).
-behavior(riak_test).
-export([confirm/0]).
-export([read_from_cluster/5, aae_fold/2, length_aae_fold/2]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_del">>).
-define(A_RING, 32).
-define(B_RING, 8).
-define(A_NVAL, 3).
-define(B_NVAL, 1).

-define(KEY_COUNT, 10000).
-define(UPDATE_COUNT, 2000).
-define(LOOP_COUNT, 10).
-define(ACTION_DELAY, 2000).

-define(SNK_WORKERS, 4).

-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(CONFIG(RingSize, NVal, DeleteMode), [
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
            {tictacaae_storeheads, true},
            {tictacaae_rebuildwait, 4},
            {tictacaae_rebuilddelay, 3600},
            {tictacaae_exchangetick, 2000},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {delete_mode, DeleteMode}
          ]}
        ]).

-define(REPL_CONFIG(LocalClusterName, PeerList, SrcQueueDefns), [
    {riak_kv,
        [
            {replrtq_srcqueue, SrcQueueDefns},
            {replrtq_enablesink, true},
            {replrtq_sinkqueue, LocalClusterName},
            {replrtq_sinkpeers, PeerList},
            {replrtq_sinkworkers, ?SNK_WORKERS}
        ]}
]).


repl_config(RemoteCluster, LocalClusterName, PeerList) ->
    ?REPL_CONFIG(LocalClusterName,
                    PeerList,
                    atom_to_list(RemoteCluster) ++ ":any").


confirm() ->
    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {5, ?CONFIG(?A_RING, ?A_NVAL, keep)},
            {1, ?CONFIG(?B_RING, ?B_NVAL, immediate)}]),

    lager:info("Test run using PB protocol an a mix of delete modes"),
    test_repl(pb, [ClusterA, ClusterB]),
    
    pass.


test_repl(Protocol, [ClusterA, ClusterB]) ->

    [NodeA1, NodeA2, NodeA3, NodeA4, NodeA5] = ClusterA,
    [NodeB1] = ClusterB,

    FoldToPeerConfig = 
        fun(Node, Acc) ->
            {Protocol, {IP, Port}} =
                lists:keyfind(Protocol, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port)
                ++ ":" ++ atom_to_list(Protocol)
        end,
    ClusterASnkPL = lists:foldl(FoldToPeerConfig, "", ClusterB),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfig, "", ClusterA),

    ACfg = repl_config(cluster_b, cluster_a, ClusterASnkPL),
    BCfg = repl_config(cluster_a, cluster_b, ClusterBSnkPL),
    rt:set_advanced_conf(NodeA1, ACfg),
    rt:set_advanced_conf(NodeA2, ACfg),
    rt:set_advanced_conf(NodeA3, ACfg),
    rt:set_advanced_conf(NodeA4, ACfg),
    rt:set_advanced_conf(NodeA5, ACfg),
    rt:set_advanced_conf(NodeB1, BCfg),

    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB),
    
    lager:info("Wait for compare between empty clusters"),
    timer:sleep(10000),
    {Protocol, {_NodeA1ip, _NodeA1port}} =
        lists:keyfind(Protocol, 1, rt:connection_info(NodeA1)),
    {Protocol, {NodeB1ip, NodeB1port}} =
        lists:keyfind(Protocol, 1, rt:connection_info(NodeB1)),
    {root_compare, 0} =
        fullsync_check(Protocol,
                        {NodeA1, ?A_NVAL, cluster_a},
                        {NodeB1ip, NodeB1port, ?B_NVAL}),
    
    lager:info("Initial data load and delete"),
    SW0A = os:timestamp(),
    timer:sleep(?ACTION_DELAY),
    write_then_delete(NodeA1, NodeB1, 1, ?KEY_COUNT),
    timer:sleep(?ACTION_DELAY),
    SW0B = os:timestamp(),
    timer:sleep(?ACTION_DELAY),
    SW1A = os:timestamp(),
    timer:sleep(?ACTION_DELAY),
    write_then_delete(NodeA1, NodeB1, ?KEY_COUNT + 1, ?KEY_COUNT * 2),
    timer:sleep(?ACTION_DELAY),
    SW1B = os:timestamp(),

    {ok, K0} = aae_fold(NodeA1,
                        {erase_keys,
                            ?TEST_BUCKET, all,
                            all,
                            {date, ts_epoch(SW0A), ts_epoch(SW0B)},
                            count}),
    lager:info("Counted ~w active keys on A1 from first time range", [K0]),
    {ok, K1} = aae_fold(NodeA1,
                        {erase_keys,
                            ?TEST_BUCKET, all,
                            all,
                            {date, ts_epoch(SW1A), ts_epoch(SW1B)},
                            count}),
    lager:info("Counted ~w active keys on A1 from second time range", [K1]),
    {ok, KA} = aae_fold(NodeA1,
                        {erase_keys,
                            ?TEST_BUCKET, all,
                            all,
                            all,
                            count}),
    lager:info("Counted ~w active keys on A1 all time", [KA]),
    {ok, T0} = aae_fold(NodeA1,
                        {reap_tombs,
                            ?TEST_BUCKET, all,
                            all,
                            {date, ts_epoch(SW0A), ts_epoch(SW0B)},
                            count}),
    lager:info("Counted ~w tombs on A1 from first time range", [T0]),
    {ok, T1} = aae_fold(NodeA1,
                        {reap_tombs,
                            ?TEST_BUCKET, all,
                            all,
                            {date, ts_epoch(SW1A), ts_epoch(SW1B)},
                            count}),
    lager:info("Counted ~w tombs on A1 from second time range", [T1]),
    {ok, TA} = aae_fold(NodeA1,
                        {reap_tombs,
                            ?TEST_BUCKET, all,
                            all,
                            all,
                            count}),
    lager:info("Counted ~w tombs on A1 all time", [TA]),
    {ok, KB} = aae_fold(NodeB1,
                        {erase_keys,
                            ?TEST_BUCKET, all,
                            all,
                            all,
                            count}),
    lager:info("Counted ~w active keys on B1 all time", [KB]),
    {ok, TB} = aae_fold(NodeB1,
                        {reap_tombs,
                            ?TEST_BUCKET, all,
                            all,
                            all,
                            count}),
    lager:info("Counted ~w tombs on B1 all time", [TB]),
    {ok, SKLA0} = aae_fold(NodeA1,
                            {find_keys, 
                                ?TEST_BUCKET, all,
                                {date, ts_epoch(SW0A), ts_epoch(SW0B)},
                                {sibling_count, 1}}),
    lager:info("Counted ~w siblings on A1 - first timerange", [length(SKLA0)]),
    {ok, SKLA1} = aae_fold(NodeA1,
                            {find_keys, 
                                ?TEST_BUCKET, all,
                                {date, ts_epoch(SW1A), ts_epoch(SW1B)},
                                {sibling_count, 1}}),
    lager:info("Counted ~w siblings on A1 - second timerange", [length(SKLA1)]),
    {ok, SKLB} = aae_fold(NodeB1,
                            {find_keys, 
                                ?TEST_BUCKET, all,
                                all,
                                {sibling_count, 1}}),
    lager:info("Counted ~w siblings on B1", [length(SKLB)]),
    KeyCount0 = 2 * ?UPDATE_COUNT, %% 2 lots of updates in each time period
    TombCount0 = ?KEY_COUNT - KeyCount0,

    ?assertMatch(KeyCount0, K0),
    ?assertMatch(KeyCount0, K1),
    ?assertMatch(TombCount0, T0),
    ?assertMatch(TombCount0, T1),
    ?assertMatch(true, KA == KB),
    ?assertMatch(0, TB),
    ?assertMatch(TA, T0 + T1),
    ?assertMatch(KB, K0 + K1),
    ?assertMatch(?KEY_COUNT, K0 + T0),
    ?assertMatch(?KEY_COUNT, K1 + T1),
    ?assertEqual(0, length(SKLA0) - ?UPDATE_COUNT),
    ?assertEqual(0, length(SKLA1) - ?UPDATE_COUNT),
    ?assertEqual(0, length(SKLB)),

    [{B, SK0, 2}|_RestA0] = SKLA0,
    {B, EK0, 2} = lists:last(SKLA0),
    [{B, SK1, 2}|_RestA1] = SKLA1,
    {B, EK1, 2} = lists:last(SKLA1),
    lager:info("Erasing partial delete siblings from Node"),
    {ok, EraseCount0} =
        aae_fold(NodeA1,
                    {erase_keys,
                        ?TEST_BUCKET, {SK0, EK0},
                        all,
                        {date, ts_epoch(SW0A), ts_epoch(SW0B)},
                        local}),
    {ok, EraseCount1} =
        aae_fold(NodeA1,
                    {erase_keys,
                        ?TEST_BUCKET, {SK1, EK1},
                        all,
                        {date, ts_epoch(SW1A), ts_epoch(SW1B)},
                        {job, 1}}),
    lager:info("re-counting siblings until there are none"),
    0 = wait_for_outcome(?MODULE,
                            length_aae_fold,
                            [NodeA1,
                                {find_keys, 
                                    ?TEST_BUCKET, all, all,
                                    {sibling_count, 1}}],
                                0,
                                ?LOOP_COUNT),
    0 = wait_for_outcome(?MODULE,
                            length_aae_fold,
                            [NodeB1,
                                {find_keys, 
                                    ?TEST_BUCKET, all, all,
                                    {sibling_count, 1}}],
                                0,
                                ?LOOP_COUNT),
    ?assertEqual(?UPDATE_COUNT, EraseCount0),
    ?assertEqual(?UPDATE_COUNT, EraseCount1),
    
    {ok, TombCount0} =
        aae_fold(NodeA1,
                    {reap_tombs,
                        ?TEST_BUCKET, all, all,
                        {date, ts_epoch(SW0A), ts_epoch(SW0B)},
                        {job, 1}}),
    {ok, TombCount1} =
        aae_fold(NodeA1,
                    {reap_tombs,
                        ?TEST_BUCKET, all, all,
                        {date, ts_epoch(SW1A), ts_epoch(SW1B)},
                        local}),

    ExpectedEC = EraseCount0 + EraseCount1,
    {ok, ExpectedEC} =
        wait_for_outcome(?MODULE,
                            aae_fold,
                            [NodeA1,
                                {reap_tombs,
                                    ?TEST_BUCKET, all, all, all,
                                    count}],
                            {ok, ExpectedEC},
                            ?LOOP_COUNT),
    {ok, 0} =
        wait_for_outcome(?MODULE,
                            aae_fold,
                            [NodeB1,
                                {reap_tombs,
                                    ?TEST_BUCKET, all, all, all,
                                    count}],
                            {ok, 0},
                            ?LOOP_COUNT),

    ?assertMatch(T0, TombCount0),
    ?assertMatch(T1, TombCount1),

    {ok, Phase1KeyCount} =
        aae_fold(NodeA1,
                    {erase_keys,
                        ?TEST_BUCKET, all, all, all,
                        count}),
    lager:info("Cluster A has ~w keys and ~w tombs at Phase 1 exit",
                    [Phase1KeyCount, ExpectedEC]),
    
    rt:stop_and_wait(NodeA5),
    timer:sleep(2000),
        % There's no pre-built function to confirm all is stable after this
        % but it should be stable soon after - so sleep
    lager:info("Node 5 has stopped in Cluster A"),
    {ok, Phase2TombCountS1} =
        aae_fold(NodeA1,
                    {reap_tombs,
                        ?TEST_BUCKET, all, all, all,
                        {job, 2}}),
        % Need to reap before we erase if we want to check the key count
        % As the erase will generate more tombstones
    {ok, Phase2KeyCountS1} =
        aae_fold(NodeA1,
                    {erase_keys,
                        ?TEST_BUCKET, all, all, all,
                        {job, 2}}),
        % Need to reap before we erase if we want to check the key count
        % As the erase will generate more tombstones
    ?assertMatch(ExpectedEC, Phase2TombCountS1),
    ?assertMatch(Phase1KeyCount, Phase2KeyCountS1),

    {ok, Phase2TombCountS2} =
        wait_until_stable(?MODULE,
                            aae_fold,
                            [NodeA1,
                                {reap_tombs,
                                    ?TEST_BUCKET, all, all, all,
                                    count}],
                            undefined,
                            ?LOOP_COUNT),
    {ok, Phase2KeyCountS2} =
        wait_until_stable(?MODULE,
                            aae_fold,
                            [NodeA1,
                                {erase_keys,
                                    ?TEST_BUCKET, all, all, all,
                                    count}],
                            undefined,
                            ?LOOP_COUNT),

    lager:info("After reap/erase duing fail - tombs ~w keys ~w",
                [Phase2TombCountS2, Phase2KeyCountS2]),
    ?assertMatch(true, Phase2TombCountS2 > 0),
    ?assertMatch(true, Phase2KeyCountS2 == 0),
        %% As this is a cluster with delete_mode of keep, then the deletes
        %% will not be queued like the tombstones - as with a delete_mode
        %% of keep deletes can happen on a best endeavours basis
    ?assertMatch(true, Phase2KeyCountS2 < Phase2KeyCountS1),

    rt:start_and_wait(NodeA5),
    lager:info("Node 5 has re-started in Cluster A"),

    timer:sleep(10000),
    {ok, Phase3TombCountS1} =
        wait_until_stable(?MODULE,
                            aae_fold,
                            [NodeA1,
                                {reap_tombs,
                                    ?TEST_BUCKET, all, all, all,
                                    count}],
                            undefined,
                            ?LOOP_COUNT),
    {ok, Phase3KeyCountS1} =
        wait_until_stable(?MODULE,
                            aae_fold,
                            [NodeA1,
                                {erase_keys,
                                    ?TEST_BUCKET, all, all, all,
                                    count}],
                            undefined,
                            ?LOOP_COUNT),
    lager:info("After node restart - tombs ~w keys ~w",
                [Phase3TombCountS1, Phase3KeyCountS1]),
    ?assertMatch(0, Phase3KeyCountS1),
        %% All deletes eventually happen
    ?assertMatch(Phase1KeyCount, Phase3TombCountS1),
        %% The keys deleted since phase 1 will now be tombstones
    {ok, KB3} = aae_fold(NodeB1,
                        {erase_keys,
                            ?TEST_BUCKET, all,
                            all,
                            all,
                            count}),
    {ok, TB3} = aae_fold(NodeB1,
                        {reap_tombs,
                            ?TEST_BUCKET, all,
                            all,
                            all,
                            count}),
    ?assertMatch(0, KB3),
    ?assertMatch(0, TB3),
        %% Cluster B should have no keys and no tombs

    pass.

fullsync_check(Protocol, {SrcNode, SrcNVal, SnkCluster},
                {SinkIP, SinkPort, SinkNVal}) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_queuename, [SnkCluster]),
    ok = rpc:call(SrcNode, ModRef, set_sink, [Protocol, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_sync, 60]),
    AAEResult.

length_aae_fold(Node, Query) ->
    {ok, List} = aae_fold(Node, Query),
    length(List).

aae_fold(Node, Query) ->
    {ok, C} = riak:client_connect(Node),
    riak_client:aae_fold(Query, C).


%% @doc Write a series of keys and ensure they are all written.
write_to_cluster(Node, Start, End, CommonValBin) ->
    lager:info("Writing ~p keys to node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    PB = rt:pbc(Node),
    B = ?TEST_BUCKET,
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            Obj = 
                case CommonValBin of
                    new_obj ->
                        CVB = ?COMMMON_VAL_INIT,
                        riakc_obj:new(B, Key, <<N:32/integer, CVB/binary>>);
                    UpdateBin ->
                        UPDV = <<N:32/integer, UpdateBin/binary>>,
                        Opts = [deletedvclock],
                        case riakc_pb_socket:get(PB, B, Key, Opts) of
                            {ok, PrevObj} ->
                                riakc_obj:update_value(PrevObj, UPDV);
                            {error, notfound, DVC} ->
                                NewObj = riakc_obj:new(B, Key, UPDV),
                                riakc_obj:set_vclock(NewObj, DVC);
                            {error, notfound} ->
                                riakc_obj:new(B, Key, UPDV)
                        end
                end,
            try riakc_pb_socket:put(PB, Obj) of
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
    lager:info("Deleting ~p keys from node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            try C:delete(?TEST_BUCKET, Key) of
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

read_from_cluster(Node, Start, End, CommonValBin, Errors) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
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
                            [{wrong_value, Key, UnexpectedVal}|Acc]
                    end;
                {error, Error} ->
                    [{fetch_error, Error, Key}|Acc]
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

wait_for_outcome(Module, Func, Args, ExpOutcome, Loops) ->
    wait_for_outcome(Module, Func, Args, ExpOutcome, 0, Loops).

wait_for_outcome(Module, Func, Args, _ExpOutcome, LoopCount, LoopCount) ->
    apply(Module, Func, Args);
wait_for_outcome(Module, Func, Args, ExpOutcome, LoopCount, MaxLoops) ->
    case apply(Module, Func, Args) of
        ExpOutcome ->
            ExpOutcome;
        NotRightYet ->
            lager:info("~w not yet ~w ~w", [Func, ExpOutcome, NotRightYet]),
            timer:sleep(LoopCount * 2000),
            wait_for_outcome(Module, Func, Args, ExpOutcome,
                                LoopCount + 1, MaxLoops)
    end.

wait_until_stable(_Module, _Func, _Args, LastResult, 0) ->
    LastResult;
wait_until_stable(Module, Func, Args, LastResult, LoopCount) ->
    case apply(Module, Func, Args) of
        LastResult ->
            LastResult;
        ThisResult ->
            timer:sleep(2000),
            wait_until_stable(Module, Func, Args, ThisResult, LoopCount - 1)
    end.

write_then_delete(NodeA1, NodeB1, Start, End) ->
    lager:info("Write ~w objects into A and read from B and C",
                [End - Start + 1]),
    write_to_cluster(NodeA1, Start, End, new_obj),
    lager:info("Waiting to read sample"),
    0 = 
        wait_for_outcome(?MODULE,
                            read_from_cluster,
                            [NodeB1, End - 31, End,
                                ?COMMMON_VAL_INIT, undefined],
                            0,
                            ?LOOP_COUNT),
    lager:info("Waiting to read all"),
    0 = 
        wait_for_outcome(?MODULE,
                            read_from_cluster,
                            [NodeB1, Start, End, ?COMMMON_VAL_INIT, undefined],
                            0,
                            ?LOOP_COUNT),
    
    lager:info("Deleting ~w objects from B and read not_found from A and C",
                [?KEY_COUNT]),
    delete_from_cluster(NodeB1, Start, End),
    lager:info("Waiting for missing sample"),
    32 =
        wait_for_outcome(?MODULE,
                        read_from_cluster,
                        [NodeA1, End - 31, End,
                            ?COMMMON_VAL_INIT, undefined],
                        32,
                        ?LOOP_COUNT),
    lager:info("Waiting for all missing"),
    ?KEY_COUNT =
        wait_for_outcome(?MODULE,
                        read_from_cluster,
                        [NodeA1, Start, End, ?COMMMON_VAL_INIT, undefined],
                        ?KEY_COUNT,
                        ?LOOP_COUNT),
    lager:info("Add ~w updates to A",
                [?UPDATE_COUNT]),
    write_to_cluster(NodeA1,
                        1 + End - (2 * ?UPDATE_COUNT), End - ?UPDATE_COUNT,
                        ?COMMMON_VAL_MOD),
    lager:info("Add ~w updates to B - should generate siblings on A",
                [?UPDATE_COUNT]),
    write_to_cluster(NodeB1,
                        End + 1 - ?UPDATE_COUNT, End,
                        ?COMMMON_VAL_MOD),
    lager:info("Write and delete cycle confirmed").

ts_epoch({MegaSecs, Secs, _MicroSecs}) ->
    Secs + 1000000 * MegaSecs.