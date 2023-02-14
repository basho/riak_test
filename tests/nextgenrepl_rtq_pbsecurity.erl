%% @doc
%% This module implements a riak_test to exercise real-time replication
%% wiht pb security enabled

-module(nextgenrepl_rtq_pbsecurity).
-behavior(riak_test).
-export([confirm/0]).
-export([fullsync_check/4, simple_testsync/4]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, <<"repl-aae-fullsync-systest_a">>).
-define(A_RING, 16).
-define(B_RING, 32).
-define(C_RING, 8).
-define(A_NVAL, 4).
-define(B_NVAL, 2).
-define(C_NVAL, 3).

-define(SNK_WORKERS, 8).
-define(COMMMON_VAL_INIT, <<"CommonValueToWriteForAllObjects">>).
-define(COMMMON_VAL_MOD, <<"CommonValueToWriteForAllModifiedObjects">>).

-define(REPL_SLEEP, 2048). 
    % May need to wait for 2 x the 1024ms max sleep time of a snk worker
-define(WAIT_LOOPS, 12).

-define(CONFIG(RingSize, NVal, CertDir, User), [
        {riak_core,
            [
            {ring_creation_size, RingSize},
            {default_bucket_props,
                [
                    {n_val, NVal},
                    {allow_mult, true},
                    {dvv_enabled, true}
                ]},
            {ssl, [
                {certfile,
                    filename:join([CertDir, "site3.basho.com/cert.pem"])},
                {keyfile,
                    filename:join([CertDir, "site3.basho.com/key.pem"])},
                {cacertfile,
                    filename:join([CertDir, "site3.basho.com/cacerts.pem"])}
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
            {repl_cacert_filename,
                filename:join([CertDir, User ++ "/cacerts.pem"])},
            {repl_cert_filename,
                filename:join([CertDir, User ++ "/cert.pem"])},
            {repl_key_filename,
                filename:join([CertDir, User ++ "/key.pem"])},
            {repl_username, User}
          ]}
        ]).

confirm() ->

    CertDir = rt_config:get(rt_scratch_dir) ++ "/pb_security_certs_repl",
    pb_security:setup_pb_certificates(CertDir),
    make_certs:gencrl(CertDir, "site1.basho.com"),
    %% start a HTTP server to serve the CRLs
    %%
    %% NB: we use the 'stand_alone' option to link the server to the
    %% test process, so it exits when the test process exits.
    {ok, _HTTPPid} =
        inets:start(httpd,
                    [{port, 8000},
                        {server_name, "localhost"},
                        {server_root, "/tmp"},
                        {document_root, CertDir},
                        {modules, [mod_get]}],
                    stand_alone),

    {ClusterA, ClusterB, ClusterC} = setup_cluster(CertDir, "site4.basho.com"),
    lager:info("Creating a certificate-authenticated user"),
    change_user(add_user, "site4.basho.com", ClusterA, ClusterB, ClusterC),
    add_source("site4.basho.com", ClusterA, ClusterB, ClusterC),
    change_user(add_user, "site5.basho.com", ClusterA, ClusterB, ClusterC),
    add_source("site5.basho.com", ClusterA, ClusterB, ClusterC),

    lager:info("Ready for test."),
    test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC, CertDir),
    
    lager:info("site5.basho.com has a revoked certificate - sync failure"),
    {error, timeout} =
        simple_testsync("site5.basho.com", CertDir, ClusterA, ClusterB),

    lager:info("Remove site4.basho.com as a user - sync failure"),
    change_user(del_user, "site4.basho.com", ClusterA, ClusterB, ClusterC),
    {error, timeout} =
        simple_testsync("site4.basho.com", CertDir, ClusterA, ClusterB),
    
    lager:info("Reimplement site4.basho.com as user - sync works"),
    change_user(add_user, "site4.basho.com", ClusterA, ClusterB, ClusterC),
    lager:info("Source is removed when user removed so re-add"),
    add_source("site4.basho.com", ClusterA, ClusterB, ClusterC),
    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            simple_testsync,
                            ["site4.basho.com", CertDir, ClusterA, ClusterB],
                            {root_compare, 0},
                            ?WAIT_LOOPS),
    
    lager:info("Rebuild cluster with site5 for real-time repl"),
    rt:clean_cluster(ClusterA),
    rt:clean_cluster(ClusterB),
    rt:clean_cluster(ClusterC),
    {ClusterA, ClusterB, ClusterC} = setup_cluster(CertDir, "site5.basho.com"),
    lager:info("Creating a certificate-authenticated user"),
    change_user(add_user, "site4.basho.com", ClusterA, ClusterB, ClusterC),
    add_source("site4.basho.com", ClusterA, ClusterB, ClusterC),
    change_user(add_user, "site5.basho.com", ClusterA, ClusterB, ClusterC),
    add_source("site5.basho.com", ClusterA, ClusterB, ClusterC),

    lager:info("Ready for test."),
    true = test_basic_repl_failure(hd(ClusterA), hd(ClusterB), hd(ClusterC)),

    pass.


setup_cluster(CertDir, User) ->
    [ClusterA, ClusterB, ClusterC] =
        rt:deploy_clusters([
            {2, ?CONFIG(?A_RING, ?A_NVAL, CertDir, User)},
            {2, ?CONFIG(?B_RING, ?B_NVAL, CertDir, User)},
            {2, ?CONFIG(?C_RING, ?C_NVAL, CertDir, User)}
        ]),
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    rt:join_cluster(ClusterC),
    
    lager:info("Waiting for convergence."),
    rt:wait_until_ring_converged(ClusterA),
    rt:wait_until_ring_converged(ClusterB),
    rt:wait_until_ring_converged(ClusterC),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                    ClusterA ++ ClusterB ++ ClusterC),
    ok = rpc:call(hd(ClusterA), riak_core_console, security_enable, [[]]),
    ok = rpc:call(hd(ClusterB), riak_core_console, security_enable, [[]]),
    ok = rpc:call(hd(ClusterC), riak_core_console, security_enable, [[]]),
    {ClusterA, ClusterB, ClusterC}.

simple_testsync(User, CertDir, ClusterA, ClusterB) ->
    SSLCredentials =
        {filename:join([CertDir, User ++ "/cacerts.pem"]),
            filename:join([CertDir, User ++ "/cert.pem"]),
            filename:join([CertDir, User ++ "/key.pem"]),
            User},
    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    {pb, {IPA, PortA}} = lists:keyfind(pb, 1, rt:connection_info(NodeA)),
    {pb, {IPB, PortB}} = lists:keyfind(pb, 1, rt:connection_info(NodeB)),
    fullsync_check({NodeA, IPA, PortA, ?A_NVAL},
                            {NodeB, IPB, PortB, ?B_NVAL},
                            cluster_a,
                            SSLCredentials).

test_rtqrepl_between_clusters(ClusterA, ClusterB, ClusterC, CertDir) ->
    SSLCredentials =
        {filename:join([CertDir, "site4.basho.com/cacerts.pem"]),
            filename:join([CertDir, "site4.basho.com/cert.pem"]),
            filename:join([CertDir, "site4.basho.com/key.pem"]),
            "site4.basho.com"},
    NodeA = hd(ClusterA),
    NodeB = hd(ClusterB),
    NodeC = hd(ClusterC),
    {pb, {IPA, PortA}} = lists:keyfind(pb, 1, rt:connection_info(NodeA)),
    {pb, {IPB, PortB}} = lists:keyfind(pb, 1, rt:connection_info(NodeB)),
    {pb, {IPC, PortC}} = lists:keyfind(pb, 1, rt:connection_info(NodeC)),
    %% valid credentials should be valid
    SecOpts = 
        [{credentials, "site4.basho.com", ""},
            {cacertfile,
                filename:join([CertDir, "site4.basho.com/cacerts.pem"])},
            {certfile,
                filename:join([CertDir, "site4.basho.com/cert.pem"])},
            {keyfile,
                filename:join([CertDir, "site4.basho.com/key.pem"])}
            ],
    {ok, SecurityTestPB} =
        riakc_pb_socket:start(IPA, PortA, SecOpts),
    ?assertEqual(pong, riakc_pb_socket:ping(SecurityTestPB)),
    riakc_pb_socket:stop(SecurityTestPB),

    lager:info("Setup queues for test"),
    setup_srcreplqueues(ClusterA, [cluster_b, cluster_c], any),
    setup_srcreplqueues(ClusterB, [cluster_a, cluster_c], any),
    setup_srcreplqueues(ClusterC, [cluster_a, cluster_b], any),
    setup_snkreplworkers(ClusterB ++ ClusterC, ClusterA, cluster_a),
    setup_snkreplworkers(ClusterA ++ ClusterC, ClusterB, cluster_b),
    setup_snkreplworkers(ClusterA ++ ClusterB, ClusterC, cluster_c),

    lager:info("Test empty clusters don't show any differences"),
    
    lager:info("Cluster A ~s ~w Cluster B ~s ~w Cluster C ~s ~w",
                [IPA, PortA, IPB, PortB, IPC, PortC]),
    
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC},
                            SSLCredentials),

    lager:info("Test 1000 key difference and resolve"),
    % Write keys to cluster A, verify B and C do have them.
    write_to_cluster(NodeA, 1, 1000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 1, 1000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 1, 1000, ?COMMMON_VAL_INIT, 0),
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC},
                            SSLCredentials),
    
    lager:info("Test replicating tombstones"),
    delete_from_cluster(NodeA, 901, 1000),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeB, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeC, 901, 1000, ?COMMMON_VAL_INIT, 100),
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC},
                            SSLCredentials),

    lager:info("Test replicating modified objects"),
    write_to_cluster(NodeB, 1, 100, ?COMMMON_VAL_MOD),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1, 100, ?COMMMON_VAL_MOD, 0),
    read_from_cluster(NodeC, 1, 100, ?COMMMON_VAL_MOD, 0),
    read_from_cluster(NodeA, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 101, 900, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 901, 1000, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeC, 901, 1000, ?COMMMON_VAL_INIT, 100),
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC},
                            SSLCredentials),
    
    lager:info("Suspend a queue at source and confirm replication stops ..."),
    lager:info("... but continues from unsuspended queues"),
    ok = action_on_srcqueue(ClusterC, cluster_a, suspend_rtq),
    write_to_cluster(NodeC, 1001, 2000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 1001, 2000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 1001, 2000, ?COMMMON_VAL_INIT, 1000),
    ok = action_on_srcqueue(ClusterC, cluster_a, resume_rtq),
    lager:info("Resuming the queue changes nothing ..."),
    lager:info("... But new PUTs will now replicate"),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 1001, 2000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 1001, 2000, ?COMMMON_VAL_INIT, 1000),
    write_to_cluster(NodeC, 1101, 2000, ?COMMMON_VAL_MOD),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1001, 1100, ?COMMMON_VAL_INIT, 100),
    read_from_cluster(NodeA, 1101, 2000, ?COMMMON_VAL_MOD, 0),
        % errors down to 100
    lager:info("Full sync from another cluster will resolve"),
    {clock_compare, 100} =
        fullsync_check({NodeB, IPB, PortB, ?B_NVAL},
                        {NodeA, IPA, PortA, ?A_NVAL},
                        cluster_a,
                        SSLCredentials),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeA, 1001, 1100, ?COMMMON_VAL_INIT, 0, true),
    read_from_cluster(NodeA, 1101, 2000, ?COMMMON_VAL_MOD, 0),
    true = check_all_insync({NodeA, IPA, PortA},
                            {NodeB, IPB, PortB},
                            {NodeC, IPC, PortC},
                            SSLCredentials),

    lager:info("Suspend working on a queue from sink and confirm ..."),
    lager:info("... replication stops but continues from unsuspended sinks"),
    ok = action_on_snkqueue(ClusterA, cluster_a, suspend_snkqueue),
    timer:sleep(?REPL_SLEEP),
        % Sleep here as there may be established workers looping on an empty
        % queue that may otherwise still replicate the first item
    write_to_cluster(NodeC, 2001, 3000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 2001, 3000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 2001, 3000, ?COMMMON_VAL_INIT, 1000),
    ok = action_on_snkqueue(ClusterA, cluster_a, resume_snkqueue),
    lager:info("Resuming the queue prompts recovery ..."),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 2001, 3000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 2001, 3000, ?COMMMON_VAL_INIT, 0),

    lager:info("Stop a node in source - and repl OK"),
    NodeA0 = hd(tl(ClusterA)),
    rt:stop_and_wait(NodeA0),
    lager:info("Node stopped"),
    write_to_cluster(NodeA, 3001, 4000, new_obj),
    read_from_cluster(NodeA, 3001, 4000, ?COMMMON_VAL_INIT, 0),
    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            fullsync_check,
                            [{NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL}, 
                                no_repair,
                                SSLCredentials],
                            {root_compare, 0},
                            ?WAIT_LOOPS),

    read_from_cluster(NodeB, 3001, 4000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 3001, 4000, ?COMMMON_VAL_INIT, 0),

    lager:info("Node restarting"),
    rt:start_and_wait(NodeA0),
    rt:wait_for_service(NodeA0, riak_kv),

    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            fullsync_check,
                            [{NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL}, 
                                no_repair,
                                SSLCredentials],
                            {root_compare, 0},
                            ?WAIT_LOOPS),
    
    lager:info("Stop a node in sink - and repl OK"),
    rt:stop_and_wait(NodeA0),
    lager:info("Node stopped"),
    write_to_cluster(NodeB, 4001, 5000, new_obj),
    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            fullsync_check,
                            [{NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL}, 
                                no_repair,
                                SSLCredentials],
                            {root_compare, 0},
                            ?WAIT_LOOPS),

    read_from_cluster(NodeA, 4001, 5000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 4001, 5000, ?COMMMON_VAL_INIT, 0),
    lager:info("Node restarting"),
    rt:start_and_wait(NodeA0),
    rt:wait_for_service(NodeA0, riak_kv),


    lager:info("Kill a node in source - and repl OK"),
    rt:brutal_kill(NodeA0),
    lager:info("Node killed"),
    timer:sleep(2000), % Cluster may settle after kill
    write_to_cluster(NodeA, 5001, 8000, new_obj),
    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            fullsync_check,
                            [{NodeA, IPA, PortA, ?A_NVAL},
                                {NodeB, IPB, PortB, ?B_NVAL}, 
                                no_repair,
                                SSLCredentials],
                            {root_compare, 0},
                            ?WAIT_LOOPS),

    read_from_cluster(NodeB, 5001, 8000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeC, 5001, 8000, ?COMMMON_VAL_INIT, 0),

    lager:info("Confirm replication from cache-less cluster ..."),
    lager:info(".. with node still killed in cluster A"),
    write_to_cluster(NodeC, 8001, 10000, new_obj),
    {root_compare, 0} =
        wait_for_outcome(?MODULE,
                            fullsync_check,
                            [{NodeA, IPA, PortA, ?A_NVAL},
                                {NodeC, IPC, PortC, ?C_NVAL}, 
                                no_repair,
                                SSLCredentials],
                            {root_compare, 0},
                            ?WAIT_LOOPS),

    read_from_cluster(NodeB, 8001, 10000, ?COMMMON_VAL_INIT, 0),
    read_from_cluster(NodeA, 8001, 10000, ?COMMMON_VAL_INIT, 0),

    rt:start_and_wait(NodeA0),

    pass.


action_on_srcqueue([], _SnkClusterName, _Action) ->
    ok;
action_on_srcqueue([SrcNode|Rest], SnkClusterName, Action) ->
    ok = rpc:call(SrcNode, riak_kv_replrtq_src, Action, [SnkClusterName]),
    action_on_srcqueue(Rest, SnkClusterName, Action).

action_on_snkqueue([], _SnkClusterName, _Action) ->
    ok;
action_on_snkqueue([SnkNode|Rest], SnkClusterName, Action) ->
    ok = rpc:call(SnkNode, riak_kv_replrtq_snk, Action, [SnkClusterName]),
    action_on_snkqueue(Rest, SnkClusterName, Action).

check_all_insync({NodeA, IPA, PortA},
                    {NodeB, IPB, PortB},
                    {NodeC, IPC, PortC},
                    SSLCredentials) ->
    {root_compare, 0} =
        wait_for_outcome(
            ?MODULE,
            fullsync_check,
            [{NodeA, IPA, PortA, ?A_NVAL},
                {NodeB, IPB, PortB, ?B_NVAL},
                cluster_b,
                SSLCredentials],
            {root_compare, 0},
            ?WAIT_LOOPS),
    {root_compare, 0} =
        wait_for_outcome(
            ?MODULE,
            fullsync_check,
            [{NodeB, IPB, PortB, ?B_NVAL},
                {NodeC, IPC, PortC, ?C_NVAL},
                cluster_c,
                SSLCredentials],
            {root_compare, 0},
            ?WAIT_LOOPS),
    {root_compare, 0} =
        wait_for_outcome(
            ?MODULE,
            fullsync_check,
            [{NodeC, IPC, PortC, ?C_NVAL},
                {NodeA, IPA, PortA, ?A_NVAL},
                cluster_a,
                SSLCredentials],
            {root_compare, 0},
            ?WAIT_LOOPS),
    true.

setup_srcreplqueues([], _SinkClusters, _Filter) ->
    ok;
setup_srcreplqueues([SrcNode|Rest], SinkClusters, Filter) ->
    SetupQueueFun =
        fun(ClusterName) ->
            true = rpc:call(SrcNode,
                            riak_kv_replrtq_src,
                            register_rtq,
                            [ClusterName, Filter])
        end,
    lists:foreach(SetupQueueFun, SinkClusters),
    setup_srcreplqueues(Rest, SinkClusters, Filter).


setup_snkreplworkers(SrcCluster, SnkNodes, SnkName) ->
    PeerMap =
        fun(Node, Acc) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            {{Acc, 0, IP, Port, pb}, Acc + 1}
        end,
    {PeerList, _} = lists:mapfoldl(PeerMap, 1, SrcCluster),
    SetupSnkFun = 
        fun(Node) ->
            ok = rpc:call(Node,
                            riak_kv_replrtq_snk,
                            add_snkqueue,
                            [SnkName, PeerList, ?SNK_WORKERS])
        end,
    lists:foreach(SetupSnkFun, SnkNodes).


fullsync_check({SrcNode, _SrcIP, _SrcPort, SrcNVal},
                {_SinkNode, SinkIP, SinkPort, SinkNVal},
                SnkClusterName,
                SSLCredentials) ->
    ModRef = riak_kv_ttaaefs_manager,
    _ = rpc:call(SrcNode, ModRef, pause, []),
    ok = rpc:call(SrcNode, ModRef, set_sink, [pb, SinkIP, SinkPort]),
    ok = rpc:call(SrcNode, ModRef, set_queuename, [SnkClusterName]),
    ok = rpc:call(SrcNode, ModRef, enable_ssl, [true, SSLCredentials]),
    ok = rpc:call(SrcNode, ModRef, set_allsync, [SrcNVal, SinkNVal]),
    AAEResult = rpc:call(SrcNode, riak_client, ttaaefs_fullsync, [all_check, 60]),

    % lager:info("Sleeping to await queue drain."),
    % timer:sleep(2000),
    
    AAEResult.

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

delete_from_cluster(Node, Start, End) ->
    lager:info("Deleting ~p keys from node ~p.", [End - Start + 1, Node]),
    lager:warning("Note that only utf-8 keys are used"),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            try riak_client:delete(?TEST_BUCKET, Key, C) of
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
read_from_cluster(Node, Start, End, CommonValBin, Errors) ->
    read_from_cluster(Node, Start, End, CommonValBin, Errors, false).

read_from_cluster(Node, Start, End, CommonValBin, Errors, LogErrors) ->
    lager:info("Reading ~p keys from node ~p.", [End - Start + 1, Node]),
    {ok, C} = riak:client_connect(Node),
    F = 
        fun(N, Acc) ->
            Key = list_to_binary(io_lib:format("~8..0B~n", [N])),
            case  riak_client:get(?TEST_BUCKET, Key, C) of
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
                        [length(ErrorsFound)]);
        _ ->
            case LogErrors of
                true ->
                    LogFun = 
                        fun(Error) ->
                            lager:info("Read error ~w", [Error])
                        end,
                    lists:foreach(LogFun, ErrorsFound);
                false ->
                    ok
            end,
            case length(ErrorsFound) of
                Errors ->
                    ok;
                _ ->
                    lists:foreach(fun(E) -> lager:warning("Read error ~w", [E]) end, ErrorsFound)
            end,
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

change_user(Action, User, ClusterA, ClusterB, ClusterC) ->
    ok = rpc:call(hd(ClusterA),
                    riak_core_console,
                    Action,
                    [[User]]),
    ok = rpc:call(hd(ClusterB),
                    riak_core_console,
                    Action,
                    [[User]]),
    ok = rpc:call(hd(ClusterC),
                    riak_core_console,
                    Action,
                    [[User]]).

add_source(User, ClusterA, ClusterB, ClusterC) ->
    ok = rpc:call(hd(ClusterA),
                    riak_core_console,
                    add_source,
                    [[User, "127.0.0.1/32", "certificate"]]),
    ok = rpc:call(hd(ClusterB),
                    riak_core_console,
                    add_source,
                    [[User, "127.0.0.1/32", "certificate"]]),
    ok = rpc:call(hd(ClusterC),
                    riak_core_console,
                    add_source,
                    [[User, "127.0.0.1/32", "certificate"]]).

test_basic_repl_failure(NodeA, NodeB, NodeC) ->
    % Write keys to cluster A, verify B and C do NOT have them.
    write_to_cluster(NodeA, 1, 1000, new_obj),
    timer:sleep(?REPL_SLEEP),
    read_from_cluster(NodeB, 1, 1000, ?COMMMON_VAL_INIT, 1000),
    read_from_cluster(NodeC, 1, 1000, ?COMMMON_VAL_INIT, 1000),
    true.