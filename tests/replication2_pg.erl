-module(replication2_pg).
-export([confirm/0]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Test proxy_get in Default and Advanced mode of 1.3+ repl
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup_repl_clusters(Conf) ->
    setup_repl_clusters(Conf, false).

setup_repl_clusters(Conf, SSL) ->
    NumNodes = 6,
    lager:info("Deploy ~p nodes", [NumNodes]),


    PrivDir = rt:priv_dir(),

    SSLConfig1 = [
            {riak_core,
             [
                    {ssl_enabled, true},
                    {certfile, filename:join([PrivDir,
                                              "certs/selfsigned/site1-cert.pem"])},
                    {keyfile, filename:join([PrivDir,
                                             "certs/selfsigned/site1-key.pem"])},
                    {cacertdir, filename:join([PrivDir,
                                               "certs/selfsigned/ca"])}
                    ]}
            ],

    SSLConfig2 = [
            {riak_core,
             [
                    {ssl_enabled, true},
                    {certfile, filename:join([PrivDir,
                                              "certs/selfsigned/site2-cert.pem"])},
                    {keyfile, filename:join([PrivDir,
                                             "certs/selfsigned/site2-key.pem"])},
                    {cacertdir, filename:join([PrivDir,
                                               "certs/selfsigned/ca"])}
                    ]}
            ],

    SSLConfig3 = [
            {riak_core,
             [
                    {ssl_enabled, true},
                    {certfile, filename:join([PrivDir,
                                              "certs/selfsigned/site3-cert.pem"])},
                    {keyfile, filename:join([PrivDir,
                                             "certs/selfsigned/site3-key.pem"])},
                    {cacertdir, filename:join([PrivDir,
                                               "certs/selfsigned/ca"])}
                    ]}
            ],



    Nodes = deploy_nodes(NumNodes, Conf),

    lager:info("Nodes = ~p", [Nodes]),
    {[AFirst|_] = ANodes, Rest} = lists:split(2, Nodes),
    {[BFirst|_] = BNodes, [CFirst|_] = CNodes} = lists:split(2, Rest),

    %%AllNodes = ANodes ++ BNodes ++ CNodes,
    rt:log_to_nodes(Nodes, "Starting replication2_pg test"),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),
    lager:info("CNodes: ~p", [CNodes]),

    case SSL of
        true ->
            lager:info("Enabling SSL for this test"),
            [rt:update_app_config(N, merge_config(SSLConfig1, Conf)) ||
                N <- ANodes],
            [rt:update_app_config(N, merge_config(SSLConfig2, Conf)) ||
                N <- BNodes],
            [rt:update_app_config(N, merge_config(SSLConfig3, Conf)) ||
                N <- CNodes];
        _ ->
            lager:info("SSL not enabled for this test")
    end,

    rt:log_to_nodes(Nodes, "Building and connecting clusters"),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    lager:info("Build cluster C"),
    repl_util:make_cluster(CNodes),

    repl_util:name_cluster(AFirst, "A"),
    repl_util:name_cluster(BFirst, "B"),
    repl_util:name_cluster(CFirst, "C"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),
    rt:wait_until_ring_converged(CNodes),

    %% get the leader for the first cluster
    repl_util:wait_until_leader(AFirst),
    LeaderA = rpc:call(AFirst, riak_core_cluster_mgr, get_leader, []),

    {ok, {_IP, BPort}} = rpc:call(BFirst, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", BPort),

    {ok, {_IP, CPort}} = rpc:call(CFirst, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", CPort),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    rt:wait_until_ring_converged(ANodes),

    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "C")),
    rt:wait_until_ring_converged(ANodes),
    {LeaderA, ANodes, BNodes, CNodes, Nodes}.


make_test_object(Suffix) ->
    Bucket = <<"test_bucket">>,
    KeyText = "test_key" ++ Suffix,
    ValueText = "testdata_" ++ Suffix,
    Key    = erlang:list_to_binary(KeyText),
    Value =  erlang:list_to_binary(ValueText),
    {Bucket, Key, Value}.


test_basic_pg(Mode) ->
    test_basic_pg(Mode, false).

test_basic_pg(Mode, SSL) ->
    banner(io_lib:format("test_basic_pg with ~p mode", [Mode]), SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),
    rt:log_to_nodes(AllNodes, "Testing basic pg"),

    case Mode of
        mode_repl13 ->
            ModeRes = rpc:call(LeaderA, riak_repl_console, modes, [["mode_repl13"]]),
            lager:info("ModeRes = ~p", [ModeRes]);
        mixed ->
            lager:info("Using mode_repl12, mode_repl13"),
            ok
    end,
    rt:wait_until_ring_converged(ANodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    lager:info("Enabled pg: ~p", [PGEnableResult]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledFor -> lager:info("PG enabled for cluster ~p",[EnabledFor])
    end,

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    lager:info("Cluster ID for A = ~p", [CidA]),

    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),

    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),

    {_FirstA, FirstB, FirstC} = get_firsts(AllNodes),
    PidB = rt:pbc(FirstB),
    lager:info("Connected to cluster B"),
    {ok, PGResult} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResult)),

    rt:log_to_nodes(AllNodes, "Disabling pg on A"),
    PGDisableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["disable","B"]]),
    lager:info("Disable pg ~p", [PGDisableResult]),
    Status2 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status2) of
        [] -> ok
    end,

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    %% After the clusters are disconnected, see if the object was
    %% written locally after the PG
    {ok, PG2Value} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),

    ?assertEqual(ValueA, riakc_obj:get_value(PG2Value)),

    %% test an object that wasn't previously "proxy-gotten", it should fail
    FailedResult = riak_repl_pb_api:get(PidB,Bucket,KeyB,CidA),
    ?assertEqual({error, notfound}, FailedResult),

    PGEnableResult2 = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    lager:info("Enabled pg: ~p", [PGEnableResult2]),
    Status3 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status3) of
        undefined -> ?assert(false);
        EnabledFor2 -> lager:info("PG enabled for cluster ~p",[EnabledFor2])
    end,

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    {ok, PGResult2} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResult2)),

    %% Test with optional n_val and sloppy_quorum Options.
    %% KeyB is not on C yet. Try via proxy get with above options.

    PGEnableResult3 = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","C"]]),
    lager:info("Enabled pg: ~p", [PGEnableResult3]),
    Status4 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status4) of
        undefined -> ?assert(false);
        EnabledFor3 -> lager:info("PG enabled for cluster ~p",[EnabledFor3])
    end,

    PidC = rt:pbc(FirstC),

    Options = [{n_val, 1}, {sloppy_quorum, false}],
    lager:info("Test proxy get from C using options: ~p", [Options]),
    PGResult3 = riak_repl_pb_api:get(PidC,Bucket,KeyA,CidA,Options),
    % it's ok if the first request fails due to the options,
    % try it again without options to see if it passes
    RetriableGet = case PGResult3 of
        {ok, PGResult3Value} ->
            riakc_obj:get_value(PGResult3Value);
        {error, notfound} ->
            RetryOptions = [{n_val, 1}],
            case riak_repl_pb_api:get(PidC,Bucket,KeyA,CidA,RetryOptions) of
                {ok, PGResult4Value} -> riakc_obj:get_value(PGResult4Value);
                UnknownResult -> UnknownResult
            end;
        UnknownResult ->
            %% welp, we might have been expecting a notfound, but we got
            %% something else.
            UnknownResult
    end,
    ?assertEqual(ValueA, RetriableGet),
    pass.

%% test 1.2 replication (aka "Default" repl)
%% Mode is either mode_repl12 or mixed. 
%% "mixed" is the default in 1.3: mode_repl12, mode_repl13
test_12_pg(Mode) ->
    test_12_pg(Mode, false).

test_12_pg(Mode, SSL) ->
    banner(io_lib:format("test_12_pg with ~p mode", [Mode]), SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),

    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),

    rt:log_to_nodes(AllNodes, "Test 1.2 proxy_get"),
    {_FirstA, FirstB, _FirstC} = get_firsts(AllNodes),
    case Mode of
        mode_repl12 ->
            ModeRes = rpc:call(FirstB, riak_repl_console, modes, [["mode_repl12"]]),
            lager:info("ModeRes = ~p", [ModeRes]);
        mixed ->
            lager:info("Using mode_repl12, mode_repl13"),
            ok
    end,
    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    PidA = rt:pbc(LeaderA),
    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),

    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    lager:info("Cluster ID for A = ~p", [CidA]),

    LeaderB = rpc:call(FirstB, riak_repl2_leader, leader_node, []),
    rt:log_to_nodes([LeaderB], "Trying to use PG while it's disabled"),
    PidB = rt:pbc(LeaderB),
    ?assertEqual({error, notfound},
                  riak_repl_pb_api:get(PidB, Bucket, KeyA, CidA)),

    rt:log_to_nodes([LeaderA], "Adding a listener"),
    ListenerArgs = [[atom_to_list(LeaderA), "127.0.0.1", "5666"]],
    Res = rpc:call(LeaderA, riak_repl_console, add_listener, ListenerArgs),
    ?assertEqual(ok, Res),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    rt:log_to_nodes([FirstB], "Adding a site"),
    SiteArgs = ["127.0.0.1", "5666", "rtmixed"],
    Res = rpc:call(FirstB, riak_repl_console, add_site, [SiteArgs]),
    lager:info("Res = ~p", [Res]),

    rt:log_to_nodes(AllNodes, "Waiting until connected"),
    wait_until_12_connection(LeaderA),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],
    lager:info("Trying proxy_get"),

    LeaderB2 = rpc:call(FirstB, riak_repl2_leader, leader_node, []),
    PidB2 = rt:pbc(LeaderB2),
    {ok, PGResult} = riak_repl_pb_api:get(PidB2, Bucket, KeyB, CidA),
    lager:info("PGResult: ~p", [PGResult]),
    ?assertEqual(ValueB, riakc_obj:get_value(PGResult)),

    lager:info("Disable repl and wait for clusters to disconnect"),

    rt:log_to_nodes([LeaderA], "Delete listener"),
    DelListenerArgs = [[atom_to_list(LeaderA), "127.0.0.1", "5666"]],
    DelListenerRes = rpc:call(LeaderA, riak_repl_console, del_listener, DelListenerArgs),
    ?assertEqual(ok, DelListenerRes),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    rt:log_to_nodes([FirstB], "Delete site"),
    DelSiteArgs = ["127.0.0.1", "5666", "rtmixed"],
    DelSiteRes = rpc:call(FirstB, riak_repl_console, add_site, [DelSiteArgs]),
    lager:info("Res = ~p", [DelSiteRes]),

    rt:log_to_nodes(AllNodes, "Waiting until disconnected"),
    wait_until_12_no_connection(LeaderA),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    rt:log_to_nodes(AllNodes, "Trying proxy_get without a connection"),
    ?assertEqual({error, notfound},
                  riak_repl_pb_api:get(PidB, Bucket, KeyA, CidA)),
    pass.

%% test shutting down nodes in source + sink clusters
test_pg_proxy() ->
    test_pg_proxy(false).

test_pg_proxy(SSL) ->
    banner("test_pg_proxy", SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),
    rt:log_to_nodes(AllNodes, "Testing pg proxy"),
    rt:wait_until_ring_converged(ANodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    lager:info("Enabled pg: ~p", [PGEnableResult]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledFor -> lager:info("PG enabled for cluster ~p",[EnabledFor])
    end,

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    lager:info("Cluster ID for A = ~p", [CidA]),

    %% Write a new k/v for every PG test, otherwise you'll get a locally written response
    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),
    {Bucket, KeyC, ValueC} = make_test_object("c"),
    {Bucket, KeyD, ValueD} = make_test_object("d"),

    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),
    rt:pbc_write(PidA, Bucket, KeyC, ValueC),
    rt:pbc_write(PidA, Bucket, KeyD, ValueD),
    %% sanity check. You know, like the 10000 tests that autoconf runs 
    %% before it actually does any work.
    {FirstA, FirstB, _FirstC} = get_firsts(AllNodes),
    PidB = rt:pbc(FirstB),
    lager:info("Connected to cluster B"),
    {ok, PGResult} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResult)),

    lager:info("Stopping leader on requester cluster"),
    PGLeaderB = rpc:call(FirstB, riak_core_cluster_mgr, get_leader, []),
    rt:log_to_nodes(AllNodes, "Killing leader on requester cluster"),
    rt:stop(PGLeaderB),
    [RunningBNode | _ ] = BNodes -- [PGLeaderB],
    repl_util:wait_until_leader(RunningBNode),
    lager:info("Now trying proxy_get"),
    ?assertEqual(ok, wait_until_pg(RunningBNode, PidB, Bucket, KeyC, CidA)),
    lager:info("If you got here, proxy_get worked after the pg block requesting leader was killed"),

    lager:info("Stopping leader on provider cluster"),
    PGLeaderA = rpc:call(FirstA, riak_core_cluster_mgr, get_leader, []),
    rt:stop(PGLeaderA),
    [RunningANode | _ ] = ANodes -- [PGLeaderA],
    repl_util:wait_until_leader(RunningANode),
    ?assertEqual(ok, wait_until_pg(RunningBNode, PidB, Bucket, KeyD, CidA)),
    lager:info("If you got here, proxy_get worked after the pg block providing leader was killed"),
    lager:info("pg_proxy test complete. Time to obtain celebratory cheese sticks."),

    pass.

%% test mapping of cluster from a retired cluster to an active one, repl issue 306
test_cluster_mapping() ->
    test_cluster_mapping(false).

test_cluster_mapping(SSL) ->
    banner("test_cluster_mapping", SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),

    {_FirstA, FirstB, FirstC} = get_firsts(AllNodes),
    LeaderB = rpc:call(FirstB, riak_core_cluster_mgr, get_leader, []),
    LeaderC = rpc:call(FirstC, riak_core_cluster_mgr, get_leader, []),
    
    % Cluser C-> connection must be set up for the proxy gets to work
    % with the cluster ID mapping
    {ok, {_IP, CPort}} = rpc:call(FirstC, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderB, "127.0.0.1", CPort),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderB, "C")),

    % enable A to serve blocks to C
    PGEnableResultA = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","C"]]),
    % enable B to serve blocks to C
    PGEnableResultB = rpc:call(LeaderB, riak_repl_console, proxy_get, [["enable","C"]]),

    lager:info("Enabled pg to A:~p", [PGEnableResultA]),
    lager:info("Enabled pg to B:~p", [PGEnableResultB]),

    StatusA = rpc:call(LeaderA, riak_repl_console, status, [quiet]),
    case proplists:get_value(proxy_get_enabled, StatusA) of
        undefined -> ?assert(false);
        EnabledForA -> lager:info("PG enabled for cluster ~p",[EnabledForA])
    end,
    StatusB = rpc:call(LeaderB, riak_repl_console, status, [quiet]),
    case proplists:get_value(proxy_get_enabled, StatusB) of
        undefined -> ?assert(false);
        EnabledForB -> lager:info("PG enabled for cluster ~p",[EnabledForB])
    end,

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    lager:info("Cluster ID for A = ~p", [CidA]),

    PidB = rt:pbc(LeaderB),
    {ok,CidB}=riak_repl_pb_api:get_clusterid(PidB),
    lager:info("Cluster ID for B = ~p", [CidB]),

    PidC = rt:pbc(LeaderC),
    {ok,CidC}=riak_repl_pb_api:get_clusterid(PidC),
    lager:info("Cluster ID for C = ~p", [CidC]),

    %% Write a new k/v for every PG test, otherwise you'll get a locally written response
    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),
    {Bucket, KeyC, ValueC} = make_test_object("c"),
    {Bucket, KeyD, ValueD} = make_test_object("d"),

    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),
    rt:pbc_write(PidA, Bucket, KeyC, ValueC),
    rt:pbc_write(PidA, Bucket, KeyD, ValueD),
    

    {ok, PGResult} = riak_repl_pb_api:get(PidA,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResult)),

    % Configure cluster_mapping on C to map cluster_id A -> C
    lager:info("Configuring cluster C to map its cluster_id to B's cluster_id"),
    %rpc:call(LeaderC, riak_core_metadata, put, [{<<"replication">>, <<"cluster-mapping">>}, CidA, CidB]),
    rpc:call(LeaderC, riak_repl_console, add_block_provider_redirect, [[CidA, CidB]]),
    Res = rpc:call(LeaderC, riak_core_metadata, get, [{<<"replication">>, <<"cluster-mapping">>}, CidA]),
    lager:info("result: ~p", [Res]),

    % full sync from CS Block Provider A to CS Block Provider B
    repl_util:enable_fullsync(LeaderA, "B"),
    rt:wait_until_ring_converged(ANodes),   

    {Time,_} = timer:tc(repl_util,start_and_wait_until_fullsync_complete,[LeaderA]),
    lager:info("Fullsync completed in ~p seconds", [Time/1000/1000]),

    % shut down cluster A
    lager:info("Shutting down cluster A"),
    [ rt:stop(Node)  || Node <- ANodes ],
    [ rt:wait_until_unpingable(Node)  || Node <- ANodes ],

    rt:wait_until_ring_converged(BNodes),
    rt:wait_until_ring_converged(CNodes),

    % proxy-get from cluster C, using A's clusterID
    % Should redirect requester C from Cid A, to Cid B, and still
    % return the correct value for the Key
    {ok, PGResultC} = riak_repl_pb_api:get(PidC, Bucket, KeyC, CidA),
    lager:info("PGResultC: ~p", [PGResultC]),
    ?assertEqual(ValueC, riakc_obj:get_value(PGResultC)),

    % now delete the redirect and make sure it's gone
    rpc:call(LeaderC, riak_repl_console, delete_block_provider_redirect, [[CidA]]),
    case rpc:call(LeaderC, riak_core_metadata, get, [{<<"replication">>, <<"cluster-mapping">>}, CidA]) of
        undefined -> 
            lager:info("cluster-mapping no longer found in meta data, after delete, which is expected");
        Match ->
            lager:info("cluster mapping ~p still in meta data after delete; problem!", [Match]),
            ?assert(false)    
    end,
    pass.

%% connect source + sink clusters, pg bidirectionally
test_bidirectional_pg() ->
    test_bidirectional_pg(false).

test_bidirectional_pg(SSL) ->
    banner("test_bidirectional_pg", SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),
    rt:log_to_nodes(AllNodes, "Testing bidirectional proxy-get"),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    {FirstA, FirstB, _FirstC} = get_firsts(AllNodes),

    LeaderB = rpc:call(FirstB, riak_repl2_leader, leader_node, []),

    {ok, {_IP, APort}} = rpc:call(FirstA, application, get_env,
                                  [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderB, "127.0.0.1", APort),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),   
    PGEnableResult = rpc:call(LeaderB, riak_repl_console, proxy_get, [["enable","A"]]),   

    lager:info("Enabled bidirectional pg ~p", [PGEnableResult]),
    StatusA = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, StatusA) of
        undefined -> ?assert(false);
        EnabledForA -> lager:info("PG enabled for cluster ~p",[EnabledForA])
    end,

    StatusB = rpc:call(LeaderB, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, StatusB) of
        undefined -> ?assert(false);
        EnabledForB -> lager:info("PG enabled for cluster ~p",[EnabledForB])
    end,

    PidA = rt:pbc(LeaderA),
    PidB = rt:pbc(FirstB),

    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    {ok,CidB}=riak_repl_pb_api:get_clusterid(PidB),
    lager:info("Cluster ID for A = ~p", [CidA]),
    lager:info("Cluster ID for B = ~p", [CidB]),

    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),

    %% write some data to cluster A
    rt:pbc_write(PidA, Bucket, KeyA, ValueA),

    %% write some data to cluster B
    rt:pbc_write(PidB, Bucket, KeyB, ValueB),

    lager:info("Trying first get"),
    wait_until_pg(LeaderB, PidB, Bucket, KeyA, CidA),
    lager:info("First get worked"),

    lager:info("Trying second get"),
    wait_until_pg(LeaderA, PidA, Bucket, KeyB, CidB),
    lager:info("Second get worked"),
    pass.

%% Test multiple sinks against a single source
test_multiple_sink_pg() ->
    test_multiple_sink_pg(false).

test_multiple_sink_pg(SSL) ->
    banner("test_multiple_sink_pg", SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),
    rt:log_to_nodes(AllNodes, "Testing basic pg"),

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),
    rt:wait_until_ring_converged(CNodes),

    PGEnableResultB = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    PGEnableResultC = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","C"]]),

    lager:info("Enabled pg to B:~p", [PGEnableResultB]),
    lager:info("Enabled pg to C:~p", [PGEnableResultC]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledForC -> lager:info("PG enabled for cluster ~p",[EnabledForC])
    end,

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    lager:info("Cluster ID for A = ~p", [CidA]),

    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),

    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),

    {_FirstA, FirstB, FirstC} = get_firsts(AllNodes),

    PidB = rt:pbc(FirstB),
    PidC = rt:pbc(FirstC),

    {ok, PGResultB} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResultB)),

    {ok, PGResultC} = riak_repl_pb_api:get(PidC,Bucket,KeyB,CidA),
    ?assertEqual(ValueB, riakc_obj:get_value(PGResultC)),

    pass.

%% test 1.2 + 1.3 repl being used at the same time
test_mixed_pg() ->
    test_mixed_pg(false).

test_mixed_pg(SSL) ->
    banner("test_mixed_pg", SSL),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf, SSL),
    rt:log_to_nodes(AllNodes, "Testing basic pg"),

    rt:wait_until_ring_converged(ANodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    lager:info("Enabled pg ~p:", [PGEnableResult]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> ?assert(false);
        EnabledFor -> lager:info("PG enabled for cluster ~p",[EnabledFor])
    end,

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    lager:info("Cluster ID for A = ~p", [CidA]),

    {Bucket, KeyB, ValueB} = make_test_object("b"),
    {Bucket, KeyC, ValueC} = make_test_object("c"),

    rt:pbc_write(PidA, Bucket, KeyB, ValueB),
    rt:pbc_write(PidA, Bucket, KeyC, ValueC),

    {_FirstA, FirstB, FirstC} = get_firsts(AllNodes),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    rt:log_to_nodes([LeaderA], "Adding a listener"),
    ListenerArgs = [[atom_to_list(LeaderA), "127.0.0.1", "5666"]],
    Res = rpc:call(LeaderA, riak_repl_console, add_listener, ListenerArgs),
    ?assertEqual(ok, Res),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

    rt:log_to_nodes([FirstC], "Adding a site"),
    SiteArgs = ["127.0.0.1", "5666", "rtmixed"],
    Res = rpc:call(FirstC, riak_repl_console, add_site, [SiteArgs]),
    lager:info("Res = ~p", [Res]),

    rt:log_to_nodes(AllNodes, "Waiting until connected"),
    wait_until_12_connection(LeaderA),

    [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],
    lager:info("Trying proxy_get"),

    LeaderC = rpc:call(FirstC, riak_repl2_leader, leader_node, []),
    PidB = rt:pbc(FirstB),
    PidC = rt:pbc(LeaderC),

    {ok, PGResultB} = riak_repl_pb_api:get(PidB, Bucket, KeyB, CidA),
    lager:info("PGResultB: ~p", [PGResultB]),
    ?assertEqual(ValueB, riakc_obj:get_value(PGResultB)),

    {ok, PGResultC} = riak_repl_pb_api:get(PidC, Bucket, KeyC, CidA),
    lager:info("PGResultC: ~p", [PGResultC]),
    ?assertEqual(ValueC, riakc_obj:get_value(PGResultC)),

    pass.


wait_until_12_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                case rpc:call(Node, riak_repl_console, status, [quiet]) of
                    {badrpc, _} ->
                        false;
                    Status ->
                        case proplists:get_value(server_stats, Status) of
                            [] ->
                                false;
                            [{_, _, too_busy}] ->
                                false;
                            [_C] ->
                                true;
                            Conns ->
                                lager:warning("multiple connections detected: ~p",
                                    [Conns]),
                                true
                        end
                end
        end). %% 40 seconds is enough for repl

wait_until_12_no_connection(Node) ->
    rt:wait_until(Node,
        fun(_) ->
                case rpc:call(Node, riak_repl_console, status, [quiet]) of
                    {badrpc, _} ->
                        false;
                    Status ->
                        case proplists:get_value(server_stats, Status) of
                            undefined ->
                                true;
                            [] ->
                                true;
                            [{_, _, too_busy}] ->
                                false;
                            [_C] ->
                                false;
                            Conns ->
                                lager:warning("multiple connections detected: ~p",
                                    [Conns]),
                                false
                        end
                end
        end). %% 40 seconds is enough for repl



%% these funs allow you to call:
%% riak_test -t replication2_pg:test_basic_pg_mode_repl13 etc
test_basic_pg_mode_repl13() ->
    test_basic_pg(mode_repl13).

test_basic_pg_mode_mixed() ->
    test_basic_pg(mixed).

test_12_pg_mode_repl12() ->
    test_12_pg(mode_repl12).

test_12_pg_mode_repl_mixed() ->
         test_12_pg(mixed).


test_basic_pg_mode_repl13_ssl() ->
    test_basic_pg(mode_repl13, true).

test_basic_pg_mode_mixed_ssl() ->
    test_basic_pg(mixed, true).

test_12_pg_mode_repl12_ssl() ->
    test_12_pg(mode_repl12, true).

test_12_pg_mode_repl_mixed_ssl() ->
    test_12_pg(mixed, true).

test_mixed_pg_ssl() ->
    test_mixed_pg(true).

test_multiple_sink_pg_ssl() ->
    test_multiple_sink_pg(true).

test_bidirectional_pg_ssl() ->
    test_bidirectional_pg(true).

test_pg_proxy_ssl() ->
    test_pg_proxy(true).

confirm() ->
    AllTests =
        [
            test_basic_pg_mode_repl13,
            test_basic_pg_mode_mixed,
            test_12_pg_mode_repl12,
            test_12_pg_mode_repl_mixed,
            test_mixed_pg,
            test_multiple_sink_pg,
            test_bidirectional_pg,
            test_cluster_mapping,
            test_pg_proxy,
            test_basic_pg_mode_repl13_ssl,
            test_basic_pg_mode_mixed_ssl,
            test_12_pg_mode_repl12_ssl,
            test_12_pg_mode_repl_mixed_ssl,
            test_mixed_pg_ssl,
            test_multiple_sink_pg_ssl,
            test_bidirectional_pg_ssl,
            test_pg_proxy_ssl

        ],
    lager:error("run riak_test with -t Mod:test1 -t Mod:test2"),
    lager:error("The runnable tests in this module are: ~p", [AllTests]),
    ?assert(false).


banner(T) ->
    banner(T, false).

banner(T, SSL) ->
    lager:info("----------------------------------------------"),
    lager:info("----------------------------------------------"),
    lager:info("~s, SSL ~s",[T, SSL]),
    lager:info("----------------------------------------------"),
    lager:info("----------------------------------------------").


get_firsts(Nodes) ->
    {[AFirst|_] = _ANodes, Rest} = lists:split(2, Nodes),
    {[BFirst|_] = _BNodes, [CFirst|_] = _CNodes} = lists:split(2, Rest),
    {AFirst, BFirst, CFirst}.


wait_until_pg(Node, Pid, Bucket, Key, Cid) ->
    rt:wait_until(Node,
        fun(_) ->
                case riak_repl_pb_api:get(Pid,Bucket,Key,Cid) of
                    {error, notfound} ->
                        false;
                    {ok, _Value} -> true;
                    _ -> false
                end
        end).

merge_config(Mixin, Base) ->
    lists:ukeymerge(1, lists:keysort(1, Mixin), lists:keysort(1, Base)).

