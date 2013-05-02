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
    NumNodes = 6,
    lager:info("Deploy ~p nodes", [NumNodes]),

    Nodes = deploy_nodes(NumNodes, Conf),

    lager:info("Nodes = ~p", [Nodes]),
    {[AFirst|_] = ANodes, Rest} = lists:split(2, Nodes),
    {[BFirst|_] = BNodes, [CFirst|_] = CNodes} = lists:split(2, Rest),

    %%AllNodes = ANodes ++ BNodes ++ CNodes,
    rt:log_to_nodes(Nodes, "Starting replication2_pg test"),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),
    lager:info("CNodes: ~p", [CNodes]),

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
    banner(io_lib:format("test_basic_pg with ~p mode", [Mode])),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf),
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
        undefined -> fail;
        EnabledFor -> lager:info("PG enabled for cluster ~p",[EnabledFor])
    end,

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    lager:info("Cluster ID for A = ~p", [CidA]),

    {Bucket, KeyA, ValueA} = make_test_object("a"),
    {Bucket, KeyB, ValueB} = make_test_object("b"),

    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),

    {_FirstA, FirstB, _FirstC} = get_firsts(AllNodes),
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

    rt:clean_cluster(AllNodes),
    pass.

%% test 1.2 replication (aka "Default" repl)
%% Mode is either mode_repl12 or mixed. 
%% "mixed" is the default in 1.3: mode_repl12, mode_repl13
test_12_pg(Mode) ->
    banner(io_lib:format("test_12_pg with ~p mode", [Mode])),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf),

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
    {error, notfound} = riak_repl_pb_api:get(PidB, Bucket, KeyA, CidA),

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

    rt:clean_cluster(AllNodes),
    pass.

%% test shutting down nodes in source + sink clusters
test_pg_proxy() ->
    banner("test_pg_proxy"),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf),
    rt:log_to_nodes(AllNodes, "Testing pg proxy"),
    rt:wait_until_ring_converged(ANodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    lager:info("Enabled pg: ~p", [PGEnableResult]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> fail;
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
    wait_until_pg(RunningBNode, PidB, Bucket, KeyC, CidA),
    lager:info("If you got here, proxy_get worked after the pg block requesting leader was killed"),

    lager:info("Stopping leader on provider cluster"),
    PGLeaderA = rpc:call(FirstA, riak_core_cluster_mgr, get_leader, []),
    rt:stop(PGLeaderA),
    rt:log_to_nodes(AllNodes, "Killing leader on provider cluster"),
    [RunningANode | _ ] = ANodes -- [PGLeaderA],
    repl_util:wait_until_leader(RunningANode),
    wait_until_pg(RunningBNode, PidB, Bucket, KeyD, CidA),
    lager:info("If you got here, proxy_get worked after the pg block providing leader was killed"),
    lager:info("pg_proxy test complete. Time to obtain celebratory cheese sticks."),

    %% clean up so riak_test doesn't panic
    rt:start(PGLeaderA),
    rt:start(PGLeaderB),
    rt:clean_cluster(AllNodes),
    pass.

%% connect source + sink clusters, pg bidirectionally
test_bidirectional_pg() ->
    banner("test_bidirectional_pg"),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf),
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
        undefined -> fail;
        EnabledForA -> lager:info("PG enabled for cluster ~p",[EnabledForA])
    end,

    StatusB = rpc:call(LeaderB, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, StatusB) of
        undefined -> fail;
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

    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    lager:info("Trying first get"),
    wait_until_pg(LeaderB, PidB, Bucket, KeyA, CidA),
    lager:info("First get worked"),

    lager:info("Trying second get"),
    wait_until_pg(LeaderA, PidA, Bucket, KeyB, CidB),
    lager:info("Second get worked"),

    rt:clean_cluster(AllNodes),
    pass.

%% Test multiple sinks against a single source
test_multiple_sink_pg() ->
    banner("test_multiple_sink_pg"),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf),
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
        undefined -> fail;
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

    rt:clean_cluster(AllNodes),
    pass.

%% test 1.2 + 1.3 repl being used at the same time
test_mixed_pg() ->
    banner("test_mixed_pg"),
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled},
              {fullsync_on_connect, false}
             ]}
           ],
    {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
        setup_repl_clusters(Conf),
    rt:log_to_nodes(AllNodes, "Testing basic pg"),

    rt:wait_until_ring_converged(ANodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    lager:info("Enabled pg ~p:", [PGEnableResult]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> fail;
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

    rt:clean_cluster(AllNodes),
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

confirm() ->
    AllTests = 
        [
         test_basic_pg(mode_repl13),
         test_basic_pg(mixed),
         test_12_pg(mode_repl12),
         test_12_pg(mixed),
         test_mixed_pg(),
         test_multiple_sink_pg(),
         test_bidirectional_pg(),
         test_pg_proxy() 
        ],
    case lists:all(fun (Result) -> Result == pass end, AllTests) of
        true ->  pass;
        false -> sadtrombone
    end.

banner(T) ->
    lager:info("----------------------------------------------"),
    lager:info("----------------------------------------------"),
    lager:info("~s",[T]),
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
