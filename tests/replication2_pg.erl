-module(replication2_pg).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [deploy_nodes/2,
             join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Test BNW PG
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


%% test_basic_pg() ->
%%     Conf = [
%%             {riak_repl,
%%              [
%%               {proxy_get, enabled}
%%              ]}
%%            ],
%%     {LeaderA, ANodes, BNodes, _CNodes, AllNodes} =
%%         setup_repl_clusters(Conf),
%%     rt:log_to_nodes(AllNodes, "Testing basic pg"),

%%     rt:wait_until_ring_converged(ANodes),

%%     PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
%%     lager:info("Enabled pg ~p", [PGEnableResult]),
%%     Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

%%     case proplists:get_value(proxy_get_enabled, Status) of
%%         undefined -> fail;
%%         EnabledFor -> lager:info("PG enabled for cluster ~p",[EnabledFor])
%%     end,

%%     PidA = rt:pbc(LeaderA),
%%     {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
%%     lager:info("Cluster ID for A = ~p", [CidA]),

%%     Bucket = <<"test_bucket">>,
%%     KeyA    = <<"test_key_foo">>,
%%     ValueA = <<"0b:", "testdata">>,

%%     Bucket = <<"test_bucket">>,
%%     KeyB    = <<"test_key_bar">>,
%%     ValueB = <<"0b:", "testdata">>,

%%     rt:pbc_write(PidA, Bucket, KeyA, ValueA),
%%     rt:pbc_write(PidA, Bucket, KeyB, ValueB),

%%     {_FirstA, FirstB, _FirstC} = get_firsts(AllNodes),
%%     PidB = rt:pbc(FirstB),
%%     lager:info("Connected to cluster B"),
%%     {ok, PGResult} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
%%     ?assertEqual(ValueA, riakc_obj:get_value(PGResult)),

%%     rt:log_to_nodes(AllNodes, "Disabling pg on A"),
%%     PGDisableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["disable","B"]]),
%%     lager:info("Disable pg ~p", [PGDisableResult]),
%%     Status2 = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

%%     case proplists:get_value(proxy_get_enabled, Status2) of
%%         [] -> ok
%%     end,

%%     rt:wait_until_ring_converged(ANodes),
%%     rt:wait_until_ring_converged(BNodes),

%%     %% After the clusters are disconnected, see if the object was
%%     %% written locally after the PG
%%     {ok, PG2Value} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),

%%     ?assertEqual(ValueA, riakc_obj:get_value(PG2Value)),

%%     %% test an object that wasn't previously "proxy-gotten", it should fail
%%     FailedResult = riak_repl_pb_api:get(PidB,Bucket,KeyB,CidA),
%%     ?assertEqual({error, notfound}, FailedResult),

%%     rt:clean_cluster(AllNodes),
%%     pass.

%% test_12_pg() ->
%%     Conf = [
%%             {riak_repl,
%%              [
%%               {proxy_get, enabled},
%%               {fullsync_on_connect, false}
%%              ]}
%%            ],
%%     {LeaderA, ANodes, BNodes, CNodes, AllNodes} =
%%         setup_repl_clusters(Conf),

%%     Bucket = <<"test_bucket">>,
%%     KeyA    = <<"test_key_foo">>,
%%     ValueA = <<"0b:", "testdata">>,

%%     rt:log_to_nodes(AllNodes, "Test 1.2 proxy_get"),
%%     {_FirstA, FirstB, _FirstC} = get_firsts(AllNodes),

%%     ModeRes = rpc:call(FirstB, riak_repl_console, modes, [["mode_repl12"]]),
%%     lager:info("ModeRes = ~p", [ModeRes]),

%%     [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

%%     PidA = rt:pbc(LeaderA),
%%     rt:pbc_write(PidA, Bucket, KeyA, ValueA),
%%     {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
%%     lager:info("Cluster ID for A = ~p", [CidA]),

%%     LeaderB = rpc:call(FirstB, riak_repl2_leader, leader_node, []),
%%     rt:log_to_nodes([LeaderB], "Trying to use PG while it's disabled"),
%%     PidB = rt:pbc(LeaderB),
%%     {error, notfound} = riak_repl_pb_api:get(PidB, Bucket, KeyA, CidA),

%%     rt:log_to_nodes([LeaderA], "Adding a listener"),
%%     ListenerArgs = [[atom_to_list(LeaderA), "127.0.0.1", "5666"]],
%%     Res = rpc:call(LeaderA, riak_repl_console, add_listener, ListenerArgs),
%%     ?assertEqual(ok, Res),

%%     [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],

%%     rt:log_to_nodes([FirstB], "Adding a site"),
%%     SiteArgs = ["127.0.0.1", "5666", "rtmixed"],
%%     Res = rpc:call(FirstB, riak_repl_console, add_site, [SiteArgs]),
%%     lager:info("Res = ~p", [Res]),

%%     rt:log_to_nodes(AllNodes, "Waiting until connected"),
%%     wait_until_12_connection(LeaderA),

%%     [rt:wait_until_ring_converged(Ns) || Ns <- [ANodes, BNodes, CNodes]],
%%     lager:info("Trying proxy_get"),

%%     LeaderB2 = rpc:call(FirstB, riak_repl2_leader, leader_node, []),
%%     PidB2 = rt:pbc(LeaderB2),
%%     {ok, PGResult} = riak_repl_pb_api:get(PidB2, Bucket, KeyA, CidA),
%%     lager:info("PGResult: ~p", [PGResult]),
%%     ?assertEqual(ValueA, riakc_obj:get_value(PGResult)),

%%     rt:clean_cluster(AllNodes),
%%     pass.


test_pg_proxy() ->
    %% To test:
    %% Kill provider node
    %% Kill requestor node
    Conf = [
            {riak_repl,
             [
              {proxy_get, enabled}
             ]}
           ],
    {LeaderA, ANodes, BNodes, _CNodes, AllNodes} =
        setup_repl_clusters(Conf),
    rt:log_to_nodes(AllNodes, "Testing basic pg"),

    rt:wait_until_ring_converged(ANodes),

    PGEnableResult = rpc:call(LeaderA, riak_repl_console, proxy_get, [["enable","B"]]),
    lager:info("Enabled pg ~p", [PGEnableResult]),
    Status = rpc:call(LeaderA, riak_repl_console, status, [quiet]),

    case proplists:get_value(proxy_get_enabled, Status) of
        undefined -> fail;
        EnabledFor -> lager:info("PG enabled for cluster ~p",[EnabledFor])
    end,

    PidA = rt:pbc(LeaderA),
    {ok,CidA}=riak_repl_pb_api:get_clusterid(PidA),
    lager:info("Cluster ID for A = ~p", [CidA]),

    Bucket = <<"test_bucket">>,
    KeyA    = <<"test_key_foo">>,
    ValueA = <<"0b:", "testdata">>,

    Bucket = <<"test_bucket">>,
    KeyB    = <<"test_key_bar">>,
    ValueB = <<"0b:", "testdata">>,

    rt:pbc_write(PidA, Bucket, KeyA, ValueA),
    rt:pbc_write(PidA, Bucket, KeyB, ValueB),

    {FirstA, FirstB, _FirstC} = get_firsts(AllNodes),
    PidB = rt:pbc(FirstB),
    lager:info("Connected to cluster B"),
    {ok, PGResult} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResult)),

    PGLeaderA = rpc:call(FirstA, riak_core_cluster_mgr, get_leader, []),
    rt:brutal_kill(PGLeaderA),
    rt:log_to_nodes(AllNodes, "Killing leader on provider cluster"),
    rt:wait_until_ring_converged(ANodes),
    rt:wait_until_ring_converged(BNodes),

    {ok, PGResult2} = riak_repl_pb_api:get(PidB,Bucket,KeyA,CidA),
    ?assertEqual(ValueA, riakc_obj:get_value(PGResult2)),

    rt:clean_cluster(AllNodes),
    pass.


%% test_bidirectional_pg() ->

%% test_mixed_pg() ->

%% wait_until_12_connection(Node) ->
%%     rt:wait_until(Node,
%%         fun(_) ->
%%                 case rpc:call(Node, riak_repl_console, status, [quiet]) of
%%                     {badrpc, _} ->
%%                         false;
%%                     Status ->
%%                         case proplists:get_value(server_stats, Status) of
%%                             [] ->
%%                                 false;
%%                             [{_, _, too_busy}] ->
%%                                 false;
%%                             [_C] ->
%%                                 true;
%%                             Conns ->
%%                                 lager:warning("multiple connections detected: ~p",
%%                                     [Conns]),
%%                                 true
%%                         end
%%                 end
%%         end). %% 40 seconds is enough for repl

confirm() ->
    %AllTests = [test_basic_pg(), test_12_pg()],
    AllTests = [test_pg_proxy()],
    case lists:all(fun (Result) -> Result == pass end, AllTests) of
        true ->  pass;
        false -> sadtrombone
    end.

get_firsts(Nodes) ->
    {[AFirst|_] = _ANodes, Rest} = lists:split(2, Nodes),
    {[BFirst|_] = _BNodes, [CFirst|_] = _CNodes} = lists:split(2, Rest),
    {AFirst, BFirst, CFirst}.
