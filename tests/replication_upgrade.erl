-module(replication_upgrade).
-compile(export_all).
-include("rt.hrl").

replication_upgrade() ->
    FromVersion = replication:get_os_env("REPL_FROM", "1.1.4"),
    ToVersion = replication:get_os_env("REPL_TO", "current"),
    lager:info("Doing rolling replication upgrade test from ~p to ~p",
        [FromVersion, ToVersion]),

    NumNodes = 6,

    Backend = list_to_atom(replication:get_os_env("RIAK_BACKEND",
            "riak_kv_bitcask_backend")),

    lager:info("Deploy ~p nodes using ~p backend", [NumNodes, Backend]),
    Conf = [
            {riak_kv,
             [
                {storage_backend, Backend}
             ]},
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
             ]}
    ],

    NodeConfig = [{FromVersion, Conf} || _ <- lists:seq(1, NumNodes)],

    Nodes = rt:deploy_nodes(NodeConfig),

    ClusterASize = list_to_integer(replication:get_os_env("CLUSTER_A_SIZE", "3")),

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    replication:make_cluster(ANodes),

    lager:info("Build cluster B"),
    replication:make_cluster(BNodes),

    %% initial replication run, homogeneous cluster
    replication:replication(ANodes, BNodes, false),
    lists:foreach(fun(Node) ->
                rtdev:upgrade(Node, ToVersion),
                rt:wait_until_pingable(Node),
                timer:sleep(1000),
                replication:replication(ANodes, BNodes, true)
        end, Nodes).
