%% Test cluster version migration with BNW replication as "new" version
-module(replication2_upgrade).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    FromVersion = proplists:get_value(upgrade_version, TestMetaData, previous),

    lager:info("Doing rolling replication upgrade test from ~p to ~p",
        [FromVersion, "current"]),

    NumNodes = rt_config:get(num_nodes, 6),

    UpgradeOrder = rt_config:get(repl_upgrade_order, "forwards"),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_kv,
                [
                    {anti_entropy, {off, []}}
                ]
            },
            {riak_repl,
             [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled},
                {diff_batch_size, 10}
             ]}
    ],

    NodeConfig = [{FromVersion, Conf} || _ <- lists:seq(1, NumNodes)],

    Nodes = rt_cluster:deploy_nodes(NodeConfig),

    NodeUpgrades = case UpgradeOrder of
        "forwards" ->
            Nodes;
        "backwards" ->
            lists:reverse(Nodes);
        "alternate" ->
            %% eg 1, 4, 2, 5, 3, 6
            lists:flatten(lists:foldl(fun(E, [A,B,C]) -> [B, C, A ++ [E]] end,
                    [[],[],[]], Nodes));
        "random" ->
            %% halfass randomization
            lists:sort(fun(_, _) -> random:uniform(100) < 50 end, Nodes);
        Other ->
            lager:error("Invalid upgrade ordering ~p", [Other]),
            erlang:exit()
    end,

    ClusterASize = rt_config:get(cluster_a_size, 3),
    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),
    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    lager:info("Replication First pass...homogenous cluster"),
    rt:log_to_nodes(Nodes, "Replication First pass...homogenous cluster"),

    %% initial "previous" replication run, homogeneous cluster
    replication2:replication(ANodes, BNodes, false),

    lager:info("Upgrading nodes in order: ~p", [NodeUpgrades]),
    rt:log_to_nodes(Nodes, "Upgrading nodes in order: ~p", [NodeUpgrades]),
    %% upgrade the nodes, one at a time
    ok = lists:foreach(fun(Node) ->
                               lager:info("Upgrade node: ~p", [Node]),
                               rt:log_to_nodes(Nodes, "Upgrade node: ~p", [Node]),
                               rt:upgrade(Node, current),
                               %% The upgrade did a wait for pingable
                               rt:wait_for_service(Node, [riak_kv, riak_pipe, riak_repl]),
                               [rt:wait_until_ring_converged(N) || N <- [ANodes, BNodes]],

                               %% Prior to 1.4.8 riak_repl registered
                               %% as a service before completing all
                               %% initialization including establishing
                               %% realtime connections.
                               %%
                               %% @TODO Ideally the test would only wait
                               %% for the connection in the case of the
                               %% node version being < 1.4.8, but currently
                               %% the rt API does not provide a
                               %% harness-agnostic method do get the node
                               %% version. For now the test waits for all
                               %% source cluster nodes to establish a
                               %% connection before proceeding.
                               case lists:member(Node, ANodes) of
                                   true ->
                                       repl_util:wait_for_connection(Node, "B");
                                   false ->
                                       ok
                               end,
                               lager:info("Replication with upgraded node: ~p", [Node]),
                               rt:log_to_nodes(Nodes, "Replication with upgraded node: ~p", [Node]),
                               replication2:replication(ANodes, BNodes, true)
                       end, NodeUpgrades),
    pass.
