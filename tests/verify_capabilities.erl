-module(verify_capabilities).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    lager:info("Deploying mixed set of nodes"),
    Nodes = rt:deploy_nodes([current, "0.14.2", "1.1.4", "1.0.3"]),
    [Node1, Node2, Node3, Node4] = Nodes,

    lager:info("Verify vnode_routing == proxy"),
    ?assertEqual(ok, rt:wait_until_capability(Node1, {riak_core, vnode_routing}, proxy)),

    lager:info("Crash riak_core_capability server"),
    crash_capability_server(Node1),
    timer:sleep(1000),

    lager:info("Verify vnode_routing == proxy after crash"),
    ?assertEqual(proxy, rt:capability(Node1, {riak_core, vnode_routing})),

    lager:info("Building current + 0.14.2 cluster"),
    rt:join(Node2, Node1),
    ?assertEqual(ok, rt:wait_until_all_members([Node1], [Node1, Node2])),
    ?assertEqual(ok, rt:wait_until_legacy_ringready(Node1)),

    lager:info("Verifying vnode_routing == legacy"),
    ?assertEqual(ok, rt:wait_until_capability(Node1, {riak_core, vnode_routing}, legacy)),

    lager:info("Crash riak_core_capability server"),
    crash_capability_server(Node1),
    timer:sleep(1000),

    lager:info("Verify vnode_routing == legacy after crash"),
    ?assertEqual(legacy, rt:capability(Node1, {riak_core, vnode_routing})),

    lager:info("Adding 1.1.4 node to cluster"),
    rt:join(Node3, Node2),
    ?assertEqual(ok, rt:wait_until_all_members([Node1], [Node1, Node2, Node3])),
    ?assertEqual(ok, rt:wait_until_legacy_ringready(Node1)),

    lager:info("Verifying vnode_routing == legacy"),
    ?assertEqual(legacy, rt:capability(Node1, {riak_core, vnode_routing})),

    lager:info("Upgrade 0.14.2 node"),
    rt:upgrade(Node2, current),

    lager:info("Verifying vnode_routing == proxy"),
    ?assertEqual(ok, rt:wait_until_capability(Node1, {riak_core, vnode_routing}, proxy)),

    lager:info("Adding 1.0.3 node to cluster"),
    rt:join(Node4, Node1),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1, Node2, Node3, Node4])),

    lager:info("Verifying vnode_routing == legacy"),
    ?assertEqual(legacy, rt:capability(Node1, {riak_core, vnode_routing})),

    lager:info("Upgrading 1.0.3 node"),
    rt:upgrade(Node4, current),

    lager:info("Verifying vnode_routing changes to proxy"),
    ?assertEqual(ok, rt:wait_until_capability(Node1, {riak_core, vnode_routing}, proxy)),

    lager:info("Upgrade 1.1.4 node"),
    rt:upgrade(Node3, current),

    %% All nodes are now current version. Test override behavior.
    Override = fun(undefined, Prefer) ->
                       [{riak_core, [{override_capability,
                                      [{vnode_routing,
                                        [{prefer, Prefer}]
                                       }]}]
                        }];
                  (Use, Prefer) ->
                       [{riak_core, [{override_capability,
                                      [{vnode_routing,
                                        [{use, Use},
                                         {prefer, Prefer}]
                                       }]}]
                        }]
               end,

    lager:info("Override: (use: legacy), (prefer: proxy)"),
    [rt:update_app_config(Node, Override(legacy, proxy)) || Node <- Nodes],

    lager:info("Verify vnode_routing == legacy"),
    ?assertEqual(legacy, rt:capability(Node1, {riak_core, vnode_routing})),

    lager:info("Override: (use: proxy), (prefer: legacy)"),
    [rt:update_app_config(Node, Override(proxy, legacy)) || Node <- Nodes],

    lager:info("Verify vnode_routing == proxy"),
    ?assertEqual(proxy, rt:capability(Node1, {riak_core, vnode_routing})),

    lager:info("Override: (prefer: legacy)"),
    [rt:update_app_config(Node, Override(undefined, legacy)) || Node <- Nodes],

    lager:info("Verify vnode_routing == legacy"),
    ?assertEqual(legacy, rt:capability(Node1, {riak_core, vnode_routing})),

    [rt:stop(Node) || Node <- Nodes],
    pass.

crash_capability_server(Node) ->
    Pid = rpc:call(Node, erlang, whereis, [riak_core_capability]),
    rpc:call(Node, erlang, exit, [Pid, kill]).

