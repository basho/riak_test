%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%%

-module(replication2_connections).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(HB_TIMEOUT,  2000).

confirm() ->
    simple_test(),
    disconnect_test(),
    error_cleanup_test(),
    pass.

simple_test() ->
    NumNodes = rt_config:get(num_nodes, 6),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
              %% turn off fullsync
              {fullsync_on_connect, false},
              {fullsync_interval, disabled},
              %% override defaults for RT heartbeat so that we
              %% can see faults sooner and have a quicker test.
              {rt_heartbeat_interval, ?HB_TIMEOUT},
              {rt_heartbeat_timeout, ?HB_TIMEOUT}
             ]}
    ],

    Nodes = rt:deploy_nodes(NumNodes, Conf),
    {ANodes, BNodes} = lists:split(3, Nodes),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    lager:info("Waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    lager:info("Waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    lager:info("Naming A"),
    repl_util:name_cluster(AFirst, "A"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Naming B"),
    repl_util:name_cluster(BFirst, "B"),
    rt:wait_until_ring_converged(BNodes),

    lager:info("Connecting A to B"),
    connect_clusters(AFirst, BFirst),

    lager:info("Enabling realtime replication from A to B."),
    repl_util:enable_realtime(AFirst, "B"),
    rt:wait_until_ring_converged(ANodes),
    repl_util:start_realtime(AFirst, "B"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Connecting B to A"),
    connect_clusters(BFirst, AFirst),

    lager:info("Enabling realtime replication from B to A."),
    repl_util:enable_realtime(BFirst, "A"),
    rt:wait_until_ring_converged(BNodes),
    repl_util:start_realtime(BFirst, "A"),
    rt:wait_until_ring_converged(BNodes),

    lager:info("Verifying connectivity between clusters."),
    [verify_connectivity(Node, "B") || Node <- ANodes],
    [verify_connectivity(Node, "A") || Node <- BNodes],

    rt:clean_cluster(Nodes),

    pass.

disconnect_test() ->
    NumNodes = rt_config:get(num_nodes, 6),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
              %% turn off fullsync
              {fullsync_on_connect, false},
              {fullsync_interval, disabled},
              %% override defaults for RT heartbeat so that we
              %% can see faults sooner and have a quicker test.
              {rt_heartbeat_interval, ?HB_TIMEOUT},
              {rt_heartbeat_timeout, ?HB_TIMEOUT}
             ]}
    ],

    Nodes = rt:deploy_nodes(NumNodes, Conf),
    {ANodes, BNodes} = lists:split(3, Nodes),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    lager:info("Waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    lager:info("Waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    lager:info("Naming A"),
    repl_util:name_cluster(AFirst, "A"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Naming B"),
    repl_util:name_cluster(BFirst, "B"),
    rt:wait_until_ring_converged(BNodes),

    lager:info("Connecting A to B"),
    connect_clusters(AFirst, BFirst),

    lager:info("Connecting B to A"),
    connect_clusters(BFirst, AFirst),

    lager:info("Verifying connectivity between clusters."),
    [verify_connectivity(Node, "B") || Node <- ANodes],
    [verify_connectivity(Node, "A") || Node <- BNodes],

    lager:info("Disconnect A to B"),
    repl_util:disconnect_cluster(AFirst, "B"),

    lager:info("Verifying disconnect from A to B."),
    [verify_disconnect(Node) || Node <- ANodes],

    lager:info("Disconnect B to A"),
    repl_util:disconnect_cluster(BFirst, "A"),

    lager:info("Verifying disconnect from B to A."),
    [verify_disconnect(Node) || Node <- BNodes],

    rt:clean_cluster(Nodes),

    pass.

error_cleanup_test() ->
    NumNodes = rt_config:get(num_nodes, 6),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
              %% turn off fullsync
              {fullsync_on_connect, false},
              {fullsync_interval, disabled},
              %% override defaults for RT heartbeat so that we
              %% can see faults sooner and have a quicker test.
              {rt_heartbeat_interval, ?HB_TIMEOUT},
              {rt_heartbeat_timeout, ?HB_TIMEOUT},
              %% Set quicker cancellation interval of 5 seconds
              {cm_cancellation_interval, 5 * 1000}
             ]}
    ],

    Nodes = rt:deploy_nodes(NumNodes, Conf),
    {ANodes, BNodes} = lists:split(3, Nodes),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    lager:info("Waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    lager:info("Waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    lager:info("Naming A"),
    repl_util:name_cluster(AFirst, "A"),
    rt:wait_until_ring_converged(ANodes),

    lager:info("Naming B"),
    repl_util:name_cluster(BFirst, "B"),
    rt:wait_until_ring_converged(BNodes),

    % Insert intercept to cause some errors on connect
    lager:info("Adding intercept to cause econnrefused errors"),
    Intercept = {riak_core_connection,[{{sync_connect, 2}, return_econnrefused}]},
    [ok = rt_intercept:add(Target, Intercept) || Target <- ANodes],

    lager:info("Connecting A to B"),
    connect_clusters(AFirst, BFirst),

    lager:info("Wait until errors in connection_manager status"),
    wait_until_connection_errors(repl_util:get_leader(AFirst), BFirst),

    lager:info("Disconnect A from B via IP/PORT"),
    Res = rpc:call(AFirst, riak_repl_console, disconnect,[["127.0.0.1","10046"]]),
    ?assertEqual(ok, Res),

    lager:info("Wait until connections clear"),
    wait_until_connections_clear(repl_util:get_leader(AFirst)),

    lager:info("Verify disconnect from A to B"),
    [verify_disconnect(Node) || Node <- ANodes],

    pass.

%% @doc Verify connectivity between sources and sink.
verify_connectivity(Node, Cluster) ->
    print_repl_ring(Node),
    wait_for_connections(Node, Cluster),
    print_repl_ring(Node),
    restart_process(Node, riak_core_connection_manager),
    wait_for_connections(Node, Cluster).

verify_disconnect(Node) ->
    print_repl_ring(Node),
    wait_for_disconnect(Node),
    print_repl_ring(Node),
    restart_process(Node, riak_core_connection_manager),
    wait_for_disconnect(Node).

print_repl_ring(Node) ->
    {ok, Ring} = rpc:call(Node,
                          riak_core_ring_manager,
                          get_my_ring,
                          []),
    Clusters = rpc:call(Node,
                        riak_repl_ring,
                        get_clusters,
                        [Ring]),
    lager:info("REPL ring shows clusters as: ~p", [Clusters]).

%% @doc Wait for connections to be established from this node to the
%%      named cluster.
wait_for_connections(Node, Cluster) ->
    rt:wait_until(Node, fun(_) ->
                lager:info("Attempting to verify connections on ~p.",
                           [Node]),
                try
                    {ok, Connections} = rpc:call(Node,
                                                 riak_core_cluster_mgr,
                                                 get_connections,
                                                 []),
                    lager:info("Waiting for sink connections on ~p: ~p.",
                               [Node, Connections]),
                    case Connections of
                        [{{cluster_by_name, Cluster}, _}] ->
                            true;
                        _ ->
                            false
                    end
                catch
                    _:Error ->
                        lager:info("Caught error: ~p.", [Error]),
                        false
                end
        end).

%% @doc Wait for disconnect from this node to the
%%      named cluster.
wait_for_disconnect(Node) ->
    rt:wait_until(Node, fun(_) ->
                lager:info("Attempting to verify disconnect on ~p.",
                           [Node]),
                try
                    {ok, Connections} = rpc:call(Node,
                                                 riak_core_cluster_mgr,
                                                 get_connections,
                                                 []),
                    lager:info("Waiting for sink disconnect on ~p: ~p.",
                               [Node, Connections]),
                    case Connections of
                        [] ->
                            true;
                        _ ->
                            false
                    end
                catch
                    _:Error ->
                        lager:info("Caught error: ~p.", [Error]),
                        false
                end
        end).

%% @doc Wait until errors in connection
wait_until_connection_errors(Node, BNode) ->
    {ok, {_IP, Port}} = rpc:call(BNode, application, get_env,
                                 [riak_core, cluster_mgr]),
    rt:wait_until(Node, fun(_) ->
                try
                    Failures = rpc:call(Node,
                                       riak_core_connection_mgr,
                                       get_connection_errors,
                                       [{"127.0.0.1",Port}]),
                    lager:info("Waiting for endpoint connection failures on ~p: ~p.",
                               [Node, Failures]),
                    case orddict:size(Failures) of
                        0 ->
                            false;
                        _ ->
                            true
                    end
                catch
                    _:Error ->
                        lager:info("Caught error: ~p.", [Error]),
                        false
                end
        end).

%% @doc Wait until canceled connections are cleared
wait_until_connections_clear(Node) ->
    rt:wait_until(Node, fun(_) ->
                try
                    Status = rpc:call(Node,
                                     riak_core_connection_mgr,
                                     get_request_states,
                                     []),
                    lager:info("Waiting for cancelled connections to clear on ~p: ~p.",
                               [Node, Status]),
                    case Status of
                        [] ->
                            true;
                        _ ->
                            false
                    end
                catch
                    _:Error ->
                        lager:info("Caught error: ~p.", [Error]),
                        false
                end
        end).


%% @doc Restart a given process by name.
restart_process(Node, Name) ->
    lager:info("Restarting ~p on ~p.", [Name, Node]),

    %% Find the process.
    Pid0 = rpc:call(Node, erlang, whereis, [Name]),
    lager:info("Found ~p on node ~p at ~p, killing.",
               [Name, Node, Pid0]),

    %% Kill it.
    true = rpc:call(Node, erlang, exit, [Pid0, brutal_kill]),

    %% Verify it restarts.
    rt:wait_until(Node, fun(_) ->
                lager:info("Waiting for ~p to restart...", [Name]),
                Pid = rpc:call(Node, erlang, whereis, [Name]),
                Pid =/= Pid0 andalso Pid =/= undefined
        end),

    lager:info("Process restarted.").

%% @doc Connect two clusters for replication using their respective
%%      leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port).
