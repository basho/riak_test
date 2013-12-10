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

    lager:info("Naming B"),
    repl_util:name_cluster(BFirst, "B"),

    lager:info("Connecting A to B"),
    connect_clusters(AFirst, BFirst),

    lager:info("Enabling realtime replication from A to B."),
    repl_util:enable_realtime(AFirst, "B"),
    repl_util:start_realtime(AFirst, "B"),

    lager:info("Connecting B to A"),
    connect_clusters(BFirst, AFirst),

    lager:info("Enabling realtime replication from B to A."),
    repl_util:enable_realtime(BFirst, "A"),
    repl_util:start_realtime(BFirst, "A"),

    lager:info("Verifying connectivity between clusters."),
    [verify_connectivity(Node, "B") || Node <- ANodes],
    [verify_connectivity(Node, "A") || Node <- BNodes],

    pass.

%% @doc Verify connectivity between sources and sink.
verify_connectivity(Node, Cluster) ->
    wait_for_connections(Node, Cluster),
    restart_process(Node, riak_core_connection_manager),
    wait_for_connections(Node, Cluster).

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
