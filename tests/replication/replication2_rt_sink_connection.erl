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

-module(replication2_rt_sink_connection).
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

    Nodes = rt_cluster:deploy_nodes(NumNodes, Conf),
    {ANodes, Rest} = lists:split(2, Nodes),
    {BNodes, CNodes} = lists:split(2, Rest),

    lager:info("Loading intercepts."),
    CNode = hd(CNodes),
    rt_intercept:load_code(CNode),
    rt_intercept:add(CNode, {riak_repl_ring_handler,
                            [{{handle_event, 2}, slow_handle_event}]}),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),
    lager:info("CNodes: ~p", [CNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    % lager:info("Waiting for cluster A to converge"),
    % rt:wait_until_ring_converged(ANodes),

    % lager:info("Waiting for cluster B to converge"),
    % rt:wait_until_ring_converged(BNodes),

    lager:info("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    lager:info("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    BFirst = hd(BNodes),

    lager:info("Naming A"),
    repl_util:name_cluster(AFirst, "A"),

    lager:info("Naming B"),
    repl_util:name_cluster(BFirst, "B"),

    connect_clusters(AFirst, BFirst),

    enable_rt(AFirst, ANodes),

    lager:info("Adding 4th node to the A cluster"),
    rt_node:join(CNode, AFirst),

    [verify_connectivity(Node) || Node <- ANodes],

    verify_connectivity(CNode),

    pass.

%% @doc Verify connectivity between sources and sink.
verify_connectivity(Node) ->
    rt:wait_until(Node, fun(N) ->
                {ok, Connections} = rpc:call(N,
                                             riak_core_cluster_mgr,
                                             get_connections,
                                             []),
                lager:info("Waiting for sink connections on ~p: ~p.",
                           [Node, Connections]),
                Connections =/= []
        end).

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    lager:info("Connect cluster A:~p to B on port ~p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port).

%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_rt(LeaderA, ANodes) ->
    lager:info("Enabling RT replication: ~p ~p.", [LeaderA, ANodes]),
    repl_util:enable_realtime(LeaderA, "B"),
    repl_util:start_realtime(LeaderA, "B").
