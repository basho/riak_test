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

-module(repl_rt_sink_rebalance).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    NumNodes = rt_config:get(num_nodes, 4),

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_repl,
             [
              %% turn off fullsync
              {fullsync_on_connect, false},
              {fullsync_interval, disabled},
              {realtime_connection_rebalance_max_delay_secs, 10}
             ]}
    ],

    Nodes = rt:deploy_nodes(NumNodes, Conf, [riak_kv, riak_repl]),
    {ANodes, BNodes} = lists:split(2, Nodes),

    %% Set up to grep logs to verify messages
    [rt:setup_log_capture(Node) || Node <- ANodes],

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodes]),

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

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

    [?assertEqual(ok,verify_connectivity(Node)) || Node <- ANodes],
    [?assertEqual(ok,verify_sinks(Node)) || Node <- BNodes],

    rt:stop(BFirst),
    ?assertEqual(ok, rt:wait_until_unpingable(BFirst)),
    rt:start(BFirst),
    ?assertEqual(ok, rt:wait_until_nodes_ready([BFirst])),
    ?assertEqual(ok, rt:wait_until_all_members(BNodes)),
    ?assertEqual(ok, rt:wait_until_ring_converged(BNodes)),
    rt:wait_until(AFirst, fun(Node) ->
        Whereis = rpc:call(Node, erlang, whreis, [riak_core_connection_manager]),
        is_pid(Whereis)
    end),
    repl_util:wait_for_connection(AFirst, "B"),

    [?assertEqual(ok,verify_sinks(Node)) || Node <- BNodes],
    [check_for_badrecord(Node) || Node <- ANodes],

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

%% @doc Verify each sink node is running at least one sink
verify_sinks(Node) ->
    rt:wait_until(Node, fun(N) ->
                Sinks = rpc:call(N,
                                             supervisor,
                                             which_children,
                                             [riak_repl2_rtsink_conn_sup]),
                lager:info("Waiting for sinks on ~p: ~p.",
                           [Node, Sinks]),
                Sinks =/= []
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

%% @doc Check for a known badrecord error in the logs of the source nodes
check_for_badrecord(Node) ->
    lager:info("Looking for error: {badrecord,state}"),
    Pattern = "error.*{badrecord,state}*",
    Res = rt:expect_in_log(Node, Pattern),
    ?assertEqual(false,Res).
