%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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

-module(replication2_rt_connection_balancing).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(HB_TIMEOUT,  2000).

confirm() ->
    Clusters = create_clusters_3_3(),
    join_nodes_3_2(Clusters),
    ?assertEqual(pass, verify_peer_connections_3_2(Clusters)),
    ?assertEqual(pass, add_sink_node(Clusters)),
    ?assertEqual(pass, verify_peer_connections_3_3(Clusters)),
    teardown(Clusters),
    pass.

create_clusters_3_3() ->
    %% We want reblanacing to happen immediately after detection of changed config,
    %% so set realtime_connection_rebalance_max_delay_secs=0.
    Conf = [
            {riak_repl, [
                         {realtime_connection_rebalance_max_delay_secs, 0}]
            }
    ],
    rt:set_advanced_conf(all, Conf),
    rt:deploy_clusters([3,3]).

join_nodes_3_2(Clusters) ->
    [ANodes, BNodes] = Clusters,
    rt:join_cluster(ANodes),
    lager:info("Cluster A built: ~p", [ANodes]),
    [BNode1, BNode2, _BNode3] = BNodes,
    BNodesToJoin = [BNode1, BNode2],
    rt:join_cluster(BNodesToJoin),
    lager:info("Cluster B built: ~p", [BNodesToJoin]),

    lager:info("ANodes: ~p", [ANodes]),
    lager:info("BNodes: ~p", [BNodesToJoin]),

    lager:info("Waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(ANodes)),
    AFirst = hd(ANodes),

    lager:info("Waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodesToJoin)),
    BFirst = hd(BNodesToJoin),

    lager:info("Naming A"),
    repl_util:name_cluster(AFirst, "A"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes)),

    lager:info("Naming B"),
    repl_util:name_cluster(BFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(BNodesToJoin)),

    lager:info("Connecting A to B"),
    connect_clusters(AFirst, BFirst),

    lager:info("Enabling realtime replication from A to B."),
    repl_util:enable_realtime(AFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes)),
    repl_util:start_realtime(AFirst, "B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(ANodes)),

    [ANodes, BNodes].

add_sink_node(TestSetup) ->
    [_ANodes, BNodes] = TestSetup,
    [BNode1, _BNode2, BNode3] = BNodes,
    rt:join(BNode3, BNode1),
    lager:info("Waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge(BNodes)),
    lager:info("Waiting until ring converges on cluster B"),
    ?assertEqual(ok, rt:wait_until_ring_converged(BNodes)),
    lager:info("Added node from Sink cluster. Waiting 20s for clusters to communicate rt change."),
    timer:sleep(timer:seconds(20)),
    pass.

verify_peer_connections_3_3(TestSetup) ->
    lager:info("Verify we have chosen the right connections for 3 source nodes and 3 sink nodes."),
    [[ANode1, ANode2, ANode3], _BNodes] = TestSetup,

    %% For this particular setup, nodes should always get exactly these peers:
    ?assertEqual("127.0.0.1:10066", get_rt_peer(ANode1)),
    ?assertEqual("127.0.0.1:10056", get_rt_peer(ANode2)),
    ?assertEqual("127.0.0.1:10046", get_rt_peer(ANode3)),
    pass.

verify_peer_connections_3_2(TestSetup) ->
    lager:info("Verify we have chosen the right connections for 3 source nodes and 2 sink nodes."),
    [[ANode1, ANode2, ANode3], _BNodes] = TestSetup,

    %% For this particular setup, nodes should always get exactly these peers:
    ?assertEqual("127.0.0.1:10056", get_rt_peer(ANode1)),
    ?assertEqual("127.0.0.1:10046", get_rt_peer(ANode2)),
    ?assertEqual("127.0.0.1:10056", get_rt_peer(ANode3)),
    pass.

teardown(TestSetup) ->
    [ANodes, BNodes] = TestSetup,
    rt:clean_cluster(ANodes),
    rt:clean_cluster(BNodes).

get_rt_peer(Node) ->
    rt:wait_until(Node,
        fun(_) ->
            case rpc:call(Node, riak_repl2_rtsource_conn_sup, enabled, []) of
                [] -> false;
            _ -> true
        end
    end),

    [{_Remote, Pid}] = rpc:call(Node, riak_repl2_rtsource_conn_sup, enabled, []),
    Status = rpc:call(Node, riak_repl2_rtsource_conn, status, [Pid]),
    Socket = proplists:get_value(socket, Status),
    lager:info("Socket: ~p", [Socket]),
    proplists:get_value(peername, Socket).


%% @doc Connect two clusters for replication using their respective
%%      leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port).
