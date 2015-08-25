%% -------------------------------------------------------------------
%%
%% Copyright (c) 2010-2015 Basho Technologies, Inc.
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

-module(verify_snmp_repl).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-compile({parse_transform, rt_intercept_pt}).

confirm() ->
    Clusters = make_clusters(["cluster-1", "cluster-2", "cluster-3"], 1),
    [{_, Leader, _}|_] = Clusters,
    intercept_riak_snmp_stat_poller(Leader),
    wait_for_snmp_stat_poller().

make_clusters(Names, NodeCount) ->
    ClusterCount = length(Names),
    Config = [{riak_snmp, [{polling_interval, 100}]}],
    AllNodes = make_nodes(NodeCount, ClusterCount, Config),
    Clusters = lists:zip(Names, AllNodes),
    lists:foreach(fun make_named_cluster/1, Clusters),
    lists:foreach(fun wait_until_ring_converged/1, Clusters),
    lists:foreach(fun wait_until_leader_converge/1, Clusters),

    ClustersWithLeaders = [{Name, repl_util:get_leader(hd(Nodes)), Nodes} || {Name, Nodes} <- Clusters],
    enable_realtime(ClustersWithLeaders),
    ClustersWithLeaders.

intercept_riak_snmp_stat_poller(Node) ->
    RiakTestProcess = self(),
    rt_intercept:add(
        Node,
        {riak_snmp_stat_poller,
         [{{set_rows, 4},
           {[RiakTestProcess],
            fun(Table, Indexes, Cols, IndexCol)
              when Table =:= replRealtimeStatusTable; Table =:= replFullsyncStatusTable ->
                try
                    riak_snmp_stat_poller_orig:set_rows_orig(Table, Indexes, Cols, IndexCol),
                    RiakTestProcess ! pass
                catch
                    Exception:Reason ->
                        RiakTestProcess ! {fail, {Exception, Reason}},
                        error({Exception, Reason})
                end
            end}}]}).

wait_for_snmp_stat_poller() ->
    receive
        pass ->
            pass;
        {fail, Reason} ->
            lager:error("Failure in wait_for_snmp_stat_poller: ~p~n", [Reason]),
            error({fail, Reason});
        X ->
            lager:error("Unknown failure in wait_for_snmp_stat_poller: ~p~n", [X]),
            error(X)
    after
        1000 ->
            Message =  "Timeout waiting for snmp_stat_poller.",
            lager:error(Message),
            error({timeout, Message})
    end.

make_nodes(NodeCount, ClusterCount, Config) ->
    Nodes = rt:deploy_nodes(NodeCount * ClusterCount, Config),
    sublists(Nodes, NodeCount).

sublists(List, Len) ->
    lists:map(
        fun(I) -> lists:sublist(List, I, Len) end,
        lists:seq(1, length(List), Len)).

make_named_cluster({Name, Nodes}) ->
    repl_util:make_cluster(Nodes),
    repl_util:name_cluster(hd(Nodes), Name).

wait_until_ring_converged({_Name, Nodes}) ->
    rt:wait_until_ring_converged(Nodes).

wait_until_leader_converge({_Name, Nodes}) ->
    repl_util:wait_until_leader_converge(Nodes).

enable_realtime([{_, Node, _}|OtherClusters]) ->
    lists:foreach(
        fun({Cluster, _, _}) ->
            repl_util:enable_realtime(Node, Cluster)
        end,
        OtherClusters).

