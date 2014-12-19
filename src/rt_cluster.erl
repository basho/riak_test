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

-module(rt_cluster).
-include_lib("eunit/include/eunit.hrl").

-export([properties/0,
         setup/1,
         augment_config/3,
         clean_cluster/1,
         join_cluster/2,
         clean_data_dir/1,
         clean_data_dir/2,
         try_nodes_ready/3,
         versions/0,
         teardown/0]).

-export([maybe_wait_for_transfers/3]).

-include("rt.hrl").

%% @doc Default properties used if a riak_test module does not specify
%% a custom properties function.
-spec properties() -> rt_properties:properties().
properties() ->
    rt_properties:new().

-spec setup(rt_properties:properties()) ->
                   {ok, rt_properties:properties()} | {error, term()}.
setup(Properties) ->
    case form_clusters(Properties) of
        {ok, ClusterNodes} ->
            maybe_wait_for_transfers(rt_properties:get(node_ids, Properties),
                                     rt_properties:get(node_map, Properties),
                                     rt_properties:get(wait_for_transfers, Properties)),
            Clusters = prepare_clusters(ClusterNodes, Properties),
            create_bucket_types(Clusters, Properties),
            rt_properties:set(clusters, Clusters, Properties);
        Error ->
            Error
    end.

-spec create_bucket_types([rt_cluster_info:cluster_info()], rt_properties:properties()) -> no_return().
create_bucket_types(Clusters, Properties) ->
    BucketTypes = rt_properties:get(bucket_types, Properties),
    create_bucket_types(Clusters, Properties, BucketTypes).

-spec create_bucket_types([rt_cluster_info:cluster_info()],
                          rt_properties:properties(),
                          rt_properties:bucket_types()) -> no_return().
create_bucket_types(_Clusters, _Properties, []) ->
    ok;
create_bucket_types([Cluster], Properties, BucketTypes) ->
    NodeMap = rt_properties:get(node_map, Properties),
    NodeIds = rt_cluster_info:get(node_ids, Cluster),
    Nodes = [rt_node:node_name(NodeId, NodeMap) || NodeId <- NodeIds],
    lists:foldl(fun maybe_create_bucket_type/2, [{Nodes, 1}], BucketTypes);
create_bucket_types(Clusters, Properties, BucketTypes) ->
    NodeMap = rt_properties:get(node_map, Properties),
    [begin
         NodeIds = rt_cluster_info:get(node_ids, Cluster),
         Nodes = [rt_node:node_name(NodeId, NodeMap) || NodeId <- NodeIds],
         lists:foldl(fun maybe_create_bucket_type/2, {Nodes, ClusterIndex}, BucketTypes)
     end || {Cluster, ClusterIndex} <- lists:zip(Clusters, lists:seq(1, length(Clusters)))].

maybe_create_bucket_type({ClusterIndex, {TypeName, TypeProps}},
                         {Nodes, ClusterIndex}) ->
    rt_bucket_types:create_and_wait(Nodes, TypeName, TypeProps),
    {Nodes, ClusterIndex};
maybe_create_bucket_type({_ApplicableIndex, {_TypeName, _TypeProps}},
                         {Nodes, _ClusterIndex}) ->
    %% This bucket type does not apply to this cluster
    {Nodes, _ClusterIndex};
maybe_create_bucket_type({TypeName, TypeProps}, {Nodes, _ClusterIndex}) ->
    %% This bucket type applies to all clusters
    rt_bucket_types:create_and_wait(Nodes, TypeName, TypeProps),
    {Nodes, _ClusterIndex}.

-spec prepare_clusters([list(string())], rt_properties:properties()) ->
                                    [rt_cluster_info:cluster_info()].
prepare_clusters([ClusterNodes], _Properties) ->
    rt_cluster_info:new([{node_ids, ClusterNodes}]);
prepare_clusters(ClusterNodesList, Properties) ->
    %% If the count of clusters is > 1 the assumption is made that the
    %% test is exercising replication and some extra
    %% made. This to avoid some noisy and oft-repeated setup
    %% boilerplate in every replication test.
    NodeMap = rt_properties:get(node_map, Properties),
    {Clusters, _, _} = lists:foldl(fun prepare_cluster/2,
                                {[], 1, NodeMap},
                                ClusterNodesList),
    lists:reverse(Clusters).

-type prepare_cluster_acc() :: {[rt_cluster_info:cluster_info()], char(), proplists:proplist()}.
-spec prepare_cluster([string()], prepare_cluster_acc()) -> prepare_cluster_acc().
prepare_cluster(NodeIds, {Clusters, Name, NodeMap}) ->
    Nodes = [rt_node:node_name(NodeId, NodeMap) || NodeId <- NodeIds],
    repl_util:name_cluster(hd(Nodes), integer_to_list(Name)),
    repl_util:wait_until_leader_converge(Nodes),
    Leader = repl_util:get_leader(hd(Nodes)),
    Cluster = rt_cluster_info:new([{node_ids, NodeIds},
                                   {leader, Leader},
                                   {name, Name}]),
    {[Cluster | Clusters], Name+1, NodeMap}.

-type clusters() :: [rt_cluster_info:cluster_info()].
-spec form_clusters(rt_properties:properties()) -> clusters().
form_clusters(Properties) ->
    NodeIds = rt_properties:get(node_ids, Properties),
    NodeMap = rt_properties:get(node_map, Properties),
    ClusterCount = rt_properties:get(cluster_count, Properties),
    ClusterWeights = rt_properties:get(cluster_weights, Properties),
    MakeCluster = rt_properties:get(make_cluster, Properties),
    case divide_nodes(NodeIds, ClusterCount, ClusterWeights) of
        {ok, Clusters} ->
            maybe_join_clusters(Clusters, NodeMap, MakeCluster),
            {ok, Clusters};
        Error ->
            Error
    end.

-spec divide_nodes([string()], pos_integer(), [float()]) ->
                          {ok, [list(string())]} | {error, atom()}.
divide_nodes(Nodes, Count, Weights)
  when length(Nodes) < Count;
       Weights =/= undefined, length(Weights) =/= Count ->
    {error, invalid_cluster_properties};
divide_nodes(Nodes, 1, _) ->
    {ok, [Nodes]};
divide_nodes(Nodes, Count, Weights) ->
    case validate_weights(Weights) of
        true ->
            TotalNodes = length(Nodes),
            NodeCounts = node_counts_from_weights(TotalNodes, Count, Weights),
            {_, Clusters, _} = lists:foldl(fun take_nodes/2, {1, [], Nodes}, NodeCounts),
                {ok, lists:reverse(Clusters)};
        false ->
            {error, invalid_cluster_weights}
    end.

take_nodes(NodeCount, {Index, ClusterAcc, Nodes}) ->
    {NewClusterNodes, RestNodes} = lists:split(NodeCount, Nodes),
    {Index + 1, [NewClusterNodes | ClusterAcc], RestNodes}.

validate_weights(undefined) ->
    true;
validate_weights(Weights) ->
    not lists:sum(Weights) > 1.0 .

node_counts_from_weights(NodeCount, ClusterCount, undefined) ->
    %% Split the nodes evenly. A remainder of nodes is handled by
    %% distributing one node per cluster until the remainder is
    %% exhausted.
    NodesPerCluster = NodeCount div ClusterCount,
    Remainder = NodeCount rem ClusterCount,
    [NodesPerCluster + remainder_to_apply(Remainder, ClusterIndex) ||
        ClusterIndex <- lists:seq(1, ClusterCount)];
node_counts_from_weights(NodeCount, ClusterCount, Weights) ->
    InitialNodeCounts = [node_count_from_weight(NodeCount, Weight) || Weight <- Weights],
    Remainder = NodeCount - lists:sum(InitialNodeCounts),
    [ClusterNodeCount + remainder_to_apply(Remainder, ClusterIndex) ||
        {ClusterIndex, ClusterNodeCount}
            <- lists:zip(lists:seq(1, ClusterCount), InitialNodeCounts)].

node_count_from_weight(TotalNodes, Weight) ->
    RawNodeCount = TotalNodes * Weight,
    IntegerPortion = trunc(RawNodeCount),
    Remainder = RawNodeCount - IntegerPortion,
    case Remainder >= 0.5 of
        true ->
            IntegerPortion + 1;
        false ->
            IntegerPortion
    end.

remainder_to_apply(Remainder, Index) when Remainder > Index;
                                          Remainder =:= 0 ->
    0;
remainder_to_apply(_Remainder, _Index) ->
    1.

maybe_join_clusters(Clusters, NodeMap, true) ->
    [join_cluster(ClusterNodes, NodeMap) || ClusterNodes <- Clusters];
maybe_join_clusters(_Clusters, _NodeMap, false) ->
    ok.

maybe_wait_for_transfers(NodeIds, NodeMap, true) ->
    lager:info("Waiting for transfers"),
    rt:wait_until_transfers_complete([rt_node:node_name(NodeId, NodeMap)
                                      || NodeId <- NodeIds]);
maybe_wait_for_transfers(_NodeIds, _NodeMap, false) ->
    ok.

join_cluster(NodeIds, NodeMap) ->
    NodeNames = [rt_node:node_name(NodeId, NodeMap) || NodeId <- NodeIds],
    %% Ensure each node owns 100% of it's own ring
    [?assertEqual([Node], rt_ring:owners_according_to(Node)) || Node <- NodeNames],

    %% Join nodes
    [Node1|OtherNodes] = NodeNames,
    case OtherNodes of
        [] ->
            %% no other nodes, nothing to join/plan/commit

            ok;
        _ ->

            %% ok do a staged join and then commit it, this eliminates the
            %% large amount of redundant handoff done in a sequential join
            [rt_node:staged_join(Node, Node1) || Node <- OtherNodes],
            rt_node:plan_and_commit(Node1),
            try_nodes_ready(NodeNames, 3, 500)
    end,

    ?assertEqual(ok, rt_node:wait_until_nodes_ready(NodeNames)),

    %% Ensure each node owns a portion of the ring
    rt_node:wait_until_nodes_agree_about_ownership(NodeNames),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(NodeNames)),
    ok.

try_nodes_ready([Node1 | _Nodes], 0, _SleepMs) ->
    lager:info("Nodes not ready after initial plan/commit, retrying"),
    rt_node:plan_and_commit(Node1);
try_nodes_ready(Nodes, N, SleepMs) ->
    ReadyNodes = [Node || Node <- Nodes, rt_node:is_ready(Node) =:= true],
    case ReadyNodes of
        Nodes ->
            ok;
        _ ->
            timer:sleep(SleepMs),
            try_nodes_ready(Nodes, N-1, SleepMs)
    end.

%% @doc Stop nodes and wipe out their data directories
clean_cluster(Nodes) when is_list(Nodes) ->
    [rt_node:stop_and_wait(Node) || Node <- Nodes],
    clean_data_dir(Nodes).

clean_data_dir(Nodes) ->
    clean_data_dir(Nodes, "").

clean_data_dir(Nodes, SubDir) when not is_list(Nodes) ->
    clean_data_dir([Nodes], SubDir);
clean_data_dir(Nodes, SubDir) when is_list(Nodes) ->
    rt_harness:clean_data_dir(Nodes, SubDir).

%% @doc Shutdown every node, this is for after a test run is complete.
teardown() ->
    %% stop all connected nodes, 'cause it'll be faster that
    %%lager:info("RPC stopping these nodes ~p", [nodes()]),
    %%[ rt_node:stop(Node) || Node <- nodes()],
    %% Then do the more exhaustive harness thing, in case something was up
    %% but not connected.
    rt_harness:teardown().

versions() ->
    rt_harness:versions().

augment_config(Section, Property, Config) ->
    UpdSectionConfig = update_section(Section,
                                      Property,
                                      lists:keyfind(Section, 1, Config)),
    lists:keyreplace(Section, 1, Config, UpdSectionConfig).

update_section(Section, Property, false) ->
    {Section, [Property]};
update_section(Section, Property, {Section, SectionConfig}) ->
    {Section, [Property | SectionConfig]}.
