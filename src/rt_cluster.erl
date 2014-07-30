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
         setup/2,
         config/0,
         augment_config/3,
         deploy_nodes/1,
         deploy_nodes/2,
         deploy_clusters/1,
         build_cluster/1,
         build_cluster/2,
         build_cluster/3,
         build_clusters/1,
         clean_cluster/1,
         join_cluster/1,
         clean_data_dir/1,
         clean_data_dir/2,
         try_nodes_ready/3,
         versions/0,
         teardown/0]).
-export([maybe_wait_for_transfers/2]).

-include("rt.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

%% @doc Default properties used if a riak_test module does not specify
%% a custom properties function.
-spec properties() -> rt_properties().
properties() ->
    #rt_properties{config=config()}.

-spec setup(rt_properties(), proplists:proplist()) ->
                   {ok, rt_properties()} | {error, term()}.
setup(Properties, MetaData) ->
    rt_config:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    RollingUpgrade = proplists:get_value(rolling_upgrade,
                                         MetaData,
                                         Properties#rt_properties.rolling_upgrade),
    Version = Properties#rt_properties.start_version,
    Versions = [{Version, Properties#rt_properties.config} ||
                   _ <- lists:seq(1, Properties#rt_properties.node_count)],
    Nodes = deploy_or_build_cluster(Versions, Properties#rt_properties.make_cluster),

    maybe_wait_for_transfers(Nodes, Properties#rt_properties.wait_for_transfers),
    UpdProperties = Properties#rt_properties{nodes=Nodes,
                                             rolling_upgrade=RollingUpgrade},
    {ok, UpdProperties}.

deploy_or_build_cluster(Versions, true) ->
    build_cluster(Versions);
deploy_or_build_cluster(Versions, false) ->
    deploy_nodes(Versions).

%% @doc Deploy a set of freshly installed Riak nodes, returning a list of the
%%      nodes deployed.
%% @todo Re-add -spec after adding multi-version support
deploy_nodes(Versions) when is_list(Versions) ->
    deploy_nodes(Versions, [riak_kv]);
deploy_nodes(NumNodes) when is_integer(NumNodes) ->
    deploy_nodes([ current || _ <- lists:seq(1, NumNodes)]).

%% @doc Deploy a set of freshly installed Riak nodes with the given
%%      `InitialConfig', returning a list of the nodes deployed.
-spec deploy_nodes(NumNodes :: integer(), any()) -> [node()].
deploy_nodes(NumNodes, InitialConfig) when is_integer(NumNodes) ->
    NodeConfig = [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)],
    deploy_nodes(NodeConfig);
deploy_nodes(Versions, Services) ->
    NodeConfig = [ rt_config:version_to_config(Version) || Version <- Versions ],
    Nodes = ?HARNESS:deploy_nodes(NodeConfig),
    lager:info("Waiting for services ~p to start on ~p.", [Services, Nodes]),
    [ ok = rt:wait_for_service(Node, Service) || Node <- Nodes,
                                              Service <- Services ],
    Nodes.

deploy_clusters(Settings) ->
    ClusterConfigs = [case Setting of
                          Configs when is_list(Configs) ->
                              Configs;
                          NumNodes when is_integer(NumNodes) ->
                              [{current, default} || _ <- lists:seq(1, NumNodes)];
                          {NumNodes, InitialConfig} when is_integer(NumNodes) ->
                              [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)];
                          {NumNodes, Vsn, InitialConfig} when is_integer(NumNodes) ->
                              [{Vsn, InitialConfig} || _ <- lists:seq(1,NumNodes)]
                      end || Setting <- Settings],
    ?HARNESS:deploy_clusters(ClusterConfigs).

build_clusters(Settings) ->
    Clusters = deploy_clusters(Settings),
    [begin
         join_cluster(Nodes),
         lager:info("Cluster built: ~p", [Nodes])
     end || Nodes <- Clusters],
    Clusters.

maybe_wait_for_transfers(Nodes, true) ->
    lager:info("Waiting for transfers"),
    rt:wait_until_transfers_complete(Nodes);
maybe_wait_for_transfers(_Nodes, false) ->
    ok.

%% @doc Safely construct a new cluster and return a list of the deployed nodes
%% @todo Add -spec and update doc to reflect mult-version changes
build_cluster(Versions) when is_list(Versions) ->
    build_cluster(length(Versions), Versions, default);
build_cluster(NumNodes) ->
    build_cluster(NumNodes, default).

%% @doc Safely construct a `NumNode' size cluster using
%%      `InitialConfig'. Return a list of the deployed nodes.
build_cluster(NumNodes, InitialConfig) ->
    build_cluster(NumNodes, [], InitialConfig).

build_cluster(NumNodes, Versions, InitialConfig) ->
    %% Deploy a set of new nodes
    Nodes =
        case Versions of
            [] ->
                deploy_nodes(NumNodes, InitialConfig);
            _ ->
                deploy_nodes(Versions)
        end,

    join_cluster(Nodes),
    lager:info("Cluster built: ~p", [Nodes]),
    Nodes.

join_cluster(Nodes) ->
    %% Ensure each node owns 100% of it's own ring
    [?assertEqual([Node], rt_ring:owners_according_to(Node)) || Node <- Nodes],

    %% Join nodes
    [Node1|OtherNodes] = Nodes,
    case OtherNodes of
        [] ->
            %% no other nodes, nothing to join/plan/commit
            ok;
        _ ->
            %% ok do a staged join and then commit it, this eliminates the
            %% large amount of redundant handoff done in a sequential join
            [rt_node:staged_join(Node, Node1) || Node <- OtherNodes],
            rt_node:plan_and_commit(Node1),
            try_nodes_ready(Nodes, 3, 500)
    end,

    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),

    %% Ensure each node owns a portion of the ring
    rt:wait_until_nodes_agree_about_ownership(Nodes),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),
    ok.

try_nodes_ready([Node1 | _Nodes], 0, _SleepMs) ->
    lager:info("Nodes not ready after initial plan/commit, retrying"),
    rt_node:plan_and_commit(Node1);
try_nodes_ready(Nodes, N, SleepMs) ->
    ReadyNodes = [Node || Node <- Nodes, rt:is_ready(Node) =:= true],
    case ReadyNodes of
        Nodes ->
            ok;
        _ ->
            timer:sleep(SleepMs),
            try_nodes_ready(Nodes, N-1, SleepMs)
    end.

%% @doc Stop nodes and wipe out their data directories
clean_cluster(Nodes) when is_list(Nodes) ->
    [rt:stop_and_wait(Node) || Node <- Nodes],
    clean_data_dir(Nodes).

clean_data_dir(Nodes) ->
    clean_data_dir(Nodes, "").

clean_data_dir(Nodes, SubDir) when not is_list(Nodes) ->
    clean_data_dir([Nodes], SubDir);
clean_data_dir(Nodes, SubDir) when is_list(Nodes) ->
    ?HARNESS:clean_data_dir(Nodes, SubDir).

%% @doc Shutdown every node, this is for after a test run is complete.
teardown() ->
    %% stop all connected nodes, 'cause it'll be faster that
    %%lager:info("RPC stopping these nodes ~p", [nodes()]),
    %%[ rt_node:stop(Node) || Node <- nodes()],
    %% Then do the more exhaustive harness thing, in case something was up
    %% but not connected.
    ?HARNESS:teardown().

versions() ->
    ?HARNESS:versions().

config() ->
    [{riak_core, [{handoff_concurrency, 11}]},
     {riak_search, [{enabled, true}]},
     {riak_pipe, [{worker_limit, 200}]}].

augment_config(Section, Property, Config) ->
    UpdSectionConfig = update_section(Section, Property, lists:keyfind(Section, 1, Config)),
    lists:keyreplace(Section, 1, Config, UpdSectionConfig).

update_section(Section, Property, false) ->
    {Section, [Property]};
update_section(Section, Property, {Section, SectionConfig}) ->
    {Section, [Property | SectionConfig]}.
