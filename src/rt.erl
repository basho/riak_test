%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2015 Basho Technologies, Inc.
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

%% @doc
%% Implements the base `riak_test' API, providing the ability to control
%% nodes in a Riak cluster as well as perform commonly reused operations.
%% Please extend this module with new functions that prove useful between
%% multiple independent tests.
-module(rt).
-deprecated(module).
-include("rt.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-export([
         admin/2,
         admin/3,
         assert_nodes_agree_about_ownership/1,
         async_start/1,
         attach/2,
         attach_direct/2,
         brutal_kill/1,
         build_cluster/1,
         build_cluster/2,
         build_cluster/3,
         build_clusters/1,
         join_cluster/1,
         capability/2,
         capability/3,
         check_singleton_node/1,
         check_ibrowse/0,
         claimant_according_to/1,
         clean_cluster/1,
         clean_data_dir/1,
         clean_data_dir/2,
         cmd/1,
         cmd/2,
         connection_info/1,
         console/2,
         create_and_activate_bucket_type/3,
         deploy_nodes/1,
         deploy_nodes/2,
         deploy_nodes/3,
         deploy_clusters/1,
         down/2,
         enable_search_hook/2,
         expect_in_log/2,
         get_deps/0,
         get_ip/1,
         get_node_logs/0,
         get_replica/5,
         get_ring/1,
         get_version/0,
         get_version/1,
         heal/1,
         http_url/1,
         https_url/1,
         httpc/1,
         httpc_read/3,
         httpc_write/4,
         is_mixed_cluster/1,
         is_pingable/1,
         join/2,
         leave/1,
         load_modules_on_nodes/2,
         log_to_nodes/2,
         log_to_nodes/3,
         members_according_to/1,
         nearest_ringsize/1,
         owners_according_to/1,
         partition/2,
         partitions_for_node/1,
         pbc/1,
         pbc/2,
         pbc_read/3,
         pbc_read/4,
         pbc_read_check/4,
         pbc_read_check/5,
         pbc_set_bucket_prop/3,
         pbc_write/4,
         pbc_put_dir/3,
         pbc_put_file/4,
         pbc_really_deleted/3,
         pmap/2,
         post_result/2,
         product/1,
         priv_dir/0,
         remove/2,
         riak/2,
         riak_repl/2,
         rpc_get_env/2,
         set_backend/1,
         set_backend/2,
         set_conf/2,
         set_advanced_conf/2,
         setup_harness/2,
         setup_log_capture/1,
         slow_upgrade/3,
         stream_cmd/1, stream_cmd/2,
         spawn_cmd/1,
         spawn_cmd/2,
         search_cmd/2,
         start/1,
         start_and_wait/1,
         status_of_according_to/2,
         stop/1,
         stop_and_wait/1,
         str/2,
         systest_read/2,
         systest_read/3,
         systest_read/5,
         systest_read/6,
         systest_write/2,
         systest_write/3,
         systest_write/5,
         systest_write/6,
         teardown/0,
         update_app_config/2,
         upgrade/2,
         upgrade/3,
         versions/0,
         wait_for_cluster_service/2,
         wait_for_cmd/1,
         wait_for_service/2,
         wait_for_control/1,
         wait_for_control/2,
         wait_until/3,
         wait_until/2,
         wait_until/1,
         wait_until_aae_trees_built/1,
         wait_until_all_members/1,
         wait_until_all_members/2,
         wait_until_bucket_props/3,
         wait_until_bucket_type_visible/2,
         wait_until_capability/3,
         wait_until_capability/4,
         wait_until_connected/1,
         wait_until_legacy_ringready/1,
         wait_until_owners_according_to/2,
         wait_until_no_pending_changes/1,
         wait_until_nodes_agree_about_ownership/1,
         wait_until_nodes_ready/1,
         wait_until_pingable/1,
         wait_until_ready/1,
         wait_until_registered/2,
         wait_until_ring_converged/1,
         wait_until_status_ready/1,
         wait_until_transfers_complete/1,
         wait_until_unpingable/1,
         wait_until_bucket_type_status/3,
         whats_up/0
        ]).

-define(HARNESS, (rt_config:get(rt_harness))).

priv_dir() ->
    rt2:priv_dir().

%% @doc gets riak deps from the appropriate harness
-spec get_deps() -> list().
get_deps() ->
    rt2:get_deps().

%% @doc if String contains Substr, return true.
-spec str(string(), string()) -> boolean().
str(String, Substr) ->
    rt2:str(String, Substr).

-spec set_conf(atom(), [{string(), string()}]) -> ok.
set_conf(Node, NameValuePairs) ->
    stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    rt_config:set_conf(Node, NameValuePairs),
    start(Node).

-spec set_advanced_conf(atom(), [{string(), string()}]) -> ok.
set_advanced_conf(all, NameValuePairs) ->
    rt_config:set_advanced_conf(all, NameValuePairs);
set_advanced_conf(Node, NameValuePairs) ->
    stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    rt_config:set_advanced_conf(Node, NameValuePairs),
    start(Node).

%% @doc Rewrite the given node's app.config file, overriding the varialbes
%%      in the existing app.config with those in `Config'.
update_app_config(all, Config) ->
    rt_config:update_app_config(all, Config);
update_app_config(Node, Config) ->
    stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    rt_config:update_app_config(Node, Config),
    start(Node).

%% @doc Helper that returns first successful application get_env result,
%%      used when different versions of Riak use different app vars for
%%      the same setting.
rpc_get_env(Node, AppVars) ->
    rt2:rpc_get_env(Node, AppVars).

-type interface() :: {http, tuple()} | {pb, tuple()}.
-type interfaces() :: [interface()].
-type conn_info() :: [{node(), interfaces()}].

-spec connection_info(node() | [node()]) -> interfaces() | conn_info().
connection_info(Node) ->
    rt2:connection_info(Node).

-spec get_pb_conn_info(node()) -> [{inet:ip_address(), pos_integer()}].
get_pb_conn_info(Node) ->
    rt_pb:get_pb_conn_info(Node).

-spec get_http_conn_info(node()) -> [{inet:ip_address(), pos_integer()}].
get_http_conn_info(Node) ->
    rt_http:get_http_conn_info(Node).

-spec get_https_conn_info(node()) -> [{inet:ip_address(), pos_integer()}].
get_https_conn_info(Node) ->
    rt_http:get_https_conn_info(Node).

%% @doc Deploy a set of freshly installed Riak nodes, returning a list of the
%%      nodes deployed.
%% @todo Re-add -spec after adding multi-version support
deploy_nodes(Versions) when is_list(Versions) ->
    deploy_nodes(Versions, [riak_kv]);
deploy_nodes(NumNodes) when is_integer(NumNodes) ->
    [NodeIds, NodeMap, _] = allocate_nodes(NumNodes, head),
    deploy_nodes(NodeIds, NodeMap, head, rt_properties:default_config(), [riak_kv]).

%% @doc Deploy a set of freshly installed Riak nodes with the given
%%      `InitialConfig', returning a list of the nodes deployed.
-spec deploy_nodes(NumNodes :: integer(), any()) -> [node()].
deploy_nodes(NumNodes, InitialConfig) when is_integer(NumNodes) ->
    deploy_nodes(NumNodes, InitialConfig, [riak_kv]);
deploy_nodes(Versions, Services) ->
    NodeConfig = [ version_to_config(Version) || Version <- Versions ],
    lager:debug("Starting nodes using config ~p and versions ~p", [NodeConfig, Versions]),

    Nodes = rt_harness:deploy_nodes(NodeConfig),
    lager:info("Waiting for services ~p to start on ~p.", [Services, Nodes]),
    [ ok = wait_for_service(Node, Service) || Node <- Nodes,
                                              Service <- Services ],
    Nodes.

deploy_nodes(NumNodes, InitialConfig, Services) when is_integer(NumNodes) ->
    NodeConfig = [{head, InitialConfig} || _ <- lists:seq(1,NumNodes)],
    deploy_nodes(NodeConfig, Services).

deploy_nodes(NodeIds, NodeMap, Version, Config, Services) ->
    _ = rt_harness_util:deploy_nodes(NodeIds, NodeMap, Version, Config, Services),
    lists:foldl(fun({_, NodeName}, Nodes) -> [NodeName|Nodes] end,
                [],
                NodeMap).

allocate_nodes(NumNodes, Version) when is_atom(Version) ->
    allocate_nodes(NumNodes, atom_to_list(Version));
allocate_nodes(NumNodes, Version) ->
    [_, AvailableNodeMap, VersionMap] = rt_harness:available_resources(),
    lager:debug("Available node map ~p and version map ~p.", [AvailableNodeMap, VersionMap]),

    AvailableNodeIds = proplists:get_value(Version, VersionMap),
    lager:debug("Availabe node ids ~p for version ~p", [AvailableNodeIds, Version]),
    AllocatedNodeIds = lists:sublist(AvailableNodeIds, NumNodes),
    lager:debug("Allocated node ids ~p", [AllocatedNodeIds]),

    [AllocatedNodeMap, NodeNames, _] = lists:foldl(
                         fun(NodeId, [AllocatedNodeMapAcc, NodeNamesAcc, Number]) ->
                                 NodeName = proplists:get_value(NodeId, AvailableNodeMap),
                                 [[{NodeId, NodeName}|AllocatedNodeMapAcc], 
                                  orddict:append(NodeId, Number, NodeNamesAcc),  
                                  Number + 1]
                         end,
                         [[], orddict:new(), 1],
                         AllocatedNodeIds),
    lager:debug("AllocatedNodeMap: ~p", [AllocatedNodeMap]),
    [Nodes, Versions] = lists:foldl(
                       fun({NodeId, NodeName}, [NodesAcc, VersionsAcc]) -> 
                               [orddict:append(NodeName, NodeId, NodesAcc),
                               orddict:append(NodeName, Version, VersionsAcc)]
                       end,
                       [orddict:new(), orddict:new()],
                       AllocatedNodeMap),
                               
    lager:debug("Allocated node map ~p", [AllocatedNodeMap]),

    rt_config:set(rt_nodes, Nodes),
    rt_config:set(rt_nodenames, NodeNames),
    rt_config:set(rt_versions, Versions),

    lager:debug("Set rt_nodes: ~p", [ rt_config:get(rt_nodes) ]),
    lager:debug("Set rt_nodenames: ~p", [ rt_config:get(rt_nodenames) ]),
    lager:debug("Set rt_versions: ~p", [ rt_config:get(rt_versions) ]),

    [AllocatedNodeIds, AllocatedNodeMap, VersionMap].

version_to_config(Config) when is_tuple(Config)-> Config;
version_to_config(Version) -> {Version, head}.

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

%% @doc Start the specified Riak node
start(Node) ->
    %% TODO Determine the best way to implement the current version specification. -jsb
    rt_node:start(Node, head).

%% @doc Start the specified Riak `Node' and wait for it to be pingable
start_and_wait(Node) ->
    start(Node),
    ?assertEqual(ok, wait_until_pingable(Node)).

async_start(Node) ->
    %% TODO Determine the best way to implement the current version specification. -jsb
    rt_node:async_start(Node, head).

%% @doc Stop the specified Riak `Node'.
stop(Node) ->
    %% TODO Determine the best way to implement the current version specification. -jsb
    rt_node:stop(Node, head).

%% @doc Stop the specified Riak `Node' and wait until it is not pingable
stop_and_wait(Node) ->
    stop(Node),
    ?assertEqual(ok, wait_until_unpingable(Node)).

%% @doc Upgrade a Riak `Node' to the specified `NewVersion'.
upgrade(Node, NewVersion) ->
    %% GAP: The new API does not provide an analog to this function. -jsb
    ?HARNESS:upgrade(Node, NewVersion).

%% @doc Upgrade a Riak `Node' to the specified `NewVersion' and update
%% the config based on entries in `Config'.
upgrade(Node, NewVersion, Config) ->
    %% TODO Determine the best way to implement the current version specification. -jsb
    rt_node:upgrade(Node, head, NewVersion, Config).

%% @doc Upgrade a Riak node to a specific version using the alternate
%%      leave/upgrade/rejoin approach
%% GAP: rt_node does not current provide 
slow_upgrade(Node, NewVersion, Nodes) ->
    lager:info("Perform leave/upgrade/join upgrade on ~p", [Node]),
    lager:info("Leaving ~p", [Node]),
    leave(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    upgrade(Node, NewVersion),
    lager:info("Rejoin ~p", [Node]),
    join(Node, hd(Nodes -- [Node])),
    lager:info("Wait until all nodes are ready and there are no pending changes"),
    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)),
    ok.

%% @doc Have `Node' send a join request to `PNode'
join(Node, PNode) ->
    rt_node:join(Node, PNode).

%% @doc Have `Node' send a join request to `PNode'
staged_join(Node, PNode) ->
    rt_node:staged_join(Node, PNode).

plan_and_commit(Node) ->
    rt_node:plan_and_commit(Node).

do_commit(Node) ->
    rt_node:do_commit(Node).

maybe_wait_for_changes(Node) ->
    rt2:maybe_wait_for_changes(Node).

%% @doc Have the `Node' leave the cluster
leave(Node) ->
    rt_node:leave(Node).

%% @doc Have `Node' remove `OtherNode' from the cluster
remove(Node, OtherNode) ->
    rt_node:remove(Node, OtherNode).

%% @doc Have `Node' mark `OtherNode' as down
down(Node, OtherNode) ->
    rt_node:down(Node, OtherNode).

%% @doc partition the `P1' from `P2' nodes
%%      note: the nodes remained connected to riak_test@local,
%%      which is how `heal/1' can still work.
partition(P1, P2) ->
    rt_node:partition(P1, P2).

%% @doc heal the partition created by call to `partition/2'
%%      `OldCookie' is the original shared cookie
heal({NewCookie, OldCookie, P1, P2}) ->
    rt_node:heal({NewCookie, OldCookie, P1, P2}).

%% @doc Spawn `Cmd' on the machine running the test harness
spawn_cmd(Cmd) ->
    rt2:spawn_cmd(Cmd).

%% @doc Spawn `Cmd' on the machine running the test harness
spawn_cmd(Cmd, Opts) ->
    rt2:spawn_cmd(Cmd, Opts).

%% @doc Wait for a command spawned by `spawn_cmd', returning
%%      the exit status and result
wait_for_cmd(CmdHandle) ->
    rt2:wait_for_cmd(CmdHandle).

%% @doc Spawn `Cmd' on the machine running the test harness, returning
%%      the exit status and result
cmd(Cmd) ->
    rt2:cmd(Cmd).

%% @doc Spawn `Cmd' on the machine running the test harness, returning
%%      the exit status and result
cmd(Cmd, Opts) ->
    rt2:cmd(Cmd, Opts).

%% @doc pretty much the same as os:cmd/1 but it will stream the output to lager.
%%      If you're running a long running command, it will dump the output
%%      once per second, as to not create the impression that nothing is happening.
-spec stream_cmd(string()) -> {integer(), string()}.
stream_cmd(Cmd) ->
    rt2:stream_cmd(Cmd).

%% @doc same as rt:stream_cmd/1, but with options, like open_port/2
-spec stream_cmd(string(), string()) -> {integer(), string()}.
stream_cmd(Cmd, Opts) ->
    rt2:stream_cmd(Cmd, Opts).

stream_cmd_loop(Port, Buffer, NewLineBuffer, Time={_MegaSecs, Secs, _MicroSecs}) ->
    receive
        {Port, {data, Data}} ->
            {_, Now, _} = now(),
            NewNewLineBuffer = case Now > Secs of
                true ->
                    lager:info(NewLineBuffer),
                    "";
                _ ->
                    NewLineBuffer
            end,
            case rt:str(Data, "\n") of
                true ->
                    lager:info(NewNewLineBuffer),
                    Tokens = string:tokens(Data, "\n"),
                    [ lager:info(Token) || Token <- Tokens ],
                    stream_cmd_loop(Port, Buffer ++ NewNewLineBuffer ++ Data, "", Time);
                _ ->
                    stream_cmd_loop(Port, Buffer, NewNewLineBuffer ++ Data, now())
            end;
        {Port, {exit_status, Status}} ->
            catch port_close(Port),
            {Status, Buffer}
    after rt:config(rt_max_receive_wait_time) ->
            {-1, Buffer}
    end.

%%%===================================================================
%%% Remote code management
%%%===================================================================
load_modules_on_nodes(Modules, Nodes) ->
    rt2:load_modules_on_nodes(Modules, Nodes).

%%%===================================================================
%%% Status / Wait Functions
%%%===================================================================

%% @doc Is the `Node' up according to net_adm:ping
is_pingable(Node) ->
    rt_node:is_pingable(Node).

is_mixed_cluster(Nodes) ->
    rt2:is_mixed_cluster(Nodes).

%% @private
is_ready(Node) ->
    rt_node:is_ready(Node).

%% @doc Utility function used to construct test predicates. Retries the
%%      function `Fun' until it returns `true', or until the maximum
%%      number of retries is reached. The retry limit is based on the
%%      provided `rt_max_receive_wait_time' and `rt_retry_delay' parameters in
%%      specified `riak_test' config file.
wait_until(Fun) when is_function(Fun) ->
    rt2:wait_until(Fun).

%% @doc Convenience wrapper for wait_until for the myriad functions that
%% take a node as single argument.
wait_until(Node, Fun) when is_atom(Node), is_function(Fun) ->
    wait_until(fun() -> Fun(Node) end).

%% @doc Retry `Fun' until it returns `Retry' times, waiting `Delay'
%% milliseconds between retries. This is our eventual consistency bread
%% and butter
wait_until(Fun, Retry, Delay) when Retry > 0 ->
    rt2:wait_until(Fun, Retry, Delay).

%% @doc Wait until the specified node is considered ready by `riak_core'.
%%      As of Riak 1.0, a node is ready if it is in the `valid' or `leaving'
%%      states. A ready node is guaranteed to have current preflist/ownership
%%      information.
wait_until_ready(Node) ->
    rt2:wait_until_ready(Node).

%% @doc Wait until status can be read from riak_kv_console
wait_until_status_ready(Node) ->
    rt2:wait_until_status_ready(Node).

%% @doc Given a list of nodes, wait until all nodes believe there are no
%% on-going or pending ownership transfers.
-spec wait_until_no_pending_changes([node()]) -> ok | fail.
wait_until_no_pending_changes(Nodes) ->
    rt2:wait_until_no_pending_changes(Nodes).

%% @doc Waits until no transfers are in-flight or pending, checked by
%% riak_core_status:transfers().
-spec wait_until_transfers_complete([node()]) -> ok | fail.
wait_until_transfers_complete(Nodes) ->
    rt2:wait_until_transfers_complete(Nodes).

wait_for_service(Node, Services) when is_list(Services) ->
    rt2:wait_for_service(Node, Services);
wait_for_service(Node, Service) ->
    wait_for_service(Node, [Service]).

wait_for_cluster_service(Nodes, Service) ->
    rt2:wait_for_cluster_service(Nodes, Service).

%% @doc Given a list of nodes, wait until all nodes are considered ready.
%%      See {@link wait_until_ready/1} for definition of ready.
wait_until_nodes_ready(Nodes) ->
    rt_node:wait_until_nodes_ready(Nodes).

%% @doc Wait until all nodes in the list `Nodes' believe each other to be
%%      members of the cluster.
wait_until_all_members(Nodes) ->
    wait_until_all_members(Nodes, Nodes).

%% @doc Wait until all nodes in the list `Nodes' believes all nodes in the
%%      list `Members' are members of the cluster.
wait_until_all_members(Nodes, ExpectedMembers) ->
    rt2:wait_until_all_members(Nodes, ExpectedMembers).

%% @doc Given a list of nodes, wait until all nodes believe the ring has
%%      converged (ie. `riak_core_ring:is_ready' returns `true').
wait_until_ring_converged(Nodes) ->
    rt2:wait_until_ring_converged(Nodes).

wait_until_legacy_ringready(Node) ->
    rt2:wait_until_legacy_ringready(Node).

%% @doc wait until each node in Nodes is disterl connected to each.
wait_until_connected(Nodes) ->
    rt2:wait_until_connected(Nodes).

%% @doc Wait until the specified node is pingable
wait_until_pingable(Node) ->
    rt2:wait_until_pingable(Node).

%% @doc Wait until the specified node is no longer pingable
wait_until_unpingable(Node) ->
    rt2:wait_until_unpingable(Node).

% Waits until a certain registered name pops up on the remote node.
wait_until_registered(Node, Name) ->
    rt2:wait_until_registered(Node, Name).

%% Waits until the cluster actually detects that it is partitioned.
wait_until_partitioned(P1, P2) ->
    rt2:wait_until_partitioned(P1, P2).

is_partitioned(Node, Peers) ->
    rt2:is_partitioned(Node, Peers).

% when you just can't wait
brutal_kill(Node) ->
    rt_node:brutal_kill(Node).

capability(Node, Capability) ->
    rt2:capability(Node, Capability).

capability(Node, Capability, Default) ->
    rt2:capability(Node, Capability, Default).

wait_until_capability(Node, Capability, Value) ->
    rt2:wait_until_capability(Node, Capability, Value).

wait_until_capability(Node, Capability, Value, Default) ->
    rt2:wait_until_capability(Node, Capability, Value, Default).

cap_equal(Val, Cap) ->
    rt2:cap_equal(Val, Cap).

wait_until_owners_according_to(Node, Nodes) ->
    rt_node:wait_until_owners_according_to(Node, Nodes).

wait_until_nodes_agree_about_ownership(Nodes) ->
    rt_node:wait_until_nodes_agree_about_ownership(Nodes).

%% AAE support
wait_until_aae_trees_built(Nodes) ->
    rt_aae:wait_until_app_trees_built(Nodes).

%%%===================================================================
%%% Ring Functions
%%%===================================================================

%% @doc Ensure that the specified node is a singleton node/cluster -- a node
%%      that owns 100% of the ring.
check_singleton_node(Node) ->
    rt_ring:check_singleton_node(Node).

% @doc Get list of partitions owned by node (primary).
partitions_for_node(Node) ->
    rt_ring:partitions_for_node(Node).

%% @doc Get the raw ring for `Node'.
get_ring(Node) ->
    rt_ring:get_ring(Node).

assert_nodes_agree_about_ownership(Nodes) ->
    rt_ring:assert_nodes_agree_about_ownership(Nodes).

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
owners_according_to(Node) ->
    rt_ring:owners_according_to(Node).

%% @doc Return a list of cluster members according to the ring retrieved from
%%      the specified node.
members_according_to(Node) ->
    rt_ring:members_according_to(Node).

%% @doc Return an appropriate ringsize for the node count passed
%%      in. 24 is the number of cores on the bigger intel machines, but this
%%      may be too large for the single-chip machines.
nearest_ringsize(Count) ->
    rt_ring:nearest_ringsize(Count).

nearest_ringsize(Count, Power) ->
    rt_ring:nearest_ringsize(Count, Power).

%% @doc Return the cluster status of `Member' according to the ring
%%      retrieved from `Node'.
status_of_according_to(Member, Node) ->
    rt_ring:status_of_according_to(Member, Node).

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
claimant_according_to(Node) ->
    rt_ring:claimant_according_to(Node).

%%%===================================================================
%%% Cluster Utility Functions
%%%===================================================================

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
    [?assertEqual([Node], owners_according_to(Node)) || Node <- Nodes],

    %% Join nodes
    [Node1|OtherNodes] = Nodes,
    case OtherNodes of
        [] ->
            %% no other nodes, nothing to join/plan/commit
            ok;
        _ ->
            %% ok do a staged join and then commit it, this eliminates the
            %% large amount of redundant handoff done in a sequential join
            [staged_join(Node, Node1) || Node <- OtherNodes],
            plan_and_commit(Node1),
            try_nodes_ready(Nodes, 3, 500)
    end,

    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),

    %% Ensure each node owns a portion of the ring
    wait_until_nodes_agree_about_ownership(Nodes),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)),
    ok.

-type products() :: riak | riak_ee | riak_cs | unknown.

-spec product(node()) -> products().
product(Node) ->
    Applications = rpc:call(Node, application, which_applications, []),

    HasRiakCS = proplists:is_defined(riak_cs, Applications),
    HasRiakEE = proplists:is_defined(riak_repl, Applications),
    HasRiak = proplists:is_defined(riak_kv, Applications),
    if HasRiakCS -> riak_cs;
       HasRiakEE -> riak_ee;
       HasRiak -> riak;
       true -> unknown
    end.
   
try_nodes_ready(Nodes, N, SleepMs) ->
    rt_cluster:try_nodes_ready(Nodes, N, SleepMs).

%% @doc Stop nodes and wipe out their data directories
clean_cluster(Nodes) when is_list(Nodes) ->
    [stop_and_wait(Node) || Node <- Nodes],
    clean_data_dir(Nodes).

clean_data_dir(Nodes) ->
    clean_data_dir(Nodes, "").

clean_data_dir(Nodes, SubDir) when not is_list(Nodes) ->
    clean_data_dir([Nodes], SubDir);
clean_data_dir(Nodes, SubDir) when is_list(Nodes) ->
    ?HARNESS:clean_data_dir(Nodes, SubDir).

%% @doc Shutdown every node, this is for after a test run is complete.
teardown() ->
    rt_cluster:teardown().

versions() ->
    rt_cluster:versions().

%%%===================================================================
%%% Basic Read/Write Functions
%%%===================================================================

systest_write(Node, Size) ->
    rt_systest:write(Node, Size).

systest_write(Node, Size, W) ->
    rt_systest:write(Node, Size, W).

systest_write(Node, Start, End, Bucket, W) ->
    rt_systest:write(Node, Start, End, Bucket, W).

%% @doc Write (End-Start)+1 objects to Node. Objects keys will be
%% `Start', `Start+1' ... `End', each encoded as a 32-bit binary
%% (`<<Key:32/integer>>'). Object values are the same as their keys.
%%
%% The return value of this function is a list of errors
%% encountered. If all writes were successful, return value is an
%% empty list. Each error has the form `{N :: integer(), Error :: term()}',
%% where N is the unencoded key of the object that failed to store.
systest_write(Node, Start, End, Bucket, W, CommonValBin) ->
    rt_systest:write(Node, Start, End, Bucket, W, CommonValBin).

systest_read(Node, Size) ->
    rt_systest:read(Node, Size, 2).

systest_read(Node, Size, R) ->
    rt_systest:read(Node, 1, Size, <<"systest">>, R).

systest_read(Node, Start, End, Bucket, R) ->
    rt_systest:read(Node, Start, End, Bucket, R, <<>>).

systest_read(Node, Start, End, Bucket, R, CommonValBin) ->
    rt_systest:read(Node, Start, End, Bucket, R, CommonValBin, false).

%% Read and verify the values of objects written with
%% `systest_write'. The `SquashSiblings' parameter exists to
%% optionally allow handling of siblings whose value and metadata are
%% identical except for the dot. This goal is to facilitate testing
%% with DVV enabled because siblings can be created internally by Riak
%% in cases where testing with DVV disabled would not. Such cases
%% include writes that happen during handoff when a vnode forwards
%% writes, but also performs them locally or when a put coordinator
%% fails to send an acknowledgment within the timeout window and
%% another put request is issued.
systest_read(Node, Start, End, Bucket, R, CommonValBin, SquashSiblings) ->
    rt_systest:read(Node, Start, End, Bucket, R, CommonValBin, SquashSiblings).

cap_subset(Val, Cap) when is_list(Cap) ->
    sets:is_subset(sets:from_list(Val), sets:from_list(Cap)).

wait_until_owners_according_to(Node, Nodes) ->
    SortedNodes = lists:usort(Nodes),
    F = fun(N) ->
        rt_ring:owners_according_to(N) =:= SortedNodes
    end,
    ?assertEqual(ok, wait_until(Node, F)),
    ok.

wait_until_nodes_agree_about_ownership(Nodes) ->
lager:info("Wait until nodes agree about ownership ~p", [Nodes]),
Results = [ wait_until_owners_according_to(Node, Nodes) || Node <- Nodes ],
?assert(lists:all(fun(X) -> ok =:= X end, Results)).

%%%===================================================================
%%% Ring Functions
%%%===================================================================


%%%===================================================================
%%% Cluster Utility Functions
%%%===================================================================

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
    [?assertEqual([Node], owners_according_to(Node)) || Node <- Nodes],

    %% Potential fix for BTA-116 and other similar "join before nodes ready" issues.
    %% TODO: Investigate if there is an actual race in Riak relating to cluster joins.
    [ok = wait_for_service(Node, riak_kv) || Node <- Nodes],

    %% Join nodes
    [Node1|OtherNodes] = Nodes,
    case OtherNodes of
        [] ->
            %% no other nodes, nothing to join/plan/commit
            ok;
        _ ->
            %% ok do a staged join and then commit it, this eliminates the
            %% large amount of redundant handoff done in a sequential join
            [staged_join(Node, Node1) || Node <- OtherNodes],
            plan_and_commit(Node1),
            try_nodes_ready(Nodes, 3, 500)
    end,

?assertEqual(ok, wait_until_nodes_ready(Nodes)),

%% Ensure each node owns a portion of the ring
wait_until_nodes_agree_about_ownership(Nodes),
?assertEqual(ok, wait_until_no_pending_changes(Nodes)),
ok.

-type products() :: riak | riak_ee | riak_cs | unknown.

-spec product(node()) -> products().
product(Node) ->
    Applications = rpc:call(Node, application, which_applications, []),

    HasRiakCS = proplists:is_defined(riak_cs, Applications),
    HasRiakEE = proplists:is_defined(riak_repl, Applications),
    HasRiak = proplists:is_defined(riak_kv, Applications),
    if HasRiakCS -> riak_cs;
       HasRiakEE -> riak_ee;
       HasRiak -> riak;
       true -> unknown
    end.

try_nodes_ready([Node1 | _Nodes], 0, _SleepMs) ->
lager:info("Nodes not ready after initial plan/commit, retrying"),
plan_and_commit(Node1);
try_nodes_ready(Nodes, N, SleepMs) ->
ReadyNodes = [Node || Node <- Nodes, is_ready(Node) =:= true],
case ReadyNodes of
Nodes ->
    ok;
_ ->
    timer:sleep(SleepMs),
    try_nodes_ready(Nodes, N-1, SleepMs)
end.

%% @doc Stop nodes and wipe out their data directories
clean_cluster(Nodes) when is_list(Nodes) ->
[stop_and_wait(Node) || Node <- Nodes],
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
%%[ rt:stop(Node) || Node <- nodes()],
%% Then do the more exhaustive harness thing, in case something was up
%% but not connected.
?HARNESS:teardown().

versions() ->
?HARNESS:versions().
%%%===================================================================
%%% Basic Read/Write Functions
%%%===================================================================

systest_write(Node, Size) ->
systest_write(Node, Size, 2).

systest_write(Node, Size, W) ->
systest_write(Node, 1, Size, <<"systest">>, W).

systest_write(Node, Start, End, Bucket, W) ->
systest_write(Node, Start, End, Bucket, W, <<>>).

%% @doc Write (End-Start)+1 objects to Node. Objects keys will be
%% `Start', `Start+1' ... `End', each encoded as a 32-bit binary
%% (`<<Key:32/integer>>'). Object values are the same as their keys.
%%
%% The return value of this function is a list of errors
%% encountered. If all writes were successful, return value is an
%% empty list. Each error has the form `{N :: integer(), Error :: term()}',
%% where N is the unencoded key of the object that failed to store.
systest_write(Node, Start, End, Bucket, W, CommonValBin)
when is_binary(CommonValBin) ->
rt:wait_for_service(Node, riak_kv),
{ok, C} = riak:client_connect(Node),
F = fun(N, Acc) ->
	Obj = riak_object:new(Bucket, <<N:32/integer>>,
			      <<N:32/integer, CommonValBin/binary>>),
	try C:put(Obj, W) of
	    ok ->
		Acc;
	    Other ->
		[{N, Other} | Acc]
	catch
	    What:Why ->
		[{N, {What, Why}} | Acc]
	end
end,
lists:foldl(F, [], lists:seq(Start, End)).

systest_read(Node, Size) ->
systest_read(Node, Size, 2).

systest_read(Node, Size, R) ->
systest_read(Node, 1, Size, <<"systest">>, R).

systest_read(Node, Start, End, Bucket, R) ->
systest_read(Node, Start, End, Bucket, R, <<>>).

systest_read(Node, Start, End, Bucket, R, CommonValBin)
when is_binary(CommonValBin) ->
systest_read(Node, Start, End, Bucket, R, CommonValBin, false).

%% Read and verify the values of objects written with
%% `systest_write'. The `SquashSiblings' parameter exists to
%% optionally allow handling of siblings whose value and metadata are
%% identical except for the dot. This goal is to facilitate testing
%% with DVV enabled because siblings can be created internally by Riak
%% in cases where testing with DVV disabled would not. Such cases
%% include writes that happen during handoff when a vnode forwards
%% writes, but also performs them locally or when a put coordinator
%% fails to send an acknowledgment within the timeout window and
%% another put request is issued.
systest_read(Node, Start, End, Bucket, R, CommonValBin, SquashSiblings)
when is_binary(CommonValBin) ->
rt:wait_for_service(Node, riak_kv),
{ok, C} = riak:client_connect(Node),
lists:foldl(systest_read_fold_fun(C, Bucket, R, CommonValBin, SquashSiblings),
	[],
	lists:seq(Start, End)).

systest_read_fold_fun(C, Bucket, R, CommonValBin, SquashSiblings) ->
fun(N, Acc) ->
    GetRes = C:get(Bucket, <<N:32/integer>>, R),
    Val = object_value(GetRes, SquashSiblings),
    update_acc(value_matches(Val, N, CommonValBin), Val, N, Acc)
end.

object_value({error, _}=Error, _) ->
Error;
object_value({ok, Obj}, SquashSiblings) ->
object_value(riak_object:value_count(Obj), Obj, SquashSiblings).

object_value(1, Obj, _SquashSiblings) ->
riak_object:get_value(Obj);
object_value(_ValueCount, Obj, false) ->
riak_object:get_value(Obj);
object_value(_ValueCount, Obj, true) ->
lager:debug("Siblings detected for ~p:~p", [riak_object:bucket(Obj), riak_object:key(Obj)]),
Contents = riak_object:get_contents(Obj),
case lists:foldl(fun sibling_compare/2, {true, undefined}, Contents) of
{true, {_, _, _, Value}} ->
    lager:debug("Siblings determined to be a single value"),
    Value;
{false, _} ->
    {error, siblings}
end.

sibling_compare({MetaData, Value}, {true, undefined}) ->
Dot = case dict:find(<<"dot">>, MetaData) of
      {ok, DotVal} ->
	  DotVal;
      error ->
	  {error, no_dot}
  end,
VTag = dict:fetch(<<"X-Riak-VTag">>, MetaData),
LastMod = dict:fetch(<<"X-Riak-Last-Modified">>, MetaData),
{true, {element(2, Dot), VTag, LastMod, Value}};
sibling_compare(_, {false, _}=InvalidMatch) ->
InvalidMatch;
sibling_compare({MetaData, Value}, {true, PreviousElements}) ->
Dot = case dict:find(<<"dot">>, MetaData) of
      {ok, DotVal} ->
	  DotVal;
      error ->
	  {error, no_dot}
  end,
VTag = dict:fetch(<<"X-Riak-VTag">>, MetaData),
LastMod = dict:fetch(<<"X-Riak-Last-Modified">>, MetaData),
ComparisonElements = {element(2, Dot), VTag, LastMod, Value},
{ComparisonElements =:= PreviousElements, ComparisonElements}.

value_matches(<<N:32/integer, CommonValBin/binary>>, N, CommonValBin) ->
true;
value_matches(_WrongVal, _N, _CommonValBin) ->
false.

update_acc(true, _, _, Acc) ->
Acc;
update_acc(false, {error, _}=Val, N, Acc) ->
[{N, Val} | Acc];
update_acc(false, Val, N, Acc) ->
[{N, {wrong_val, Val}} | Acc].

verify_systest_value(N, Acc, CommonValBin, Obj) ->
Values = riak_object:get_values(Obj),
Res = [begin
       case V of
	   <<N:32/integer, CommonValBin/binary>> ->
	       ok;
	   _WrongVal ->
	       wrong_val
       end
   end || V <- Values],
case lists:any(fun(X) -> X =:= ok end, Res) of
true ->
    Acc;
false ->
    [{N, {wrong_val, hd(Values)}} | Acc]
end.

% @doc Reads a single replica of a value. This issues a get command directly
% to the vnode handling the Nth primary partition of the object's preflist.
get_replica(Node, Bucket, Key, I, N) ->
    rt2:get_replica(Node, Bucket, Key, I, N).

%%%===================================================================

%% @doc PBC-based version of {@link systest_write/1}
pbc_systest_write(Node, Size) ->
    rt_pb:pbc_systest_write(Node, Size).

pbc_systest_write(Node, Size, W) ->
    rt_pb:pbc_systest_write(Node, Size, W).

pbc_systest_write(Node, Start, End, Bucket, W) ->
    rt_pb:pbc_systest_write(Node, Start, End, Bucket, W).

pbc_systest_read(Node, Size) ->
    rt_pb:pbc_systest_read(Node, Size).

pbc_systest_read(Node, Size, R) ->
    rt_pb:pbc_systest_read(Node, Size, R).

pbc_systest_read(Node, Start, End, Bucket, R) ->
    rt_pb:pbc_systest_read(Node, Start, End, Bucket, R).

%%%===================================================================
%%% PBC & HTTPC Functions
%%%===================================================================

%% @doc get me a protobuf client process and hold the mayo!
-spec pbc(node()) -> pid().
pbc(Node) ->
    pbc(Node, [{auto_reconnect, true}]).

%% GAP: rt_pb does not provide an analog to this function. -jsb
-spec pbc(node(), proplists:proplist()) -> pid().
pbc(Node, Options) ->
    rt:wait_for_service(Node, riak_kv),
    ConnInfo = proplists:get_value(Node, connection_info([Node])),
    {IP, PBPort} = proplists:get_value(pb, ConnInfo),
    {ok, Pid} = riakc_pb_socket:start_link(IP, PBPort, Options),
    Pid.

%% @doc does a read via the erlang protobuf client
-spec pbc_read(pid(), binary(), binary()) -> binary().
pbc_read(Pid, Bucket, Key) ->
    pbc_read(Pid, Bucket, Key, []).

-spec pbc_read(pid(), binary(), binary(), [any()]) -> binary().
pbc_read(Pid, Bucket, Key, Options) ->
    rt_pb:pbc_read(Pid, Bucket, Key, Options).

-spec pbc_read_check(pid(), binary(), binary(), [any()]) -> boolean().
pbc_read_check(Pid, Bucket, Key, Allowed) ->
    pbc_read_check(Pid, Bucket, Key, Allowed, []).

-spec pbc_read_check(pid(), binary(), binary(), [any()], [any()]) -> boolean().
pbc_read_check(Pid, Bucket, Key, Allowed, Options) ->
    rt_pb:pbc_read_check(Pid, Bucket, Key, Allowed, Options).

%% @doc does a write via the erlang protobuf client
-spec pbc_write(pid(), binary(), binary(), binary()) -> atom().
pbc_write(Pid, Bucket, Key, Value) ->
    rt_pb:pbc_write(Pid, Bucket, Key, Value).

%% @doc does a write via the erlang protobuf client plus content-type
-spec pbc_write(pid(), binary(), binary(), binary(), list()) -> atom().
pbc_write(Pid, Bucket, Key, Value, CT) ->
    rt_pb:pbc_write(Pid, Bucket, Key, Value, CT).

%% @doc sets a bucket property/properties via the erlang protobuf client
-spec pbc_set_bucket_prop(pid(), binary(), [proplists:property()]) -> atom().
pbc_set_bucket_prop(Pid, Bucket, PropList) ->
    rt_pb:pbc_set_bucket_prop(Pid, Bucket, PropList).

%% @doc Puts the contents of the given file into the given bucket using the
%% filename as a key and assuming a plain text content type.
pbc_put_file(Pid, Bucket, Key, Filename) ->
    rt_pb:pbc_put_file(Pid, Bucket, Key, Filename).

%% @doc Puts all files in the given directory into the given bucket using the
%% filename as a key and assuming a plain text content type.
pbc_put_dir(Pid, Bucket, Dir) ->
    rt_pb:pbc_put_dir(Pid, Bucket, Dir).

%% @doc True if the given keys have been really, really deleted.
%% Useful when you care about the keys not being there. Delete simply writes
%% tombstones under the given keys, so those are still seen by key folding
%% operations.
pbc_really_deleted(Pid, Bucket, Keys) ->
    rt_pb:pbc_really_deleted(Pid, Bucket, Keys).

%% @doc Returns HTTPS URL information for a list of Nodes
https_url(Nodes) ->
    rt_http:https_url(Nodes).

%% @doc Returns HTTP URL information for a list of Nodes
http_url(Nodes) ->
    rt_http:http_url(Nodes).

%% @doc get me an http client.
-spec httpc(node()) -> term().
httpc(Node) ->
    rt_http:httpc(Node).

%% @doc does a read via the http erlang client.
-spec httpc_read(term(), binary(), binary()) -> binary().
httpc_read(C, Bucket, Key) ->
    rt_http:httpc_read(C, Bucket, Key).

%% @doc does a write via the http erlang client.
-spec httpc_write(term(), binary(), binary(), binary()) -> atom().
httpc_write(C, Bucket, Key, Value) ->
    rt_http:httpc_write(C, Bucket, Key, Value).

%%%===================================================================
%%% Command Line Functions
%%%===================================================================

%% @doc Call 'bin/riak-admin' command on `Node' with arguments `Args'
admin(Node, Args) ->
    admin(Node, Args, []).

%% @doc Call 'bin/riak-admin' command on `Node' with arguments `Args'.
%% The third parameter is a list of options. Valid options are:
%%    * `return_exit_code' - Return the exit code along with the command output
admin(Node, Args, Options) ->
    rt_cmd_line:admin(Node, Args, Options).

%% @doc Call 'bin/riak' command on `Node' with arguments `Args'
riak(Node, Args) ->
    rt_cmd_line:riak(Node, Args).

%% @doc Call 'bin/riak-repl' command on `Node' with arguments `Args'
riak_repl(Node, Args) ->
    rt_cmd_line:riak_repl(Node, Args).

search_cmd(Node, Args) ->
    rt_cmd_line:search_cmd(Node, Args).

%% @doc Runs `riak attach' on a specific node, and tests for the expected behavoir.
%%      Here's an example: ```
%%      rt:attach(Node, [{expect, "erlang.pipe.1 \(^D to exit\)"},
%%                       {send, "riak_core_ring_manager:get_my_ring()."},
%%                       {expect, "dict,"},
%%                       {send, [4]}]), %% 4 = Ctrl + D'''
%%      `{expect, String}' scans the output for the existance of the String.
%%         These tuples are processed in order.
%%
%%      `{send, String}' sends the string to the console.
%%         Once a send is encountered, the buffer is discarded, and the next
%%         expect will process based on the output following the sent data.
%%
attach(Node, Expected) ->
    rt_cmd_line:attach(Node, Expected).

%% @doc Runs 'riak attach-direct' on a specific node
%% @see rt:attach/2
attach_direct(Node, Expected) ->
    rt_cmd_line:attach_direct(Node, Expected).

%% @doc Runs `riak console' on a specific node
%% @see rt:attach/2
console(Node, Expected) ->
    rt_cmd_line:console(Node, Expected).

%%%===================================================================
%%% Search
%%%===================================================================

%% doc Enable the search KV hook for the given `Bucket'.  Any `Node'
%%     in the cluster may be used as the change is propagated via the
%%     Ring.
enable_search_hook(Node, Bucket) ->
    rt2:enable_search_hook(Node, Bucket).

%%%===================================================================
%%% Test harness setup, configuration, and internal utilities
%%%===================================================================

%% @doc Sets the backend of ALL nodes that could be available to riak_test.
%%      this is not limited to the nodes under test, but any node that
%%      riak_test is able to find. It then queries each available node
%%      for it's backend, and returns it if they're all equal. If different
%%      nodes have different backends, it returns a list of backends.
%%      Currently, there is no way to request multiple backends, so the
%%      list return type should be considered an error.
-spec set_backend(atom()) -> atom()|[atom()].
set_backend(Backend) ->
    set_backend(Backend, []).

-spec set_backend(atom(), [{atom(), term()}]) -> atom()|[atom()].
set_backend(bitcask, _) ->
    set_backend(riak_kv_bitcask_backend);
set_backend(eleveldb, _) ->
    set_backend(riak_kv_eleveldb_backend);
set_backend(memory, _) ->
    set_backend(riak_kv_memory_backend);
set_backend(multi, Extras) ->
    set_backend(riak_kv_multi_backend, Extras);
set_backend(Backend, _) when Backend == riak_kv_bitcask_backend; Backend == riak_kv_eleveldb_backend; Backend == riak_kv_memory_backend ->
    lager:info("rt:set_backend(~p)", [Backend]),
    update_app_config(all, [{riak_kv, [{storage_backend, Backend}]}]),
    get_backends();
set_backend(Backend, Extras) when Backend == riak_kv_multi_backend ->
    MultiConfig = proplists:get_value(multi_config, Extras, default),
    Config = make_multi_backend_config(MultiConfig),
    update_app_config(all, [{riak_kv, Config}]),
    get_backends();
set_backend(Other, _) ->
    lager:warning("rt:set_backend doesn't recognize ~p as a legit backend, using the default.", [Other]),
    get_backends().

make_multi_backend_config(default) ->
    [{storage_backend, riak_kv_multi_backend},
     {multi_backend_default, <<"eleveldb1">>},
     {multi_backend, [{<<"eleveldb1">>, riak_kv_eleveldb_backend, []},
                      {<<"memory1">>, riak_kv_memory_backend, []},
                      {<<"bitcask1">>, riak_kv_bitcask_backend, []}]}];
make_multi_backend_config(indexmix) ->
    [{storage_backend, riak_kv_multi_backend},
     {multi_backend_default, <<"eleveldb1">>},
     {multi_backend, [{<<"eleveldb1">>, riak_kv_eleveldb_backend, []},
                      {<<"memory1">>, riak_kv_memory_backend, []}]}];
make_multi_backend_config(Other) ->
    lager:warning("rt:set_multi_backend doesn't recognize ~p as legit multi-backend config, using default", [Other]),
    make_multi_backend_config(default).

get_backends() ->
    Backends = ?HARNESS:get_backends(),
    case Backends of
        [riak_kv_bitcask_backend] -> bitcask;
        [riak_kv_eleveldb_backend] -> eleveldb;
        [riak_kv_memory_backend] -> memory;
        [Other] -> Other;
        MoreThanOne -> MoreThanOne
    end.

-spec get_backend([proplists:property()]) -> atom() | error.
get_backend(AppConfigProplist) ->
    case kvc:path('riak_kv.storage_backend', AppConfigProplist) of
        [] -> error;
        Backend -> Backend
    end.

%% @doc Gets the current version under test. In the case of an upgrade test
%%      or something like that, it's the version you're upgrading to.
-spec get_version() -> binary().
get_version() ->
    rt2:get_version().

-spec get_version(string()) -> binary().
get_version(Node) ->
    rt_harness:get_version(Node).

%% @doc outputs some useful information about nodes that are up
whats_up() ->
    rt2:whats_up().

-spec get_ip(node()) -> string().
get_ip(Node) ->
    rt2:get_ip(Node).

%% @doc Log a message to the console of the specified test nodes.
%%      Messages are prefixed by the string "---riak_test--- "
%%      Uses lager:info/1 'Fmt' semantics
log_to_nodes(Nodes, Fmt) ->
    log_to_nodes(Nodes, Fmt, []).

%% @doc Log a message to the console of the specified test nodes.
%%      Messages are prefixed by the string "---riak_test--- "
%%      Uses lager:info/2 'LFmt' and 'LArgs' semantics
log_to_nodes(Nodes0, LFmt, LArgs) ->
    rt2:log_to_nodes(Nodes0, LFmt, LArgs).

pmap(F, L) ->
    rt2:pmap(F, L).

setup_harness(Test, Args) ->
    rt2:setup_harness(Test, Args).

%% @doc Downloads any extant log files from the harness's running
%%   nodes.
get_node_logs() ->
    rt2:get_node_logs().

check_ibrowse() ->
    rt2:check_ibrowse().

post_result(TestResult, Webhook) ->
    rt2:post_result(TestResult, Webhook).

%%%===================================================================
%%% Bucket Types Functions
%%%===================================================================

%% @doc create and immediately activate a bucket type
create_and_activate_bucket_type(Node, Type, Props) ->
    rt_bucket_type:create_and_activate_bucket_type(Node, Type, Props).

wait_until_bucket_type_status(Type, ExpectedStatus, Nodes) ->
    rt_bucket_type:wait_until_bucket_type_status(Type, ExpectedStatus, Nodes).

wait_until_bucket_type_visible(Nodes, Type) ->
    rt_bucket_types:wait_until_bucket_type_visble(Nodes, Type).

wait_until_bucket_props(Nodes, Bucket, Props) ->
    rt_bucket_types:wait_until_bucket_props(Nodes, Bucket, Props).


%% @doc Set up in memory log capture to check contents in a test.
setup_log_capture(Nodes) ->
    rt2:setup_log_capture(Nodes).

expect_in_log(Node, Pattern) ->
    rt2:expect_in_log(Node, Pattern).

%% @doc Wait for Riak Control to start on a single node.
%%
%% Non-optimal check, because we're blocking for the gen_server to start
%% to ensure that the routes have been added by the supervisor.
%%
wait_for_control(Vsn, Node) ->
    rt2:wait_for_control(Vsn, Node).

%% @doc Wait for Riak Control to start on a series of nodes.
wait_for_control(VersionedNodes) ->
    rt2:wait_for_control(VersionedNodes).

-ifdef(TEST).

verify_product(Applications, ExpectedApplication) ->
    ?_test(begin
               meck:new(rpc, [unstick]),
               meck:expect(rpc, call, fun([], application, which_applications, []) ->
                                           Applications end),
               ?assertMatch(ExpectedApplication, product([])),
               meck:unload(rpc)
           end).

product_test_() ->
    {foreach,
     fun() -> ok end,
     [verify_product([riak_cs], riak_cs),
      verify_product([riak_repl, riak_kv, riak_cs], riak_cs),
      verify_product([riak_repl], riak_ee),
      verify_product([riak_repl, riak_kv], riak_ee),
      verify_product([riak_kv], riak),
      verify_product([kernel], unknown)]}.

-endif.
