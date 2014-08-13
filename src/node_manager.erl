-module(node_manager).

-behavior(gen_server).

%% API
-export([start_link/3,
         reserve_nodes/3,
         deploy_nodes/5,
         upgrade_nodes/5,
         return_nodes/1,
         status/0,
         stop/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {nodes :: [string()],
                node_map :: [{string(), node()}],
                nodes_available :: [string()],
                nodes_deployed=[] :: [string()],
                deployed_versions=[] :: [{string(), string()}],
                version_map :: [{string(), [string()]}]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Nodes, NodeMap, VersionMap) ->
    Args = [Nodes, NodeMap, VersionMap],
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec reserve_nodes(pos_integer(), [string()], function()) -> ok.
reserve_nodes(NodeCount, Versions, NotifyFun) ->
    gen_server:cast(?MODULE, {reserve_nodes, NodeCount, Versions, NotifyFun}).

-spec deploy_nodes([string()], string(), term(), list(atom()), function()) -> ok.
deploy_nodes(Nodes, Version, Config, Services, NotifyFun) ->
    gen_server:cast(?MODULE, {deploy_nodes, Nodes, Version, Config, Services, NotifyFun}).

-spec upgrade_nodes([string()], string(), string(), term(), function()) -> ok.
upgrade_nodes(Nodes, CurrentVersion, NewVersion, Config, NotifyFun) ->
    gen_server:cast(?MODULE, {deploy_nodes, Nodes, CurrentVersion, NewVersion, Config, NotifyFun}).

-spec return_nodes([string()]) -> ok.
return_nodes(Nodes) ->
    gen_server:cast(?MODULE, {return_nodes, Nodes}).

-spec status() -> [{atom(), list()}].
status() ->
    gen_server:call(?MODULE, status, infinity).

-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Nodes, NodeMap, VersionMap]) ->
    SortedNodes = lists:sort(Nodes),
    {ok, #state{nodes=SortedNodes,
                node_map=NodeMap,
                nodes_available=SortedNodes,
                version_map=VersionMap}}.

handle_call(status, _From, State) ->
    Status = [{nodes, State#state.nodes},
              {nodes_available, State#state.nodes_available},
              {version_map, State#state.version_map}],
    {reply, Status, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({reserve_nodes, Count, Versions, NotifyFun}, State) ->
    {Result, UpdState} =
        reserve(Count, Versions, State),
    NotifyFun({nodes, Result, State#state.node_map}),
    {noreply, UpdState};
handle_cast({deploy_nodes, Nodes, Version, Config, Services, NotifyFun}, State) ->
    Result = deploy(Nodes, State#state.node_map, Version, Config, Services),
    DeployedVersions = State#state.deployed_versions,
    UpdDeployedVersions = update_deployed_versions(deploy, Nodes, Version, DeployedVersions),
    NotifyFun({nodes_deployed, Result}),
    {noreply, State#state{deployed_versions=UpdDeployedVersions}};
handle_cast({upgrade_nodes, Nodes, CurrentVersion, NewVersion, Config, NotifyFun}, State) ->
    Result = upgrade(Nodes, CurrentVersion, NewVersion, Config),
    DeployedVersions = State#state.deployed_versions,
    UpdDeployedVersions = update_deployed_versions(upgrade, Nodes, NewVersion, DeployedVersions),
    NotifyFun({nodes_upgraded, Result}),
    {noreply, State#state{deployed_versions=UpdDeployedVersions}};
handle_cast({return_nodes, Nodes}, State) ->
    %% Stop nodes and clean data dirs so they are ready for next use.
    NodesAvailable = State#state.nodes_available,
    DeployedVersions = State#state.deployed_versions,
    NodeMap = State#state.node_map,
    stop_and_clean(Nodes, NodeMap, DeployedVersions, false),
    NodesNowAvailable = lists:merge(lists:sort(Nodes), NodesAvailable),

    UpdDeployedVersions = update_deployed_versions(return, Nodes, undefined, DeployedVersions),
    {noreply, State#state{nodes_available=NodesNowAvailable,
                          deployed_versions=UpdDeployedVersions}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Stop and reset all deployed nodes
    stop_and_clean(State#state.nodes_deployed,
                   State#state.node_map,
                   State#state.deployed_versions,
                   true),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

stop_and_clean(NodeIds, NodeMap, DeployedVersions, Wait) ->
    [begin
         case version_deployed(NodeId, DeployedVersions) of
             undefined ->
                 ok;
             Version ->
                 rt_node:stop_and_wait(NodeId,
                                       rt_node:node_name(NodeId, NodeMap),
                                       Version),
                 wait_for_cleaner(rt_node:clean_data_dir(NodeId, Version), Wait)
         end
     end || NodeId <- NodeIds].

wait_for_cleaner(Pid, true) ->
    WaitFun =
        fun() ->
                not is_process_alive(Pid)
        end,
    rt:wait_until(WaitFun);
wait_for_cleaner(_, false) ->
    ok.

reserve(Count, _Versions, State=#state{nodes_available=NodesAvailable})
  when Count > length(NodesAvailable) ->
    {not_enough_nodes, State};
reserve(Count, Versions, State=#state{nodes_available=NodesAvailable,
                                      nodes_deployed=NodesDeployed,
                                      version_map=VersionMap})
  when Count =:= length(NodesAvailable) ->
    case versions_available(Count, Versions, VersionMap) of
        true ->
            UpdNodesDeployed = lists:sort(NodesDeployed ++ NodesAvailable),
            {NodesAvailable, State#state{nodes_available=[],
                                         nodes_deployed=UpdNodesDeployed}};
        false ->
            {insufficient_versions_available, State}
    end;
reserve(Count, Versions, State=#state{nodes_available=NodesAvailable,
                                      nodes_deployed=NodesDeployed,
                                      version_map=VersionMap}) ->
    case versions_available(Count, Versions, VersionMap) of
        true ->
            {Reserved, UpdNodesAvailable} = lists:split(Count, NodesAvailable),
            UpdNodesDeployed = lists:sort(NodesDeployed ++ Reserved),
            UpdState = State#state{nodes_available=UpdNodesAvailable,
                                   nodes_deployed=UpdNodesDeployed},
            {Reserved, UpdState};
        false ->
            {insufficient_versions_available, State}
    end.

versions_available(Count, Versions, VersionMap) ->
    lists:all(version_available_fun(Count, VersionMap), Versions).

version_available_fun(Count, VersionMap) ->
    fun(Version) ->
            case lists:keyfind(Version, 1, VersionMap) of
                {Version, VersionNodes} when length(VersionNodes) >= Count ->
                    true;
                {Version, _} ->
                    false;
                false ->
                    false
            end
    end.

deploy(Nodes, NodeMap, Version, Config, Services) ->
    rt_harness_util:deploy_nodes(Nodes, NodeMap, Version, Config, Services).

upgrade(Nodes, CurrentVersion, NewVersion, Config) ->
    [rt_node:upgrade(Node, CurrentVersion, NewVersion, Config) ||
        Node <- Nodes].

update_deployed_versions(deploy, Nodes, Version, DeployedVersions) ->
    {_, UpdDeployedVersions} = lists:foldl(fun add_deployed_version/2,
                                           {Version, DeployedVersions},
                                           Nodes),
    UpdDeployedVersions;
update_deployed_versions(upgrade, Nodes, Version, DeployedVersions) ->
    {_, UpdDeployedVersions} = lists:foldl(fun replace_deployed_version/2,
                                           {Version, DeployedVersions},
                                           Nodes),
    UpdDeployedVersions;
update_deployed_versions(return, Nodes, _, DeployedVersions) ->
    lists:foldl(fun remove_deployed_version/2, DeployedVersions, Nodes).

add_deployed_version(Node, {Version, DeployedVersions}) ->
    {Version, [{Node, Version} | DeployedVersions]}.

replace_deployed_version(Node, {Version, DeployedVersions}) ->
    {Version, lists:keyreplace(Node, 1, DeployedVersions, {Node, Version})}.

remove_deployed_version(Node, DeployedVersions) ->
    lists:keydelete(Node, 1, DeployedVersions).

version_deployed(Node, DeployedVersions) ->
    case lists:keyfind(Node, 1, DeployedVersions) of
        {Node, Version} ->
            Version;
        false ->
            undefined
    end.
