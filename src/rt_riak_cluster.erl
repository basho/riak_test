%%%-------------------------------------------------------------------
%%% @author John Burwell <>
%%% @copyright (C) 2015, John Burwell
%%% @doc
%%%
%%% @end
%%% Created : 19 Mar 2015 by John Burwell <>
%%%-------------------------------------------------------------------
-module(rt_riak_cluster).

-behaviour(gen_fsm).

%% API
-export([activate_bucket_type/2,
         clean/1,
         create_and_activate_bucket_type/3,
         create_bucket_type/3,
         is_mixed/1,
         join/2,
         leave/2,
         name/1,
         nodes/1,
         partition/3,
         start/1,
         start_link/2,
         stop/1,
         wait_until_all_members/1,
         wait_until_connected/1,
         wait_until_legacy_ring_ready/1,
         wait_until_no_pending_changes/1,
         wait_until_nodes_agree_about_ownership/1,
         wait_until_ring_converged/1,
         version/1,
         upgrade/2]).

%% gen_fsm callbacks
-export([init/1, state_name/2, state_name/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-type node_attribute() :: {name, atom()} | {version, rt_util:version()} |
                          {hostname, rt_host:hostname()} | {config, term()} |
                          {type, proplists:proplist()}.
-type node_definition() :: [node_attribute()].

-record(state, {name :: cluster_name(),
                bucket_types :: [term()],   %% should this be a string?
                indexes :: [atom()],        %% should this be a string?
                nodes :: [node()],
                version :: rt_util:version(),
                config :: term(),
                is_mixed=false :: boolean()}).

%%%===================================================================
%%% API
%%%===================================================================
-type cluster_name() :: atom().
-type cluster_id() :: pid().
%% -type partition() :: [node()].

-spec activate_bucket_type(cluster_id(), atom()) -> rt_util:result().
activate_bucket_type(Cluster, BucketType) ->
    gen_fsm:sync_send_event(Cluster, {activate_bucket_type, BucketType}).

%% @doc Stops cluster, `Cluster', removes all data, and restarts it
-spec clean(cluster_id()) -> rt_util:result().
clean(Cluster) ->
    gen_fsm:sync_send_event(Cluster, clean).

%% @doc Creates and activates a bucket type, `BucketType', on cluster, `Cluster', using properties, `Properties'
-spec create_and_activate_bucket_type(cluster_id(), atom(), proplists:proplist()) -> rt_util:result().
create_and_activate_bucket_type(Cluster, BucketType, Properties) ->
    ok = create_bucket_type(Cluster, BucketType, Properties),
    activate_bucket_type(Cluster, BucketType).

%% @doc Creates a bucket type, `BucketType', on cluster, `Cluster', with properties, `Properties'
-spec create_bucket_type(cluster_id(), atom(), proplists:proplist()) -> rt_util:result().
create_bucket_type(Cluster, BucketType, Properties) ->
    gen_fsm:create_bucket_type(Cluster, {create_bucket_type, BucketType, Properties}).

-spec is_mixed(cluster_id()) -> boolean().
is_mixed(Cluster) ->
    gen_fsm:sync_send_all_state_event(Cluster, is_mixed).

%% @doc Joins a node, `Node', to the cluster, `Cluster'
-spec join(cluster_id(), node()) -> rt_util:result().
join(Cluster, Node) ->
    gen_fsm:sync_send_event(Cluster, {join, Node}).

-spec leave(cluster_id(), node()) -> rt_util:result().
leave(Cluster, Node) ->
    gen_fsm:sync_send_event(Cluster, {leave, Node}).

-spec name(cluster_id()) -> atom().
name(Cluster) ->
    gen_fsm:sync_send_all_state_event(Cluster, name).

%% @doc Returns the list of nodes in the cluster
-spec nodes(cluster_id()) -> [node()].
nodes(Cluster) ->
    gen_fsm:sync_send_all_state_event(Cluster, nodes).

%% -spec partition(cluster_id(), partition(), partition()) -> [atom(), atom(), partition(), partition()] | rt_util:error().
partition(Cluster, P1, P2) ->
    gen_fsm:sync_send_event(Cluster, {partition, P1, P2}).

%% @doc Starts each node in the cluster
-spec start(cluster_id()) -> rt_util:result().
start(Cluster) ->
    gen_fsm:sync_send_event(Cluster, start).

%% @doc Stops each node in the cluster
-spec stop(cluster_id()) -> rt_util:result().
stop(Cluster) ->
    gen_fsm:sync_send_event(Cluster, stop).

-spec wait_until_all_members(cluster_id()) -> rt_util:result().
wait_until_all_members(Cluster) ->
    gen_fsm:sync_send_event(Cluster, wait_unit_all_members).

-spec wait_until_connected(cluster_id()) -> rt_util:result().
wait_until_connected(Cluster) ->
    gen_fsm:sync_send_event(Cluster, wait_until_connected).

-spec wait_until_legacy_ring_ready(cluster_id()) -> rt_util:result().
wait_until_legacy_ring_ready(Cluster) ->
    gen_fsm:sync_send_event(Cluster, wait_until_legacy_ring).

-spec wait_until_no_pending_changes(cluster_id()) -> rt_util:result().
wait_until_no_pending_changes(Cluster) ->
    gen_fsm:sync_send_event(Cluster, wait_until_no_pending_changes).

-spec wait_until_nodes_agree_about_ownership(cluster_id()) -> rt_util:result().
wait_until_nodes_agree_about_ownership(Cluster) ->
    gen_fsm:sync_send_event(Cluster, wait_until_nodes_agree_about_ownership).

-spec wait_until_ring_converged(cluster_id()) -> rt_util:result().
wait_until_ring_converged(Cluster) ->
    gen_fsm:sync_send_event(Cluster, wait_until_ring_converged).

%% @doc Returns the version of the cluster, `Cluster'
-spec version(cluster_id()) -> rt_util:version().
version(Cluster) ->
    gen_fsm:sync_send_all_state_event(Cluster, version).

%% @doc Performs a rolling upgrade of the cluster, `Cluster', to version, `NewVersion'.
-spec upgrade(cluster_id(), rt_util:version()) -> rt_util:result().
upgrade(Cluster, NewVersion) ->
    gen_fsm:sync_send_event(Cluster, {upgrade, NewVersion}).

%% index creation ...

%% security: users/acls, etc

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
%%-spec start_link([cluster_name(), [{node(), rt_util:version()}], term()]) -> {ok, pid()} | ignore | rt_util:error().
-spec start_link(cluster_name(), [node_definition()]) -> {ok, pid()} | ignore | rt_util:error().
start_link(Name, NodeDefinitions) ->
    gen_fsm:start_link(?MODULE, [Name, NodeDefinitions], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([Name, NodeDefinitions]) ->
    %% TODO Calculate the mixed flag
    Nodes = lists:foldl(fun(NodeDefinition, Nodes) ->
                                Hostname = proplists:get_value(hostname, NodeDefinition),
                                Type = proplists:get_value(type, NodeDefinition),
                                Config = proplists:get_value(config, NodeDefinition),
                                Version = proplists:get_value(version, NodeDefinition),

                                {ok, NodeName} = rt_riak_node:start_link(Hostname, Type, 
                                                                         Config, Version),
                                                                         
                                lists:apped(NodeName, Nodes)
                        end, [], NodeDefinitions),
                      
    {ok, allocated, #state{name=Name, nodes=Nodes}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
state_name(_Event, State) ->
    {next_state, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
state_name(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
