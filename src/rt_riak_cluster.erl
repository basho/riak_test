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
         decommission/1,
         clean/1,
         create_and_activate_bucket_type/3,
         create_bucket_type/3,
         is_mixed/1,
         join/2,
         leave/2,
         nodes/1,
         partition/3,
         provision/4,
         staged_join/2,
         start/1,
         stop/1,
         wait_until_all_members/1,
         wait_until_connected/1,
         wait_until_legacy_ring_ready/1,
         wait_until_no_pending_changes/1,
         wait_until_nodes_agree_about_ownership/1,
         wait_until_ring_converged/1,
         upgrade/2]).

%% gen_fsm callbacks
-export([init/1, state_name/2, state_name/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {name :: cluster_name(),
                bucket_types :: [atom()],   %% should this be a string?
                indexes :: [atom()],        %% should this be a string?
                nodes :: [node()],
                version :: string()
         }).

%%%===================================================================
%%% API
%%%===================================================================
-type cluster_name() :: atom().
-type cluster_id() :: pid().
%% -type partition() :: [node()].

%% TODO Move to the rt_version module
%% TODO Split out the product type
-type version() :: {atom(), string()}.

-spec activate_bucket_type(cluster_id(), atom()) -> rt_util:result().
activate_bucket_type(Cluster, BucketType) ->
    lager:error("activate_bucket_type(~p, ~p, ~p) not implemented", [Cluster, BucketType]), 
    ok.

-spec decommission(cluster_id()) -> rt_util:result().
decommission(Cluster) ->
    lager:error("decomission(~p) not implemented", [Cluster]), 
    ok.

-spec clean(cluster_id()) -> rt_util:result().
clean(Cluster) ->
    lager:error("clean(~p) not implemented", [Cluster]), 
    ok.

-spec create_and_activate_bucket_type(cluster_id(), atom(), proplists:proplist()) -> rt_util:result().
create_and_activate_bucket_type(Cluster, BucketType, Parameters) ->
    ok = create_bucket_type(Cluster, BucketType, Parameters),
    activate_bucket_type(Cluster, BucketType).

-spec create_bucket_type(cluster_id(), atom(), proplists:proplist()) -> rt_util:result().
create_bucket_type(Cluster, BucketType, Parameters) ->
    lager:error("create_bucket_type(~p, ~p, ~p) not implemented", [Cluster, BucketType, Parameters]), 
    ok.

-spec is_mixed(cluster_id()) -> boolean().
is_mixed(Cluster) ->
    lager:error("is_mixed(~p) not implemented", [Cluster]), 
    ok.

-spec join(cluster_id(), node()) -> rt_util:result().
join(Cluster, Node) ->
    lager:error("join(~p, ~p) not implemented.", [Cluster, Node]),
    ok.

-spec leave(cluster_id(), node()) -> rt_util:result().
leave(Cluster, Node) ->
    lager:error("leave(~p, ~p) not implemented.", [Cluster, Node]),
    ok.

-spec nodes(cluster_id()) -> [node()].
nodes(Cluster) ->
    lager:error("nodes(~p) not implemented", [Cluster]), 
    ok.

%% -spec partition(cluster_id(), partition(), partition()) -> [atom(), atom(), partition(), partition()] | rt_util:error().
partition(Cluster, P1, P2) -> 
    lager:error("partition(~p, ~p, ~p) not implemented.", [Cluster, P1, P2]),
    ok.

-spec provision(cluster_name(), [node()], proplists:proplist(), proplists:proplist()) -> {cluster_name(), cluster_id()} | rt_util:error().
provision(Name, Nodes, Conf, AdvancedConfig) ->
    lager:error("provision(~p, ~p, ~p, ~p) not implemented.", [Name, Nodes, Conf, AdvancedConfig]),
    ok.

-spec staged_join(cluster_id(), node()) -> rt_util:result().
staged_join(Cluster, Node) ->
    lager:error("stagad_join(~p, ~p) not implemented.", [Cluster, Node]),
    ok.

-spec start(cluster_id()) -> rt_util:result().
start(Cluster) ->
    lager:error("start(~p) not implemented", [Cluster]), 
    ok.

-spec stop(cluster_id()) -> rt_util:result().
stop(Cluster) ->
    lager:error("stop(~p) not implemented", [Cluster]), 
    ok.

-spec wait_until_all_members([node()]) -> rt_util:result().
wait_until_all_members(Nodes) ->
    lager:error("wait_until_all_members(~p) not implemented", [Nodes]), 
    ok.

-spec wait_until_connected(cluster_id()) -> rt_util:result().
wait_until_connected(Cluster) ->
    lager:error("wait_until_connected(~p) not implemented", [Cluster]), 
    ok.

-spec wait_until_legacy_ring_ready(cluster_id()) -> rt_util:result().
wait_until_legacy_ring_ready(Cluster) ->
    lager:error("wait_until_legacy_ring_ready(~p) not implemented", [Cluster]), 
    ok.

-spec wait_until_no_pending_changes(cluster_id()) -> rt_util:result().
wait_until_no_pending_changes(Cluster) ->
    lager:error("wait_until_no_pending_changes(~p) not implemented", [Cluster]), 
    ok.

-spec wait_until_nodes_agree_about_ownership(cluster_id()) -> rt_util:result().
wait_until_nodes_agree_about_ownership(Cluster) ->
    lager:error("wait_until_nodes_agree_about_ownership(~p) not implemented", [Cluster]), 
    ok.

-spec wait_until_ring_converged(cluster_id()) -> rt_util:result().
wait_until_ring_converged(Cluster) ->
    lager:error("wait_until_ring_converged(~p) not implemented", [Cluster]), 
    ok.

-spec upgrade(cluster_id(), version()) -> rt_util:result().
upgrade(Cluster, Version) ->
    lager:error("upgrade(~p, ~p) not implemented.", [Cluster, Version]),
    ok.

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
%%start_link() ->
%%    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

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
init([]) ->
    {ok, state_name, #state{}}.

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
