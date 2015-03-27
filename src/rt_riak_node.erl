%%%-------------------------------------------------------------------
%%% @author John Burwell <>
%%% @copyright (C) 2015, John Burwell
%%% @doc
%%%
%%% @end
%%% Created : 19 Mar 2015 by John Burwell <>
%%%-------------------------------------------------------------------
-module(rt_riak_node).

-behaviour(gen_fsm).

-include_lib("eunit/include/eunit.hrl").

%% API
-export([admin/2,
         admin/3,
         assert_singleton/1,
         attach/2,
         attach_direct/2,
         brutal_kill/1,
         claimant_according_to/1,
         clean_data_dir/1,
         clean_data_dir/2,
         commit/1,
         console/2,
         get_ring/1,
         is_allocated/1,
         is_ready/1,
         is_stopped/1,
         join/2,
         members_according_to/1,
         owners_according_to/1,
         partitions/1,
         ping/1,
         plan/1,
         riak/2,
         riak_repl/2,
         search_cmd/2,
         start/1,
         start/2,
         start_link/5,
         status_of_according_to/1,
         stop/1,
         stop/2,
         upgrade/2,
         wait_for_service/2,
         wait_until_pingable/1,
         wait_until_registered/2,
         wait_until_unpingable/1,
         version/1]).

%% gen_fsm callbacks
-export([init/1, state_name/2, state_name/3, handle_event/3,
         handle_sync_event/4, handle_info/3, ready/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-type host() :: string().
-type node_id() :: string().

-record(configuration, {one :: proplists:proplist(),
                        two :: proplists:proplist()}).

-record(state, {config :: #configuration{},
                host :: host(),
                id :: node_id(),
                install_type :: module(),
                name :: node(),
                transport :: module(),
                version :: string()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Call 'bin/riak-admin' command on `Node' with arguments `Args'
-spec admin(node(), [term()]) -> {ok, term()} | rt_util:error().
admin(Node, Args) ->
    admin(Node, Args, []).

%% @doc Call 'bin/riak-admin' command on `Node' with arguments `Args'.
%% The third parameter is a list of options. Valid options are:
%%    * `return_exit_code' - Return the exit code along with the command output
-spec admin(node(), [term()], [term()]) -> {ok, term()} | rt_util:error().
admin(Node, Args, Options) ->
    gen_fsm:sync_send_event(Node, {admin, Node, Args, Options}).

%% @doc Ensure that the specified node is a singleton node/cluster -- a node
%%      that owns 100% of the ring.
-spec assert_singleton(node()) -> boolean().
assert_singleton(Node) ->
    SingletonFlag = gen_fsm:sync_send_event(Node, check_singleton),
    ?assert(SingletonFlag),
    SingletonFlag.

%% @doc Runs `riak attach' on a specific node, and tests for the expected behavoir.
%%      Here's an example: ```
%%      rt_cmd_line:attach(Node, [{expect, "erlang.pipe.1 \(^D to exit\)"},
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
-spec attach(node(), {expect, list()} | {send, list()}) -> {ok, term()} | rt_util:error().
attach(Node, Expected) ->
    gen_fsm:sync_send_event(Node, {attach, Expected}).

%% @doc Runs 'riak attach-direct' on a specific node
%% @see rt_riak_node:attach/2
-spec attach_direct(node(), {expect, list()} | {send, list()}) -> {ok, term()} | rt_util:error().
attach_direct(Node, Expected) ->
    gen_fsm:sync_send_event(Node, {attach_direct, Expected}).

-spec brutal_kill(node()) -> rt_util:result().
brutal_kill(Node) ->
    gen_fsm:sync_send_all_state_event(Node, brutal_kill).

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
claimant_according_to(Node) ->
    gen_fsm:sync_send_event(Node, claimant_according_to).

-spec clean_data_dir(node()) -> rt_util:result().
clean_data_dir(Node) ->
    gen_fsm:sync_send_event(Node, clean_data_dir).

-spec clean_data_dir(node(), list()) -> rt_util:result().
clean_data_dir(Node, SubDir) ->
    gen_fsm:sync_send_event(Node, {clean_data_dir, SubDir}).

%% @doc Runs `riak console' on a specific node
%% @see rt_riak_node:attach/2
-spec console(node(), {expect, list()} | {send, list()}) -> {ok, term()} | rt_util:error().
console(Node, Expected) ->
    geb_fsm:sync_send_event(Node, {console, Expected}).

-spec commit(node()) -> rt_util:result().
commit(Node) ->
    gen_fsm:sync_send_event(Node, commit).

%% @doc Get the raw ring for `Node'.
-spec get_ring(node()) -> term().
get_ring(Node) ->
    gen_fsm:sync_send_event(Node, get_ring).

-spec is_allocated(node()) -> boolean.
is_allocated(Node) ->
    gen_fsm:sync_send_event(Node, is_allocated).

-spec is_ready(node()) -> boolean().
is_ready(Node) ->
    gen_fsm:sync_send_event(Node, is_ready).

-spec is_stopped(node()) -> boolean().
is_stopped(Node) ->
    gen_fsm:sync_send_event(Node, is_stopped).

-spec join(node(), node()) -> rt_util:result().
join(Node, ToNode) ->
    gen_fsm:sync_send_event(Node, {join, ToNode}).

%% @doc Return a list of cluster members according to the ring retrieved from
%%      the specified node.
-spec members_according_to(node()) -> [term()] | rt_util:error().
members_according_to(Node) ->
    gen_fsm:sync_send_event(Node, members_according_to).

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
-spec owners_according_to(node()) -> [term()].
owners_according_to(Node) ->
    gen_fsm:sync_send_event(Node, owners_according_to).

%% @doc Get list of partitions owned by node (primary).
-spec partitions(node()) -> [term()].
partitions(Node) ->
    lager:error("partitions(~p) is not implemented.", [Node]),
    [].

-spec ping(node()) -> boolean().
ping(Node) ->
    gen_fsm:sync_send_event(Node, ping).

-spec plan(node()) -> rt_util:result().
plan(Node) ->
    gen_fsm:sync_send_event(Node, plan).

%% @doc Call 'bin/riak' command on `Node' with arguments `Args'
-spec riak(node(), [term()]) -> {ok, term()} | rt_util:error().
riak(Node, Args) ->
    gen_fsm:sync_send_event(Node, {riak, Args}).

%% @doc Call 'bin/riak' command on `Node' with arguments `Args'
-spec riak_repl(node(), [term()]) -> {ok, term()} | rt_util:error().
riak_repl(Node, Args) ->
    gen_fsm:sync_send_event(Node, {riak_repl, Args}).

-spec search_cmd(node(), [term()]) -> {ok, term()} | rt_util:error().
search_cmd(Node, Args) ->
    gen_fsm:sync_send_event(Node, {search_cmd, Args}).

-spec start(node()) -> rt_util:result().
start(Node) ->
    start(Node, true).

-spec start(node(), boolean()) -> rt_util:result().
start(Node, Wait) ->
    gen_fsm:sync_send_event(Node, {start, Wait}).

%% @doc Starts a gen_fsm process to configure, start, and
%% manage a Riak node on the `Host' identified by `NodeId'
%% and `NodeName' using Riak `Version' ({product, release})
-spec start_link(host(), node_id(), node(), #configuration{}, string()) -> {ok, pid()} | ignore | rt_util:error().
start_link(Host, NodeId, NodeName, Config, Version) ->
    Args = [Host, NodeId, NodeName, Config, Version],
    gen_fsm:start_link(NodeName, ?MODULE, Args, []).

%% @doc Return the cluster status of `Member' according to the ring
%%      retrieved from `Node'.
-spec status_of_according_to(node()) -> [term()] | rt_util:error().
status_of_according_to(Node) ->
    gen_fsm:sync_send_event(Node, status_of_according_to).


-spec stop(node()) -> rt_util:result().
stop(Node) ->
    stop(Node, true).

-spec stop(node(), boolean()) -> rt_util:result().
stop(Node, Wait) ->
    gen_fsm:sync_send_event(Node, {stop, Wait}).

upgrade(Node, NewVersion) ->
    gen_fsm:sync_send_event(Node, {upgrade, NewVersion}).

-spec wait_for_service(node(), [string()]) -> rt_util:result().
wait_for_service(Node, Services) ->
    gen_fsm:sync_send_event(Node, {wait_for_services, Services}).

-spec wait_until_pingable(node()) -> rt_util:result().
wait_until_pingable(Node) ->
    gen_fsm:sync_send_event(Node, wait_until_pingable).

-spec wait_until_registered(node(), atom()) -> rt_util:result().
wait_until_registered(Node, Name) ->
    gen_fsm:sync_send_event(Node, {wait_until_registered, Name}).

-spec wait_until_unpingable(node()) -> rt_util:result().
wait_until_unpingable(Node) ->
    gen_fsm:sync_send_event(Node, wait_until_unpingable).

version(Node) ->
    gen_fsm:sync_send_event(Node, version).

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
init([Host, NodeId, NodeName, Config, Version]) ->
    State = #state{host=Host,
                   id=NodeId,
                   name=NodeName,
                   config=Config,
                   version=Version},
    {ok, allocated, State}.

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

ready(get_ring, _From, #state{name=NodeName}=State) ->
    {ok, Ring} = maybe_get_ring(NodeName),
    {reply, Ring, ready, State};
ready(members_according_to, _From, #state{name=NodeName}=State) ->
    Members = maybe_members_according_to(NodeName),
    {reply, Members, ready, State};
ready(partitions, _From, #state{name=NodeName}=State) ->
    Partitions = maybe_partitions(NodeName),
    {reply, Partitions, ready, State};
ready(owners_according_to, _From, #state{name=NodeName}=State) ->
    Owners = maybe_owners_according_to(NodeName),
    {reply, Owners, ready, State}.

-spec maybe_get_ring(node()) -> rt_rpc_result().
maybe_get_ring(NodeName) ->
    maybe_rpc_call(NodeName, riak_core_ring_manager, get_raw_ring, []).

-spec maybe_partitions(node()) -> rt_rpc_result().
maybe_partitions(NodeName) ->
    maybe_partitions(NodeName, maybe_get_ring(NodeName)).

-spec maybe_partitions(node(), rt_rpc_result()) -> [term()] | rt_util:error().
maybe_partitions(NodeName, {ok, Ring}) ->
    [Idx || {Idx, Owner} <- riak_core_ring:all_owners(Ring), Owner == NodeName];
maybe_partitions(_NodeName, {error, Reason}) ->
    {error, Reason}.

-spec maybe_members_according_to(node() | rt_rpc_result()) -> [term()] | rt_util:error().
maybe_members_according_to({ok, Ring}) ->
    riak_core_ring:all_members(Ring);
maybe_members_according_to({error, Reason}) ->
    {error, Reason};
maybe_members_according_to(NodeName) ->
    maybe_members_according_to(maybe_get_ring(NodeName)).

-spec maybe_owners_according_to(node() | rt_rpc_result()) -> [term()] | rt_util:error().
maybe_owners_according_to({ok, Ring}) ->
    Owners = [Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)],
    lists:usort(Owners);
maybe_owners_according_to({error, Reason}) ->
    {error,Reason};
maybe_owners_according_to(NodeName) ->
    maybe_owners_according_to(maybe_get_ring(NodeName)).

%% TODO Move to rt_util ??
-type erl_rpc_result() :: {ok, term()} | {badrpc, term()}.
-type rt_rpc_result() :: {ok, term()} | rt_util:error().
-spec maybe_rpc_call(node(), module(), function(), [term()]) -> erl_rpc_result().
maybe_rpc_call(NodeName, Module, Function, Args) ->
    maybe_rpc_call(rpc:call(NodeName, Module, Function, Args)).

-spec maybe_rpc_call(erl_rpc_result()) -> rt_rpc_result().
maybe_rpc_call({badrpc, _}) ->
    {error, badrpc};
maybe_rpc_call(Result) ->
    Result.

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
