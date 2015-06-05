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

%% TODO Document the following topics:
%% - State model and defintions
%% - Error handling: what is passed back to the client to handle 
%%   vs. what stops the FSM
%% - 


%% API
-export([admin/2,
         admin/3,
         attach/2,
         attach_direct/2,
         brutal_kill/1,
         check_singleton/1,
         claimant_according_to/1,
         clean_data_dir/1,
         clean_data_dir/2,
         commit/1,
         console/2,
         cookie/1,
         copy_logs/2,
         get_ring/1,
         ip/1,
         is_invalid/1,
         is_ready/1,
         is_started/1,
         is_stopped/1,
         host/1,
         join/2,
         maybe_wait_for_changes/1,
         members_according_to/1,
         owners_according_to/1,
         partitions/1,
         ping/1,
         plan/1,
         release/1,
         riak/2,
         riak_repl/2,
         search_cmd/2,
         set_cookie/2,
         staged_join/2,
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
-export([init/1, handle_event/3, stopped/3, stopped/2, 
         handle_sync_event/4, handle_info/3, invalid/3, 
         ready/2, ready/3, started/3, terminate/3, 
         code_change/4]).

%% internal exports
-export([do_check_singleton/3, do_wait_until_pingable/2, 
         do_wait_until_registered/3, do_wait_for_service/3, 
         do_unpingable/1, get_os_pid/2, start_riak_daemon/3]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, infinity).

%% @doc The directory overlay describes the layout of the various
%% directories on the node.  While the paths are absolute, they
%% are agnostic of a local/remote installation.  The values in
%% this structure correspond to the directory locations in the 
%% Riak 2.x+ riak.conf file.  Use of this structure allows paths
%% to commands and files to be calculated in an installation/transport
%% neutral manner.
%%
%% For devrel-based installations, the layout will be calculated 
%% relative to the root_path provided in the node_type.
%%
%% When support for package-based installations is implemented,
%% the directories will correspond to those used by the package
%% to deploy Riak on the host OS. 
%%
%% @since 1.1.0
-record(directory_overlay, {bin_dir :: filelib:dirname(),
                            conf_dir :: filelib:dirname(),
                            data_dir :: filelib:dirname(),
                            home_dir :: filelib:dirname(),
                            lib_dir :: filelib:dirname(),
                            log_dir :: filelib:dirname()}).

%% @doc Defines the following metadata elements required to initialize 
%% and control a Riak devrel node:
%%
%%     * id: The devrel atom
%%     * root_path: The absolute path to the root directory to
%%                  the available version/node sets
%%     * node_id: The number of the devrel node to be managed by
%%                the FSM process (e.g. 1).  This number is
%%                used to form the base path of node as
%%                <root_path>/<version>/dev<node_id>
%%
%% @since 1.1.0
-type devrel_node_type() :: [{root_path, filelib:dirname()} |
                             {id, devrel} |
                             {node_id, pos_integer()}].

%% @doc This record bundles the pieces required to provision a node
%% and attach orchestration to it.  
%% 
%% @since 1.1.0 
-record(definition, {config=[] :: proplists:proplist(),
                     hostname=localhost :: rt_host:hostname(),
                     name :: node(),
                     type :: devrel_node_type(),
                     version :: rt_util:version()}).

-type(node_definition() :: #definition{}).
-exporttype(node_definition/0).

-record(state, {host :: rt_host:host(),
                id :: node_id(),
                directory_overlay :: #directory_overlay{},
                name :: node(),
                os_pid=0 :: pos_integer(),
                required_services=[riak_kv] :: [atom()],
                start_command :: rt_host:command(),
                stop_command :: rt_host:command(),
                version :: rt_util:version()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Call 'bin/riak-admin' command on `Node' with arguments `Args'
%%
%% @since 1.1.0
-spec admin(node(), [term()]) -> {ok, term()} | rt_util:error().
admin(Node, Args) ->
    admin(Node, Args, []).

%% @doc Call 'bin/riak-admin' command on `Node' with arguments `Args'.
%% The third parameter is a list of options. Valid options are:
%%    * `return_exit_code' - Return the exit code along with the command output
%%
%% @since 1.1.0
-spec admin(node(), [term()], [term()]) -> {ok, term()} | rt_util:error().
admin(Node, Args, Options) ->
    gen_fsm:sync_send_event(Node, {admin, Node, Args, Options}, ?TIMEOUT).

%% @doc Runs `riak attach' on a specific node, and tests for the expected behavoir.
%%      Here's an example: ```
%%      rt_riak_node:attach(Node, [{expect, "erlang.pipe.1 \(^D to exit\)"},
%%                       {send, "riak_core_ring_manager:get_my_ring()."},
%%                       {expect, "dict,"},
%%                       {send, [4]}]), %% 4 = Ctrl + D'''
%%      `{expect, String}' scans the output for the existance of the String.
%%         These tuples are processed in order.
%%
%%      `{send, String}' sends the string to the console.
%%         Once a send is encountered, the buffer is discarded, and the next
%%         expect will process based on the output following the sent data.
%%       ```
%%
%% @since 1.1.0
-spec attach(node(), {expect, list()} | {send, list()}) -> {ok, term()} | rt_util:error().
attach(Node, Expected) ->
    gen_fsm:sync_send_event(Node, {attach, Expected}, ?TIMEOUT).

%% @doc Runs 'riak attach-direct' on a specific node
%% @see rt_riak_node:attach/2
%%
%% @since 1.1.0
-spec attach_direct(node(), {expect, list()} | {send, list()}) -> {ok, term()} | rt_util:error().
attach_direct(Node, Expected) ->
    gen_fsm:sync_send_event(Node, {attach_direct, Expected}, ?TIMEOUT).

%% @doc Kills any Riak processes running on the passed `Node', and resets the
%%      the state of the FSM to `stopped'.  Therefore, this function is the means
%%      to reset/resync the state of a Riak node FSM with a running Riak node.
%%
%% @since 1.1.0 
-spec brutal_kill(node()) -> rt_util:result().
brutal_kill(Node) ->
    gen_fsm:sync_send_all_state_event(Node, brutal_kill, ?TIMEOUT).

%% @doc Ensure that the specified node is a singleton node/cluster -- a node
%%      that owns 100% of the ring.
%%
%% @since 1.1.0
-spec check_singleton(node()) -> boolean().
check_singleton(Node) ->
    gen_fsm:sync_send_event(Node, check_singleton, ?TIMEOUT).

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
%%
%% @since 1.1.0
claimant_according_to(Node) ->
    gen_fsm:sync_send_event(Node, claimant_according_to, ?TIMEOUT).

-spec clean_data_dir(node()) -> rt_util:result().
clean_data_dir(Node) ->
    gen_fsm:sync_send_event(Node, clean_data_dir, ?TIMEOUT).

-spec clean_data_dir(node(), list()) -> rt_util:result().
clean_data_dir(Node, SubDir) ->
    gen_fsm:sync_send_event(Node, {clean_data_dir, SubDir}, ?TIMEOUT).

%% @doc Runs `riak console' on a the passed `Node'
%% @see rt_riak_node:attach/2
%%
%% @since 1.1.0
-spec console(node(), {expect, list()} | {send, list()}) -> {ok, term()} | rt_util:error().
console(Node, Expected) ->
    geb_fsm:sync_send_event(Node, {console, Expected}, ?TIMEOUT).

%% @doc Commits changes to a cluster using the passed `Node'
%%
%% @since 1.1.0
-spec commit(node()) -> rt_util:result().
commit(Node) ->
    gen_fsm:sync_send_event(Node, commit, ?TIMEOUT).

%% @doc Retrieves the Erlang cookie current of the passed `Node'
%%
%% @since 1.1.0
-spec cookie(node()) -> atom() | rt_util:error().
cookie(Node) ->
    gen_fsm:sync_send_event(Node, cookie, ?TIMEOUT).

%% @doc Copy all logs from the passed node, `Node', to the directory, `ToDir'
%%
%% @since 1.1.0
-spec copy_logs(node(), string()) -> rt_util:result().
copy_logs(Node, ToDir) ->
    gen_fsm:sync_send_all_state_event(Node, {copy_logs, ToDir}, ?TIMEOUT).

%% @doc Get the raw ring for `Node'.
%%
%% @since 1.1.0
-spec get_ring(node()) -> {ok, term()} | rt_util:error().
get_ring(Node) ->
    gen_fsm:sync_send_event(Node, get_ring, ?TIMEOUT).

%% @doc Returns the IP address of the passed `Node'
%%
%% @since 1.1.0
-spec ip(node() | string()) -> string(). 
ip(Node) ->
    gen_fsm:sync_send_all_state_event(Node, ip, ?TIMEOUT).

is_invalid(Node) ->
    gen_fsm:sync_send_all_state_event(Node, is_invalid, ?TIMEOUT).

%% @doc Returns `true' if the passed node, `Node', is ready to
%%      accept requests.  If the node is not ready or stopped,
%5      this function returns `false'.
%%
%% @since 1.1.0
-spec is_ready(node()) -> boolean().
is_ready(Node) ->
    gen_fsm:sync_send_all_state_event(Node, is_ready, ?TIMEOUT).

is_started(Node) ->
    gen_fsm:sync_send_all_state_event(Node, is_started, ?TIMEOUT).

%% @doc Returns `true' if the passed node, `Node', is not running.  
%%      If the node is started, ready, or invalid, this function 
%%      returns `false'.
%%
%% @since 1.1.0
-spec is_stopped(node()) -> boolean().
is_stopped(Node) ->
    gen_fsm:sync_send_all_state_event(Node, is_stopped, ?TIMEOUT).

-spec host(node()) -> rt_host:host().
host(Node) ->
    gen_fsm:sync_send_all_state_event(Node, host, ?TIMEOUT).

-spec join(node(), node()) -> rt_util:result().
join(Node, ToNode) ->
    gen_fsm:sync_send_event(Node, {join, ToNode}, ?TIMEOUT).

-spec maybe_wait_for_changes(node()) -> rt_util:result().
maybe_wait_for_changes(Node) ->
    gen_fsm:sync_send_event(Node, maybe_wait_for_changes, ?TIMEOUT).

%% @doc Return a list of cluster members according to the ring retrieved from
%%      the specified node.
%%
%% @since 1.1.0
-spec members_according_to(node()) -> [term()] | rt_util:error().
members_according_to(Node) ->
    gen_fsm:sync_send_event(Node, members_according_to, ?TIMEOUT).

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
%%
%% @since 1.1.0
-spec owners_according_to(node()) -> [term()].
owners_according_to(Node) ->
    gen_fsm:sync_send_event(Node, owners_according_to, ?TIMEOUT).

%% @doc Get list of partitions owned by node (primary).
%%
%% @since 1.1.0
-spec partitions(node()) -> [term()].
partitions(Node) ->
    gen_fsm:sync_send_event(Node, partitions, ?TIMEOUT).

-spec ping(node()) -> boolean().
ping(Node) ->
    gen_fsm:sync_send_all_state_event(Node, ping, ?TIMEOUT).

-spec plan(node()) -> rt_util:result().
plan(Node) ->
    gen_fsm:sync_send_event(Node, plan, ?TIMEOUT).

%% @doc Releases the `Node' for use by another cluster.  This function ensures
%% that the node is stopped before returning it for use by another cluster.
%%
%% @since 1.1.0
-spec release(node()) -> rt_util:result().
release(Node) ->
    case whereis(Node) of
        undefined ->
            ok;
        _ ->
            gen_fsm:sync_send_all_state_event(Node, release, ?TIMEOUT)
    end.

%% @doc Call 'bin/riak' command on `Node' with arguments `Args'
%%
%% @since 1.1.0
-spec riak(node(), [term()]) -> {ok, term()} | rt_util:error().
riak(Node, Args) ->
    gen_fsm:sync_send_all_state_event(Node, {riak, Args}, ?TIMEOUT).

%% @doc Call 'bin/riak' command on `Node' with arguments `Args'
%%
%% @since 1.1.0
-spec riak_repl(node(), [term()]) -> {ok, term()} | rt_util:error().
riak_repl(Node, Args) ->
    gen_fsm:sync_send_event(Node, {riak_repl, Args}, ?TIMEOUT).

-spec search_cmd(node(), [term()]) -> {ok, term()} | rt_util:error().
search_cmd(Node, Args) ->
    gen_fsm:sync_send_event(Node, {search_cmd, Args}, ?TIMEOUT).

-spec set_cookie(node(), atom()) -> rt_util:result().
set_cookie(Node, NewCookie) ->
    gen_fsm:sync_send_event(Node, {set_cookie, NewCookie}, ?TIMEOUT).

-spec staged_join(node(), node()) -> rt_util:result().
staged_join(Node, ToNode) ->
    gen_fsm:sync_send_event(Node, {staged_join, ToNode}, ?TIMEOUT).

%% TODO Document the behavior of start including ready checks
-spec start(node()) -> rt_util:result().
start(Node) ->
    start(Node, true).

-spec start(node(), boolean()) -> rt_util:result().
start(Node, true) ->
    gen_fsm:sync_send_event(Node, start, ?TIMEOUT);
start(Node, false) ->
    gen_fsm:send_event(Node, start).

%% @doc Starts a gen_fsm process to configure, start, and
%% manage a Riak node on the `Host' identified by `NodeId'
%% and `NodeName' using Riak `Version' ({product, release})
-spec start_link(rt_host:hostname(), node_definition(), node_id(), proplists:proplist(), rt_util:version()) -> 
                        {ok, node()} | ignore | rt_util:error().
start_link(Hostname, NodeDefinition, NodeId, Config, Version) ->
    %% TODO Re-implement node naming when 1.x and 2.x configuration is propely implemented
    %% NodeName = list_to_atom(string:join([dev(NodeId), atom_to_list(Hostname)], "@")),
    NodeName = list_to_atom(string:join([dev(NodeId), "127.0.0.1"], "@")),
    Args = [Hostname, NodeType, NodeId, NodeName, Config, Version],
    case gen_fsm:start({local, NodeName}, ?MODULE, Args, []) of
        {ok, _} ->
            {ok, NodeName};
        Error={error, _} ->
            Error
    end.

%% @doc Return the cluster status of `Member' according to the ring
%%      retrieved from `Node'.
%%
%% @since 1.1.0
-spec status_of_according_to(node()) -> [term()] | rt_util:error().
status_of_according_to(Node) ->
    gen_fsm:sync_send_event(Node, status_of_according_to, ?TIMEOUT).

-spec stop(node()) -> rt_util:result().
stop(Node) ->
    stop(Node, true).

-spec stop(node(), boolean()) -> rt_util:result().
stop(Node, true) ->
    gen_fsm:sync_send_event(Node, stop, ?TIMEOUT);
stop(Node, false) ->
    gen_fsm:send_event(Node, stop).

-spec upgrade(node(), rt_util:version()) -> rt_util:result().
upgrade(Node, NewVersion) ->
    gen_fsm:sync_send_event(Node, {upgrade, NewVersion}, ?TIMEOUT).

-spec wait_for_service(node(), atom() | [atom()]) -> rt_util:wait_result().
wait_for_service(Node, Services) when is_list(Services) ->
    gen_fsm:sync_send_event(Node, {wait_for_services, Services}, ?TIMEOUT);
wait_for_service(Node, Service) ->
    wait_for_service(Node, [Service]).

-spec wait_until_pingable(node()) -> rt_util:wait_result().
wait_until_pingable(Node) ->
    gen_fsm:sync_send_event(Node, wait_until_pingable, ?TIMEOUT).

-spec wait_until_registered(node(), atom()) -> rt_util:wait_result().
wait_until_registered(Node, Name) ->
    gen_fsm:sync_send_event(Node, {wait_until_registered, Name}, ?TIMEOUT).

-spec wait_until_unpingable(node()) -> rt_util:wait_result().
wait_until_unpingable(Node) ->
    gen_fsm:sync_send_all_state_event(Node, wait_until_unpingable, ?TIMEOUT).

-spec version(node()) -> rt_util:version().
version(Node) ->
    gen_fsm:sync_send_all_state_event(Node, version, ?TIMEOUT).

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
init([Hostname, NodeType, NodeId, NodeName, ConfigOverrides, Version]) ->
    DirOverlay = create_directory_overlay(NodeType, Version, NodeId),
    case rt_host:connect(Hostname) of
        {ok, Host} ->
            DirCreateResult = create_snmp_dirs(Host, DirOverlay#directory_overlay.data_dir),
            ConfigureResult = maybe_configure_node(DirCreateResult, Version, NodeName, Host, 
                                                   DirOverlay, ConfigOverrides),
            maybe_transition_to_stopped(ConfigureResult, Version, NodeId, NodeName, Host, 
                                        DirOverlay, NodeType);
         Error ->
            maybe_transition_to_stopped(Error, Version, NodeId, NodeName, none, DirOverlay, 
                                        NodeType)
    end.

-spec maybe_configure_node(rt_util:result(), rt_util:version(), node(), rt_host:host(), #directory_overlay{}, proplists:proplist()) -> rt_util:result().
maybe_configure_node(ok, Version, NodeName, Host, DirOverlay, ConfigOverrides) ->
    ExistingConfig = case load_configuration(Version, Host, DirOverlay) of 
                         {ok, [Term]} -> 
                             Term;
                         {error, LoadError} -> 
                             lager:warning("Unable to load existing configuration for node ~p due to ~p.  Defaulting to an empty configuration.", 
                                           [NodeName, LoadError]),
                             []
                     end,
    %% TODO account for properly assigning the node name ...
    %% TODO handle backend ...
    Config = rt_util:merge_configs(ExistingConfig, ConfigOverrides),
    save_configuration(Version, Host, Config, DirOverlay);
maybe_configure_node(Error={error, _}, _Version, _NodeName, _Host, _DirOverlay, _ConfigOverrides) ->
    Error.

-spec maybe_transition_to_stopped(rt_util:result(), rt_util:version(), node_id(), node(), rt_host:host(), 
                                  #directory_overlay{}, atom()) -> {ok, stopped, #state{}} | {stop, term()}.
maybe_transition_to_stopped(ok, Version, NodeId, NodeName, Host, DirOverlay, NodeType) ->
    State=#state{host=Host,
                 id=NodeId,
                 name=NodeName,
                 directory_overlay=DirOverlay,
                 start_command=start_command(NodeType, DirOverlay),
                 stop_command=stop_command(NodeType, DirOverlay),
                 version=Version},
    {ok, stopped, State};
maybe_transition_to_stopped({error, Reason}, _Version, _NodeId, _NodeName, _Host, _DirOverlay, _NodeType) ->
    {stop, Reason}.

invalid(Event, _From, State) ->
    lager:error("Attempt to perform ~p operation (state: ~p)", [Event, State]),
    {reply, {error, node_invalid}, invalid, State}.
    
stopped(clean_data_dir, _From, State=#state{directory_overlay=DirOverlay, host=Host}) ->
    {reply, do_clean_data_dir(Host, DirOverlay), stopped, State};
stopped({clean_data_dir, SubDir}, _From, State=#state{directory_overlay=DirOverlay, host=Host}) ->
    {reply, do_clean_data_dir(Host, DirOverlay, SubDir), stopped, State};
stopped(start, _From, State=#state{host=Host, name=Node}) ->
    lager:info("Starting node synchronously ~p on ~p", [Node, Host]),
    {Result, {NextState, UpdState}} = do_start_and_update_state(State),
    {reply, Result, NextState, UpdState}; 
stopped(stop, _From, State=#state{host=Host, name=Node}) ->
    lager:warning("Stop called on an already stopped node ~p on ~p", [Node, Host]),
    {reply, ok, stopped, State};
stopped(Event, _From, State=#state{host=Host, name=Node}) ->
    %% The state of the node is not harmed.  Therefore, we leave the FSM running
    %% in the stopped state, but refuse to execute the command ...
    lager:error("Invalid operation ~p when node ~p on ~p is in stopped state", [Event, Node, Host]),
    {reply, {error, invalid_stopped_event}, stopped, State}.

started(start, _From, State=#state{host=Host, name=Node, required_services=RequiredServices,
                                    version=Version}) ->
    case do_wait_until_ready(Host, Node, RequiredServices, Version) of
        true -> {reply, ok, ready, State};
        false -> {error, node_not_ready, started, State}
    end;
started(Event, _From, State=#state{host=Host, name=Node}) ->
    lager:error("Invalid operation ~p when node ~p on ~p is in started state", [Event, Node, Host]),
    {reply, {error, invalid_started_event}, started, State}.

ready({admin, Args, Options}, _From, State=#state{host=Host, directory_overlay=DirOverlay}) ->
    {reply, do_admin(Args, Options, DirOverlay, Host), ready, State};
ready({attach, Expected}, _From, State=#state{directory_overlay=DirOverlay}) ->
    {reply, do_attach(DirOverlay, Expected), ready, State};
ready({attach_direct, Expected}, _From, State=#state{directory_overlay=DirOverlay}) ->
    {reply, do_attach_direct(DirOverlay, Expected), ready, State};
ready(check_singleton, _From, State=#state{host=Host, name=Node, version=Version}) ->
    %% TODO consider adding the started state back
    transition_to_state_and_reply(do_check_singleton(Host, Node, Version), ready, stopped, State);
ready(commit, _From, State=#state{host=Host, name=Node}) ->
    {reply, do_commit(Host, Node), ready, State};
ready({console, Expected}, _From, State=#state{directory_overlay=DirOverlay}) ->
    {reply, do_console(DirOverlay, Expected), ready, State};
ready(cookie, _From, State=#state{name=Node}) ->
    {reply, rt_util:maybe_rpc_call(Node, erlang, get_cookie, []), ready, State};
%% TODO Determine whether or not it makes sense to support get_ring in the started
%% state ...
ready(get_ring, _From, #state{name=NodeName}=State) ->
    Result = maybe_get_ring(NodeName),
    {reply, Result, ready, State};
ready({join, ToNode}, _From, State=#state{host=Host, name=Node}) ->
    {reply, do_join(Host, Node, ToNode), ready, State};
ready(maybe_wait_for_changes, _From, State=#state{host=Host, name=Node}) ->
    {reply, do_maybe_wait_for_changes(Host, Node), ready, State};
ready(members_according_to, _From, #state{name=NodeName}=State) ->
    Members = maybe_members_according_to(NodeName),
    {reply, Members, ready, State};
ready(owners_according_to, _From, #state{name=NodeName}=State) ->
    Owners = maybe_owners_according_to(NodeName),
    {reply, Owners, ready, State};
ready(partitions, _From, State=#state{name=Node}) ->
    Partitions = maybe_partitions(Node),
    {reply, Partitions, ready, State};
ready(plan, _From, State=#state{host=Host, name=Node}) ->
    {reply, do_plan(Host, Node), ready, State};
ready({riak_repl, Args}, _From, State=#state{host=Host, directory_overlay=DirOverlay}) ->
    Result = rt_host:exec(Host, riak_repl_path(DirOverlay), Args),
    {reply, Result, ready, State};
ready({set_cookie, NewCookie}, _From, State=#state{name=Node}) ->
    {reply, rt_util:maybe_rpc_call(Node, erlang, set_cookie, [Node, NewCookie]), ready, State};
ready({staged_join, ToNode}, _From, State=#state{host=Host, name=Node}) ->
    {reply, do_staged_join(Host, Node, ToNode), ready, State};
ready(stop, _From, State) ->
    {Result, UpdState} = do_stop_and_update_state(State),
    %% TODO need an invalid state ...
    transition_to_state_and_reply(Result, stopped, ready, UpdState);
ready({wait_for_service, Services}, _From, State=#state{host=Host, name=Node}) ->
    %% TODO consider adding back the started state
    transition_to_state_and_reply(do_wait_for_service(Host, Node, Services), ready, stopped, State);
ready(wait_until_pingable, _From, State=#state{host=Host, name=Node}) ->
    %% TODO consider adding back the started state
    transition_to_state_and_reply(do_wait_until_pingable(Host, Node), ready, stopped, State);
ready({wait_until_registered, Name}, _From, State=#state{host=Host, name=Node}) ->
    transition_to_state_and_reply(do_wait_until_registered(Host, Node, Name), ready, stopped, State);
ready(Event, _From, State=#state{host=Host, name=Node}) ->
    %% The state of the node is not harmed.  Therefore, we leave the FSM running
    %% in the stopped state, but refuse to execute the command ...
    lager:error("Invalid operation ~p when node ~p on ~p is in ready state", [Event, Node, Host]),
    {reply, {error, invalid_ready_event}, ready, State}.

transition_to_state_and_reply(Result={error, _}, _SuccessState, FailedState, State) ->
    {reply, Result, FailedState, State};
transition_to_state_and_reply(Result, SuccessState, _FailedState, State) ->
    {reply, Result, SuccessState, State}.


transition_to_state({error, _}, _SuccessState, FailedState, State) ->
    {next_state, FailedState, State};
transition_to_state(ok, SuccessState, _FailedState, State) ->
    {next_state, SuccessState, State}.

stopped(start, State=#state{host=Host, name=Node}) ->
    lager:info("Starting node asynchronously ~p on ~p", [Node, Host]),
    {_, {NextState, UpdState}} = do_start_and_update_state(State),
    {next_state, NextState, UpdState};
stopped(stop, State=#state{host=Host, name=Node}) ->
    lager:warning("Stop called on a stopped node ~p on ~p", [Node, Host]),
    {next_state, stopped, State};
stopped(_Event, State) ->
    {next_state, stopped, State}.

ready(stop, State) ->
    {Result, UpdState} = do_stop_and_update_state(State),
    transition_to_state(Result, stopped, ready, UpdState);
ready(start, State=#state{host=Host, name=Node}) ->
    lager:warning("Start called on ready node ~p on ~p", [Node, Host]),
    {next_state, ready, State};
ready(_Event, State) ->
    {next_state, ready, State}.
    
-spec maybe_get_ring(node()) -> rt_util:rt_rpc_result().
maybe_get_ring(NodeName) ->
    rt_util:maybe_rpc_call(NodeName, riak_core_ring_manager, get_raw_ring, []).

-spec maybe_partitions(node()) -> rt_util:rt_rpc_result().
maybe_partitions(NodeName) ->
    maybe_partitions(NodeName, maybe_get_ring(NodeName)).

-spec maybe_partitions(node(), rt_util:rt_rpc_result()) -> [term()] | rt_util:error().
maybe_partitions(NodeName, {ok, Ring}) ->
    [Idx || {Idx, Owner} <- riak_core_ring:all_owners(Ring), Owner == NodeName];
maybe_partitions(_NodeName, {error, Reason}) ->
    {error, Reason}.

-spec maybe_members_according_to(node() | rt_util:tt_rpc_result()) -> [term()] | rt_util:error().
maybe_members_according_to({ok, Ring}) ->
    riak_core_ring:all_members(Ring);
maybe_members_according_to({error, Reason}) ->
    {error, Reason};
maybe_members_according_to(NodeName) ->
    maybe_members_according_to(maybe_get_ring(NodeName)).

-spec maybe_owners_according_to(node() | rt_util:rt_rpc_result()) -> [term()] | rt_util:error().
maybe_owners_according_to({ok, Ring}) ->
    Owners = [Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)],
    lists:usort(Owners);
maybe_owners_according_to({error, Reason}) ->
    {error,Reason};
maybe_owners_according_to(NodeName) ->
    maybe_owners_according_to(maybe_get_ring(NodeName)).

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
%% handle_sync_event(_Event, _From, StateName, State) ->
%%     Reply = ok,
%%     {reply, Reply, StateName, State}.
handle_sync_event(brutal_kill, _From, _StateName, 
                  State=#state{host=Host, name=Node, os_pid=OSPid}) ->
    case do_brutal_kill(Node, Host, OSPid) of
        ok -> UpdState=State#state{os_pid=0},
              {reply, ok, stopped, UpdState};
        Error={error, _} -> {stop, Error, State}
    end;
handle_sync_event({copy_logs, ToDir}, _From, StateName, 
                  State=#state{host=Host, directory_overlay=DirOverlay}) ->
    Result = rt_host:copy_dir(Host, DirOverlay#directory_overlay.log_dir, ToDir), 
    transition_to_state_and_reply(Result, StateName, StateName, State);
handle_sync_event(host, _From, StateName, State=#state{host=Host}) ->
    {reply, Host, StateName, State};
handle_sync_event(ip, _From, StateName, State=#state{host=Host}) ->
    {reply, rt_host:ip_addr(Host), StateName, State};
handle_sync_event(is_invalid, _From, invalid, State) ->
    {reply, true, invalid, State};
handle_sync_event(is_invalid, _From, StateName, State) ->
    {reply, false, StateName, State};
handle_sync_event(is_ready, _From, ready, State) ->
    {reply, true, ready, State};
handle_sync_event(is_ready, _From, StateName, State) ->
    {reply, false, StateName, State};
handle_sync_event(is_started, _From, started, State) ->
    {reply, true, started, State};
handle_sync_event(is_started, _From, StateName, State) ->
    {reply, false, StateName, State};
handle_sync_event(is_stopped, _From, stopped, State) ->
    {reply, true, stopped, State};
handle_sync_event(is_stopped, _From, StateName, State) ->
    {reply, false, StateName, State};
handle_sync_event(ping, _From, StateName, State=#state{name=Node}) ->
    {reply, do_ping(Node), StateName, State};
handle_sync_event({riak, Args}, _From, StateName, 
                  State=#state{host=Host, directory_overlay=DirOverlay}) ->
    {reply, rt_host:exec(Host, riak_path(DirOverlay), Args), StateName, State};
handle_sync_event(release, _From, _StateName, State=#state{host=Host, name=Node}) ->
    lager:info("Releasing node ~p on ~p", [Node, Host]),
    {stop, normal, ok, State};
handle_sync_event(version, _From, StateName, State=#state{version=Version}) ->
    {reply, Version, StateName, State};
handle_sync_event(wait_until_unpingable, _From, StateName, State=#state{host=Host, name=Node}) ->
    {reply, do_wait_until_unpingable(Host, Node), StateName, State}.

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
terminate(_Reason, _StateName, 
          #state{host=Host=Host, name=Node, os_pid=OSPid}) ->
    _ = do_brutal_kill(Node, Host, OSPid),
    rt_host:disconnect(Host),
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

-spec create_snmp_dirs(rt_host:host(), filelib:dirname()) -> rt_util:result().
create_snmp_dirs(Host, DataDir) ->
    SnmpDir = filename:join([DataDir, "snmp", "agent", "db"]), 
    lager:debug("Creating SNMP data directory ~p on localhost", [SnmpDir]),
    rt_host:mkdirs(Host, SnmpDir).

-spec create_directory_overlay({atom(), filelib:dirname()}, rt_util:version(), node()) -> #directory_overlay{}.
create_directory_overlay({devrel, RootPath}, Version, NodeId) -> 
    HomeDir = filename:join([rt_util:base_dir_for_version(RootPath, Version),
                             dev(NodeId)]),
    #directory_overlay{bin_dir=filename:join([HomeDir, "bin"]),
                       conf_dir=filename:join([HomeDir, "etc"]),
                       data_dir=filename:join([HomeDir, "data"]),
                       home_dir=HomeDir,
                       lib_dir=filename:join([HomeDir, "lib"]),
                       log_dir=filename:join([HomeDir, "log"])}.

-spec dev(pos_integer()) -> string().
dev(NodeId) ->
    lists:concat(["dev", integer_to_list(NodeId)]).

-spec do_admin([term()], [term()], #directory_overlay{}, rt_host:host()) -> {ok, term()} | rt_util:error().
do_admin(Args, Options, DirOverlay, Host) ->
    rt_host:exec(Host, riak_admin_path(DirOverlay), Args, Options).

-spec do_attach(#directory_overlay{}, term()) -> term().
do_attach(DirOverlay, Expected) ->
    interactive(DirOverlay, "attach", Expected).

-spec do_attach_direct(#directory_overlay{}, term()) -> term().
do_attach_direct(DirOverlay, Expected) ->    
    interactive(DirOverlay, "attach-direct", Expected).

-spec do_brutal_kill(node(), rt_util:host(), pos_integer()) -> rt_util:result().
do_brutal_kill(_Node, _Host, 0) ->
    ok;
do_brutal_kill(Node, Host, OSPid) ->
    lager:info("Killing node ~p running as PID ~s", [Node, OSPid]),
    %% try a normal kill first, but set a timer to
    %% kill -9 after 5 seconds just in case
    timer:apply_after(5000, rt_host, kill, [Host, 9, OSPid]),
    rt_host:kill(Host, 15, OSPid).

%% @private
-spec do_check_singleton(rt_host:host(), node(), rt_util:version()) -> boolean() | rt_util:error().
do_check_singleton(Host, Node, {_, Release}) when Release =/= "0.14.2" ->
    HostName = rt_host:hostname(Host),
    lager:info("Check that node ~p on ~p is a singleton", [Node, HostName]),
    Result = case maybe_get_ring(Node) of
        {ok, Ring} ->
            Owners = lists:usort([Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)]),
            [Node] =:= Owners;
        Error ->
            Error
    end,
    do_check_singleton(Result);
do_check_singleton(_Host, _Node, _Version) ->
    true.

do_check_singleton(true) ->
    true;
do_check_singleton(false) ->
    {error, node_not_singleton};
do_check_singleton(Error) ->
    Error.
    
-spec do_clean_data_dir(rt_host:host(), #directory_overlay{}) -> rt_util:result().
do_clean_data_dir(Host, DirOverlay) ->
    do_clean_data_dir(Host, DirOverlay, "").

-spec do_clean_data_dir(rt_host:host(), #directory_overlay{}, filelib:dirname()) -> rt_util:result().
do_clean_data_dir(Host, #directory_overlay{data_dir=DataDir}, SubDir) ->
    TmpDir = rt_host:temp_dir(Host),
    FromDir = filename:join([DataDir, SubDir]),
    ToDir = filename:join([TmpDir, "child"]),
    HostName = rt_host:hostname(Host),

    lager:info("Cleaning data directory ~p on ~p", [FromDir, HostName]),
    rt_util:maybe_call_funs([
                             [rt_host, mkdirs, [Host, ToDir]],
                             [rt_host, mvdir, [Host, FromDir, ToDir]],
                             [rt_host, rmdir, [Host, ToDir]],
                             [rt_host, mkdirs, [Host, ToDir]]
                            ]).

-spec do_commit(rt_util:host(), node()) -> rt_util:result().
do_commit(Host, Node) ->
    lager:info("Commit cluster plan using node ~p on host ~p", 
               [Node, Host]),
    case rt_util:maybe_rpc_call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            lager:info("commit: plan changed"),
            timer:sleep(100),
            ok = do_maybe_wait_for_changes(Host, Node),
            ok = do_plan(Host, Node),
            ok = do_commit(Host, Node);
        {error, ring_not_ready} ->
            lager:info("commit: ring not ready"),
            timer:sleep(100),
            ok = do_maybe_wait_for_changes(Host, Node),
            ok = do_commit(Host, Node);
        {error,nothing_planned} ->
            %% Assume plan actually committed somehow
            ok;
        ok ->
            ok
    end.

-spec do_console(#directory_overlay{}, term()) -> term().
do_console(DirOverlay, Expected) ->
    interactive(DirOverlay, "console", Expected).

-spec do_join(rt_util:host(), node(), node()) -> rt_util:result().
do_join(Host, Node, ToNode) ->
    Result = rt_util:maybe_rpc_call(Node, riak_core, join, [ToNode]),
    lager:info("[join] ~p on ~p to ~p: ~p", [Node, Host, ToNode, Result]),
    Result.

-spec do_staged_join(rt_util:host(), node(), node()) -> rt_util:result().
do_staged_join(Host, Node, ToNode) ->
    Result = rt_util:maybe_rpc_call(Node, riak_core, staged_join, [ToNode]),
    lager:info("[staged join] ~p on ~p to (~p): ~p", [Node, Host, ToNode, Result]),
    Result.

-spec do_maybe_wait_for_changes(rt_host:host(), node()) -> ok.
do_maybe_wait_for_changes(Host, Node) ->
    Ring = rt_ring:get_ring(Node),
    Changes = riak_core_ring:pending_changes(Ring),
    Joining = riak_core_ring:members(Ring, [joining]),
    lager:info("maybe_wait_for_changes node ~p on ~p, changes: ~p joining: ~p ",
               [Node, Host, Changes, Joining]),
    if Changes =:= [] ->
            ok;
       Joining =/= [] ->
            ok;
       true ->
            ok = rt_util:wait_until_no_pending_changes(Host, [Node])
    end.

-spec do_plan(rt_host:host(), node()) -> rt_util:result().
do_plan(Host, Node) ->
    Result = rt_util:maybe_rpc_call(Node, riak_core_claimant, plan, []),
    lager:info("Planned cluster using node ~p on ~p with result ~p", [Node, Host, Result]),
    case Result of 
            {ok, _, _} -> ok;
            Error  -> Error
    end.

-spec do_ping(node()) -> boolean().
do_ping(Node) ->
    net_adm:ping(Node) =:= pong.


-spec do_start(rt_util:host(), node(), rt_host:command()) -> pos_integer | rt_util:error().
do_start(Host, Node, StartCommand) ->
    OSPid = rt_util:maybe_call_funs([
                              [?MODULE, start_riak_daemon, [Host, Node, StartCommand]],
                              [?MODULE, get_os_pid, [Host, Node]]
                             ]),
    lager:info("Started node ~p on ~p with OS pid ~p", [Node, Host ,OSPid]),
    OSPid.
    
-spec do_wait_until_ready(rt_host:host(), node(), [atom()], rt_util:version()) -> boolean.
do_wait_until_ready(Host, Node, RequiredServices, Version) ->
    lager:debug("Checking pingable, registered, singleton, and services for node ~p",
            [Node]),
    Result = rt_util:maybe_call_funs([
        [?MODULE, do_wait_until_pingable, [Host, Node]],
        [?MODULE, do_wait_until_registered, [Host, Node, riak_core_ring_manager]],
        [?MODULE, do_check_singleton, [Host, Node, Version]],
        [?MODULE, do_wait_for_service, [Host, Node, RequiredServices]]
    ]),
    case Result of
        ok -> true;
        Error -> lager:warning("Unable to determine that node ~p on ~p is ready due to ~p",
                            [Node, Host, Error]),
                 false
    end.

-spec do_start_and_update_state(#state{}) -> {ok | rt_util:error(), {invalid | started | ready, #state{}}}.
do_start_and_update_state(State=#state{host=Host, name=Node, required_services=RequiredServices, 
                                       start_command=StartCommand, version=Version}) ->

    OSPid = do_start(Host, Node, StartCommand),
 
    case OSPid of
        Error={error, _} -> {Error, {invalid, State}};
        _ -> UpdState = State#state{os_pid=OSPid},
             Result = do_wait_until_ready(Host, Node, RequiredServices, Version),
             case Result of
                 true -> {ok, {ready, UpdState}};
                 false -> {{error, node_not_ready}, {started, UpdState}}
             end
    end.
    

-spec do_stop_and_update_state(rt_host:host()) -> {ok | {error, term()}, #state{}}. 
do_stop_and_update_state(State=#state{host=Host, stop_command=StopCommand}) -> 
    case do_stop(Host, StopCommand) of
        {ok, _} -> {ok, State#state{os_pid=0}};
        {error, Reason} -> {{error, Reason}, State}
    end.

-spec do_stop(rt_host:host(), string()) -> rt_util:result().
do_stop(Host, StopCommand) ->
    rt_host:exec(Host, StopCommand).

%% @private
-spec do_wait_for_service(rt_host:host(), node(), [atom()]) -> rt_util:wait_result().
do_wait_for_service(Host, Node, Services) ->
    HostName = rt_host:hostname(Host),
    lager:info("Waiting for services ~p on node ~p on ~p", [Services, Node, HostName]),
    F = fun(N) ->
        case rt_util:maybe_rpc_call(N, riak_core_node_watcher, services, [N]) of
            Error={error, _} ->
                lager:error("Request for the list of services on node ~p on ~p failed due to ~p", 
                            [Node, HostName, Error]),
                Error;
            CurrServices when is_list(CurrServices) ->
                lager:debug("Found services ~p on ~p on ~p", [CurrServices, N, HostName]),
                lists:all(fun(Service) -> lists:member(Service, CurrServices) end, Services);
            Res ->
                Res
        end
    end,
    rt_util:wait_until(Node, F).

%% @private
-spec do_wait_until_pingable(rt_host:host(), node()) -> rt_util:wait_result().
do_wait_until_pingable(Host, Node) ->
    HostName = rt_host:hostname(Host),
    lager:info("Waiting for node ~p on ~p to become pingable", [Node, HostName]),
    rt_util:wait_until(Node, fun do_ping/1).

%% @private
-spec do_wait_until_registered(rt_host:host(), node(), atom()) -> rt_util:wait_result().
do_wait_until_registered(Host, Node, Name) ->
    HostName = rt_host:hostname(Host),
    lager:info("Waiting for node ~p on ~p to become registered", [Node, HostName]),
    F = fun() ->
            Registered = rpc:call(Node, erlang, registered, []),
            lists:member(Name, Registered)
    end,
    case rt_util:wait_until(F) of
    ok ->
        lager:info("Node ~p on ~p is registered", [Node, HostName]),
        ok;
    _ ->
        lager:error("The server  ~p on node ~p on ~p is not coming up.",
                   [Name, Node, HostName]),
        ?assert(registered_name_timed_out)
    end.

-spec do_wait_until_unpingable(rt_host:host(), node()) -> rt_util:result().
do_wait_until_unpingable(Host, Node) ->
    do_wait_until_unpingable(Host, Node, rt_config:get(rt_max_receive_wait_time)).

-spec do_wait_until_unpingable(rt_host:host(), node(), pos_integer()) -> rt_util:result().
do_wait_until_unpingable(Host, Node, WaitTime) ->
    Delay = rt_config:get(rt_retry_delay),
    Retry = WaitTime div Delay,
    lager:info("Wait until ~p on ~p is not pingable for ~p seconds with a retry of ~p", 
               [Node, Host, Delay, Retry]),
    %% TODO Move to stop ...
    case rt_util:wait_until(fun() -> do_unpingable(Node) end, Retry, Delay) of
        ok -> ok;
        _ ->
            lager:error("Timed out waiting for node ~p on ~p to shutdown", [Node, Host]),
            {error, node_shutdown_timed_out}
    end.
    

%% @private
-spec do_unpingable(node()) -> boolean().
do_unpingable(Node) ->
    net_adm:ping(Node) =:= pang.

%% @private
-spec get_os_pid(rt_host:host(), node()) -> pos_integer().
get_os_pid(Host, Node) ->
    HostName = rt_host:hostname(Host),
    lager:info("Retrieving the OS pid for node ~p on ~p", [Node, HostName]),
    case rt_util:maybe_rpc_call(Node, os, getpid, []) of
        Error={error, _} ->
            Error;
        OSPid ->
            list_to_integer(OSPid)
    end.

%% TODO Consider how to capture errors to provide a more meaningul status.  Using assertions
%% feels wrong ...
-spec interactive(#directory_overlay{}, string(), term()) -> term().
interactive(DirOverlay, Command, Expected) ->
    Cmd = riak_path(DirOverlay),
    lager:info("Opening a port for riak ~s.", [Command]),
    lager:debug("Calling open_port with cmd ~s", [binary_to_list(iolist_to_binary(Cmd))]),
    Port = open_port({spawn, binary_to_list(iolist_to_binary(Cmd))},
                     [stream, use_stdio, exit_status, binary, stderr_to_stdout]),
    interactive_loop(Port, Expected).

interactive_loop(Port, Expected) ->
    receive
        {Port, {data, Data}} ->
            %% We've gotten some data, so the port isn't done executing
            %% Let's break it up by newline and display it.
            Tokens = string:tokens(binary_to_list(Data), "\n"),
            [lager:debug("~s", [Text]) || Text <- Tokens],

            %% Now we're going to take hd(Expected) which is either {expect, X}
            %% or {send, X}. If it's {expect, X}, we foldl through the Tokenized
            %% data looking for a partial match via rt:str/2. If we find one,
            %% we pop hd off the stack and continue iterating through the list
            %% with the next hd until we run out of input. Once hd is a tuple
            %% {send, X}, we send that test to the port. The assumption is that
            %% once we send data, anything else we still have in the buffer is
            %% meaningless, so we skip it. That's what that {sent, sent} thing
            %% is about. If there were a way to abort mid-foldl, I'd have done
            %% that. {sent, _} -> is just a pass through to get out of the fold.

            NewExpected = lists:foldl(fun(X, Expect) ->
                    [{Type, Text}|RemainingExpect] = case Expect of
                        [] -> [{done, "done"}|[]];
                        E -> E
                    end,
                    case {Type, rt2:str(X, Text)} of
                        {expect, true} ->
                            RemainingExpect;
                        {expect, false} ->
                            [{Type, Text}|RemainingExpect];
                        {send, _} ->
                            port_command(Port, list_to_binary(Text ++ "\n")),
                            [{sent, "sent"}|RemainingExpect];
                        {sent, _} ->
                            Expect;
                        {done, _} ->
                            []
                    end
                end, Expected, Tokens),
            %% Now that the fold is over, we should remove {sent, sent} if it's there.
            %% The fold might have ended not matching anything or not sending anything
            %% so it's possible we don't have to remove {sent, sent}. This will be passed
            %% to interactive_loop's next iteration.
            NewerExpected = case NewExpected of
                [{sent, "sent"}|E] -> E;
                E -> E
            end,
            %% If NewerExpected is empty, we've met all expected criteria and in order to boot
            %% Otherwise, loop.
            case NewerExpected of
                [] -> ?assert(true);
                _ -> interactive_loop(Port, NewerExpected)
            end;
        {Port, {exit_status,_}} ->
            %% This port has exited. Maybe the last thing we did was {send, [4]} which
            %% as Ctrl-D would have exited the console. If Expected is empty, then
            %% We've met every expectation. Yay! If not, it means we've exited before
            %% something expected happened.
            ?assertEqual([], Expected)
        after rt_config:get(rt_max_receive_wait_time) ->
            %% interactive_loop is going to wait until it matches expected behavior
            %% If it doesn't, the test should fail; however, without a timeout it
            %% will just hang forever in search of expected behavior. See also: Parenting
            ?assertEqual([], Expected)
    end.


-spec start_command({devrel, string()}, #directory_overlay{}) -> rt_host:command().
start_command({devrel, _}, DirOverlay) ->
    {riak_path(DirOverlay), ["start"]}.

-spec start_riak_daemon(rt_host:host(), node(), rt_host:command()) -> rt_util:result().
start_riak_daemon(Host, Node, StartCommand) ->
    HostName = rt_host:hostname(Host),
    lager:notice("Starting riak node ~p on ~p", [Node, HostName]),
    rt_host:exec(Host, StartCommand).

-spec stop_command({devrel, string()}, #directory_overlay{}) -> rt_host:command().
stop_command({devrel, _}, DirOverlay) ->
    {riak_path(DirOverlay), ["stop"]}.

-spec riak_path(#directory_overlay{}) -> filelib:filename(). 
riak_path(#directory_overlay{bin_dir=BinDir}) ->
    filename:join([BinDir, "riak"]).

-spec riak_admin_path(#directory_overlay{}) -> filelib:filename(). 
riak_admin_path(#directory_overlay{bin_dir=BinDir}) ->
    filename:join([BinDir, "riak-admin"]).

-spec riak_repl_path(#directory_overlay{}) -> filelib:filename(). 
riak_repl_path(#directory_overlay{bin_dir=BinDir}) ->
    filename:join([BinDir, "riak-repl"]).

-spec load_configuration(rt_util:version(), rt_host:host(), #directory_overlay{}) -> 
                                {ok, proplists:proplist()} | rt_util:error().
load_configuration(Version, Host, #directory_overlay{conf_dir=ConfDir}) ->         
    rt_host:consult(Host, config_file_path(rt_util:major_release(Version), ConfDir)).

-spec save_configuration(rt_util:release(), rt_host:host(), term(), #directory_overlay{}) -> rt_util:result().
save_configuration(Version, Host, Config, #directory_overlay{conf_dir=ConfDir}) ->
    MajorRelease = rt_util:major_release(Version),
    rt_host:write_file(Host, config_file_path(MajorRelease, ConfDir), rt_util:term_serialized_form(Config)).

-spec config_file_path(rt_util:release(), filelib:dirname()) -> filelib:filename() | rt_util:error().
config_file_path(MajorRelease, ConfDir) ->
    filename:join(ConfDir, config_file_name(MajorRelease)).

-spec config_file_name(rt_util:release()) -> string().
config_file_name(1) ->
    "app.config";
config_file_name(2) ->
    "advanced.config";
config_file_name(Release) ->
    erlang:error(io_lib:format("Configuration of release ~p is not supported", [Release])).
 
-ifdef(TEST).

-define(TEST_ROOT_PATH, filename:join([os:getenv("HOME"), "rt", "riak"])).
-define(TEST_VERSION, {riak_ee, "2.0.5"}).

bootstrap() ->
    rt_util:setup_test_env().

init_node(HostName, NodeId) ->
    {Result, Node} = start_link(HostName, {devrel, ?TEST_ROOT_PATH}, NodeId, [], ?TEST_VERSION),
    ?assertEqual(ok, clean_data_dir(Node)),
    ?debugFmt("Initialized node id ~p (~p) on ~p as ~p with result ~p", 
              [NodeId, ?TEST_VERSION, HostName, Node, Result]),
    ?assertEqual(ok, Result),
    ?assertEqual(true, is_stopped(Node)),
    ?assertEqual(false, is_ready(Node)),

    Node.

setup() ->
    true = bootstrap(),
    init_node(localhost, 1).

multi_node_setup(NumNodes) ->
    true = bootstrap(),
    [ init_node(localhost, NodeId) || NodeId <- lists:seq(1, NumNodes) ].

teardown(Nodes) when is_list(Nodes) ->
    lists:map(fun(Node) -> release(Node) end, Nodes);
teardown(Node) ->
    ?debugFmt("Releasing node ~p", [Node]),
    release(Node).

dev_test_() ->
    ?_assertEqual("dev1", dev(1)).

ip_test_() ->
    {timeout, 300,
        {foreach,
         fun setup/0,
         fun teardown/1,
         [fun(Node) -> {"get_ip", ?_assertEqual("127.0.0.1", ip(Node))} end]}}.

verify_async_start(Node) ->
    Result = start(Node, false),
    ?debugFmt("Started node asynchronously ~p with result ~p", [Node, Result]),
 
    ?assertEqual(ok, Result),
    Result.

verify_sync_start(Node) ->
    Result = start(Node),
    ?debugFmt("Started node synchronously ~p with result ~p", [Node, Result]),

    ?assertEqual(ok, Result),
    ?assertEqual(false, is_invalid(Node)),
    ?assertEqual(false, is_stopped(Node)),
    ?assertEqual(false, is_started(Node)),
    ?assertEqual(true, is_ready(Node)),
    Result.
    
verify_sync_stop(Node) ->
    Result = stop(Node),
    ?debugFmt("Stopped node ~p with result ~p", [Node, Result]),

    ?assertEqual(ok, Result),
    ?assertEqual(true, is_stopped(Node)),
    ?assertEqual(false, is_ready(Node)),
    ?assertEqual(false, is_invalid(Node)),
    ?assertEqual(false, is_started(Node)),
    Result.

async_start_test_() ->
    {foreach,
    fun setup/0,
    fun teardown/1,
    [fun(Node) ->
            {timeout, 600000,
                ?_test(begin
                        ok = verify_async_start(Node),
                        ?assertEqual(false, is_stopped(Node)),
                        ?debugFmt("Node ~p asyncronously started .. checking is_ready", [Node]),
                        ?assertEqual(ok, rt_util:wait_until(Node, fun(Node1) -> is_ready(Node1) end))
                end)}
    end]}.

cookie_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [fun(Node) ->
              {timeout, 600000,
               ?_test(begin
                         ok = verify_sync_start(Node),
                         ?assertEqual(riak, cookie(Node))
                      end)}
      end]}.

load_configuration_test_() ->
    {foreach,
     fun () ->
        application:ensure_started(exec),
        {Result, Host} = rt_host:connect(localhost),
        ?assertEqual(ok, Result),
        Host
     end,
     fun(Host) -> rt_host:disconnect(Host) end,
     [fun(Host) ->
        ?_test(begin
            DirOverlay = create_directory_overlay({devrel, ?TEST_ROOT_PATH}, ?TEST_VERSION, 1),
            {Result, Config} = load_configuration(?TEST_VERSION, Host, DirOverlay),

            ?assertEqual(ok, Result),
            ?assertEqual(true, is_list(Config))
        end)
     end]}.

plan_commit_join_test_() ->
    {foreach,
     fun() -> multi_node_setup(3) end,
     fun teardown/1,
     [fun(Nodes) ->
              {timeout, 600000,
               ?_test(begin
                          %% Start all of the nodes ...
                          StartResults = rt_util:pmap(fun(Node) -> rt_riak_node:start(Node) end, Nodes),
                          lists:map(fun(Result) -> ?assertEqual(ok, Result) end, StartResults),
                          
                          %% Split the list into a leader and followers ...
                          [Leader|Followers] = Nodes,
                         
                          %% Join the followers to the leader ...
                          JoinResults = rt_util:pmap(fun(Follower) -> rt_riak_node:join(Follower, Leader) end, Followers),
                          lists:map(fun(Result) -> ?assertEqual(ok, Result) end, JoinResults),
                          ?debugFmt("Joined nodes ~p to ~p with result ~p", [Followers, Leader, JoinResults]),
                          ?assertEqual([ok, ok], JoinResults),

                          %% Plan the cluster on the leader ...
                          PlanResult = plan(Leader),
                          ?debugFmt("Plan result: ~p", [PlanResult]),
                          ?assertEqual(ok, PlanResult),
                          
                          %% Commit the cluster changes on the leader ...
                          CommitResult = commit(Leader),
                          ?assertEqual(ok, CommitResult)
                      end)}
      end]}.
                          

sync_start_stop_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [fun(Node) ->
        {timeout, 600000,
         ?_test(begin
            ok = verify_sync_start(Node),
            ok = verify_sync_stop(Node)
         end)}
      end]}.

wait_until_pingable_test_() ->
     {foreach,
      fun setup/0,
      fun teardown/1,
      [fun(Node) ->
               {timeout, 600000,
                ?_test(begin
                           ok = verify_sync_start(Node),
                           ?assertEqual(ok, wait_until_pingable(Node))
                end)}
       end,
       fun(Node) ->
               {timeout, 600000,
                ?_test(begin
                           ok = verify_async_start(Node),
                           ?assertEqual(ok, wait_until_pingable(Node))
                       end)}
       end]}.

wait_until_unpingable_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [fun(Node) ->
              {timeout, 600000,
               ?_test(begin
                         ok = verify_sync_start(Node),
                         Result = stop(Node),
                         ?assertEqual(ok, Result),
                         WaitResult = wait_until_unpingable(Node),
                         ?assertEqual(ok, WaitResult)
                     end)}
      end,
      fun(Node) ->
              {timeout, 600000,
               ?_test(begin
                          ok = verify_sync_start(Node),
                          Result = stop(Node, false),
                          ?assertEqual(ok, Result),
                          WaitResult = wait_until_unpingable(Node),
                          ?assertEqual(ok, WaitResult)
                      end)}
      end]}.
              
version_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [fun(Node) -> {"get_version", ?_assertEqual(?TEST_VERSION, version(Node))} end]}.

-endif.
