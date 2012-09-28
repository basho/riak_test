%% @doc
%% Implements the base `riak_test' API, providing the ability to control
%% nodes in a Riak cluster as well as perform commonly reused operations.
%% Please extend this module with new functions that prove useful between
%% multiple independent tests.
-module(rt).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-export([deploy_nodes/1,
         deploy_nodes/2,
         build_cluster/1,
         build_cluster/2,
         start/1,
         stop/1,
         join/2,
         leave/1,
         get_os_env/1,
         get_os_env/2,
         get_ring/1,
         admin/2,
         upgrade/2,
         wait_until_pingable/1,
         wait_until_unpingable/1,
         wait_until_ready/1,
         wait_until_no_pending_changes/1,
         wait_until_nodes_ready/1,
         wait_until/2,
         remove/2,
         down/2,
         check_singleton_node/1,
         owners_according_to/1,
         members_according_to/1,
         status_of_according_to/2,
         claimant_according_to/1,
         wait_until_all_members/1,
         wait_until_all_members/2,
         wait_until_legacy_ringready/1,
         wait_until_ring_converged/1]).

%% Search API
-export([enable_search_hook/2]).

-export([cleanup_harness/0,
         load_config/1,
         set_config/2,
         setup_harness/2,
         teardown/0,
         config/1,
         config/2
        ]).

-define(HARNESS, (rt:config(rt_harness))).

get_os_env(Var) ->
    case get_os_env(Var, undefined) of
        undefined -> exit({os_env_var_undefined, Var});
        Value -> Value
    end.

get_os_env(Var, Default) ->
    case os:getenv(Var) of
        false -> Default;
        Value -> Value
    end.

%% @doc Get the raw ring for the given `Node'.
get_ring(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Ring.

%% @doc Rewrite the given node's app.config file, overriding the varialbes
%%      in the existing app.config with those in Config.
update_app_config(Node, Config) ->
    stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    ?HARNESS:update_app_config(Node, Config),
    start(Node).

%% @doc Deploy a set of freshly installed Riak nodes, returning a list of the
%%      nodes deployed.
%% @todo Re-add -spec after adding multi-version support
deploy_nodes(Versions) when is_list(Versions) ->
    NodeConfig = lists:map(fun({Vsn,Config}) ->
                                   {Vsn, Config};
                              (Vsn) ->
                                   {Vsn, default}
                           end, Versions),
    ?HARNESS:deploy_nodes(NodeConfig);
deploy_nodes(NumNodes) ->
    deploy_nodes(NumNodes, default).

%% @doc Deploy a set of freshly installed Riak nodes with the given
%%      `InitialConfig', returning a list of the nodes deployed.
-spec deploy_nodes(NumNodes :: integer(), any()) -> [node()].
deploy_nodes(NumNodes, InitialConfig) ->
    NodeConfig = [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)],
    ?HARNESS:deploy_nodes(NodeConfig).

%% @doc Start the specified Riak node
start(Node) ->
    ?HARNESS:start(Node).

async_start(Node) ->
    spawn(fun() -> start(Node) end).

%% @doc Stop the specified Riak node
stop(Node) ->
    ?HARNESS:stop(Node).

%% @doc Upgrade a Riak node to a specific version
upgrade(Node, NewVersion) ->
    ?HARNESS:upgrade(Node, NewVersion).

%% @doc Upgrade a Riak node to a specific version using the alternate
%%      leave/upgrade/rejoin approach
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
    R = try_join(Node, PNode),
    lager:debug("[join] ~p to (~p): ~p", [Node, PNode, R]),
%%    wait_until_ready(Node),
    ?assertEqual(ok, R),
    ok.

try_join(Node, PNode) ->
    case rpc:call(Node, riak_core, join, [PNode]) of
        {badrpc, _} ->
            rpc:call(Node, riak, join, [PNode]);
        Result ->
            Result
    end.

%% @doc Have the specified node leave the cluster
leave(Node) ->
    R = try_leave(Node),
    lager:debug("[leave] ~p: ~p", [Node, R]),
    ?assertEqual(ok, R),
    ok.

try_leave(Node) ->
    case rpc:call(Node, riak_core, leave, []) of
        {badrpc, _} ->
            rpc:call(Node, riak_kv_console, leave, [[]]),
            ok;
        Result ->
            Result
    end.

admin(Node, Args) ->
    ?HARNESS:admin(Node, Args).

%% @doc Have `Node' remove `OtherNode' from the cluster
remove(Node, OtherNode) ->
    ?assertEqual(ok,
                 rpc:call(Node, riak_kv_console, remove, [[atom_to_list(OtherNode)]])).

%% @doc Have `Node' mark `OtherNode' as down
down(Node, OtherNode) ->
    rpc:call(Node, riak_kv_console, down, [[atom_to_list(OtherNode)]]).

%% @doc Spawn `Cmd' on the machine running the test harness
spawn_cmd(Cmd) ->
    ?HARNESS:spawn_cmd(Cmd).

%% @doc Wait for a command spawned by `spawn_cmd', returning
%%      the exit status and result
wait_for_cmd(CmdHandle) ->
    ?HARNESS:wait_for_cmd(CmdHandle).

%% @doc Spawn `Cmd' on the machine running the test harness, returning
%%      the exit status and result
cmd(Cmd) ->
    ?HARNESS:cmd(Cmd).

%% @doc Ensure that the specified node is a singleton node/cluster -- a node
%%      that owns 100% of the ring.
check_singleton_node(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Owners = lists:usort([Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)]),
    ?assertEqual([Node], Owners),
    ok.

%% @doc Wait until the specified node is considered ready by `riak_core'.
%%      As of Riak 1.0, a node is ready if it is in the `valid' or `leaving'
%%      states. A ready node is guaranteed to have current preflist/ownership
%%      information.
wait_until_ready(Node) ->
    ?assertEqual(ok, wait_until(Node, fun is_ready/1)),
    ok.

%% @doc Given a list of nodes, wait until all nodes believe there are no
%% on-going or pending ownership transfers.
-spec wait_until_no_pending_changes([node()]) -> ok | fail.
wait_until_no_pending_changes(Nodes) ->
    F = fun(Node) ->
                [rpc:call(NN, riak_core_vnode_manager, force_handoffs, [])
                 || NN <- Nodes],
                {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
                riak_core_ring:pending_changes(Ring) =:= []
        end,
    [?assertEqual(ok, wait_until(Node, F)) || Node <- Nodes],
    ok.

%% @private
are_no_pending(Node) ->
    rpc:call(Node, riak_core_vnode_manager, force_handoffs, []),
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    riak_core_ring:pending_changes(Ring) =:= [].

wait_for_service(Node, Service) ->
    F = fun(N) ->
                Services = rpc:call(N, riak_core_node_watcher, services, []),
                lists:member(Service, Services)
        end,
    ?assertEqual(ok, wait_until(Node, F)),
    ok.

wait_for_cluster_service(Nodes, Service) ->
    F = fun(N) ->
                UpNodes = rpc:call(N, riak_core_node_watcher, nodes, [Service]),
                (Nodes -- UpNodes) == []
        end,
    [?assertEqual(ok, wait_until(Node, F)) || Node <- Nodes],
    ok.

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
owners_according_to(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Owners = [Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)],
    lists:usort(Owners).

%% @doc Return a list of cluster members according to the ring retrieved from
%%      the specified node.
members_according_to(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Members = riak_core_ring:all_members(Ring),
    Members.

%% @doc Return the cluster status of `Member' according to the ring
%%      retrieved from `Node'.
status_of_according_to(Member, Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Status = riak_core_ring:member_status(Ring, Member),
    Status.

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
claimant_according_to(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Claimant = riak_core_ring:claimant(Ring),
    Claimant.

%% @doc Given a list of nodes, wait until all nodes are considered ready.
%%      See {@link wait_until_ready/1} for definition of ready.
wait_until_nodes_ready(Nodes) ->
    [?assertEqual(ok, wait_until(Node, fun is_ready/1)) || Node <- Nodes],
    ok.

%% @private
is_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            lists:member(Node, riak_core_ring:ready_members(Ring));
        _ ->
            false
    end.

%% @doc Wait until all nodes in the list `Nodes' believe each other to be
%%      members of the cluster.
wait_until_all_members(Nodes) ->
    wait_until_all_members(Nodes, Nodes).

%% @doc Wait until all nodes in the list `Nodes' believes all nodes in the
%%      list `Members' are members of the cluster.
wait_until_all_members(Nodes, Members) ->
    S1 = ordsets:from_list(Members),
    F = fun(Node) ->
                S2 = ordsets:from_list(members_according_to(Node)),
                ordsets:is_subset(S1, S2)
        end,
    [?assertEqual(ok, wait_until(Node, F)) || Node <- Nodes],
    ok.

%% @doc Given a list of nodes, wait until all nodes believe the ring has
%%      converged (ie. `riak_core_ring:is_ready' returns `true').
wait_until_ring_converged(Nodes) ->
    [?assertEqual(ok, wait_until(Node, fun is_ring_ready/1)) || Node <- Nodes],
    ok.

wait_until_legacy_ringready(Node) ->
    rt:wait_until(Node,
                  fun(_) ->
                          case rpc:call(Node, riak_kv_status, ringready, []) of
                              {ok, _Nodes} ->
                                  true;
                              _ ->
                                  false
                          end
                  end).

%% @private
is_ring_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            riak_core_ring:ring_ready(Ring);
        _ ->
            false
    end.

%% @doc Wait until the specified node is pingable
wait_until_pingable(Node) ->
    F = fun(N) ->
                net_adm:ping(N) =:= pong
        end,
    ?assertEqual(ok, wait_until(Node, F)),
    ok.

%% @doc Wait until the specified node is no longer pingable
wait_until_unpingable(Node) ->
    F = fun(N) ->
                net_adm:ping(N) =:= pang
        end,
    ?assertEqual(ok, wait_until(Node, F)),
    ok.

%% @doc Utility function used to construct test predicates. Retries the
%%      function `Fun' until it returns `true', or until the maximum
%%      number of retries is reached. The retry limit is based on the
%%      provided `rt_max_wait_time' and `rt_retry_delay' parameters in
%%      specified `riak_test' config file.
wait_until(Node, Fun) ->
    MaxTime = rt:config(rt_max_wait_time),
    Delay = rt:config(rt_retry_delay),
    Retry = MaxTime div Delay,
    wait_until(Node, Fun, Retry, Delay).

%% @deprecated Use {@link wait_until/2} instead.
wait_until(Node, Fun, Retry) ->
    wait_until(Node, Fun, Retry, 500).

%% @deprecated Use {@link wait_until/2} instead.
wait_until(Node, Fun, Retry, Delay) ->
    Pass = Fun(Node),
    case {Retry, Pass} of
        {_, true} ->
            ok;
        {0, _} ->
            fail;
        _ ->
            timer:sleep(Delay),
            wait_until(Node, Fun, Retry-1)
    end.

capability(Node, Capability) ->
    rpc:call(Node, riak_core_capability, get, [Capability]).

wait_until_capability(Node, Capability, Value) ->
    rt:wait_until(Node,
                  fun(_) ->
                          Value == capability(Node, Capability)
                  end).

%%%===================================================================
%%% Search
%%%===================================================================

%% doc Enable the search KV hook for the given `Bucket'.  Any `Node'
%%     in the cluster may be used as the change is propagated via the
%%     Ring.
enable_search_hook(Node, Bucket) when is_binary(Bucket) ->
    ?assertEqual(ok, rpc:call(Node, riak_search_kv_hook, install, [Bucket])).

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
setup_harness(Test, Args) ->
    ?HARNESS:setup_harness(Test, Args).

%% @private
cleanup_harness() ->
    ?HARNESS:cleanup_harness().

load_config(ConfigName) ->
    case load_config_file(ConfigName) of
        ok -> ok;
        {error, enoent} -> load_dot_config(ConfigName)
    end.

%% @private
load_dot_config(ConfigName) ->
    case file:consult(filename:join([os:getenv("HOME"), ".riak_test.config"])) of
        {ok, Terms} ->
            Config = proplists:get_value(list_to_atom(ConfigName), Terms),
            [set_config(Key, Value) || {Key, Value} <- Config],
            ok;            
        {error, Reason} ->
            erlang:error("Failed to parse config file", ["~/.riak_test.config", Reason])
 end.
%% @private
load_config_file(File) ->
    case file:consult(File) of
        {ok, Terms} ->
            [set_config(Key, Value) || {Key, Value} <- Terms],
            ok;
        {error, enoent} ->
            {error, enoent};            
        {error, Reason} ->
            erlang:error("Failed to parse config file", [File, Reason])
    end.

%% @private
set_config(Key, Value) ->
    ok = application:set_env(riak_test, Key, Value).

%% @private
config(Key) ->
    case application:get_env(riak_test, Key) of
        {ok, Value} ->
            Value;
        undefined ->
            erlang:error("Missing configuration key", [Key])
    end.

%% @private
config(Key, Default) ->
    case application:get_env(riak_test, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.

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

    %% Ensure each node owns 100% of it's own ring
    [?assertEqual([Node], owners_according_to(Node)) || Node <- Nodes],

    %% Join nodes
    [Node1|OtherNodes] = Nodes,
    [join(Node, Node1) || Node <- OtherNodes],

    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)),

    %% Ensure each node owns a portion of the ring
    [?assertEqual(Nodes, owners_according_to(Node)) || Node <- Nodes],
    lager:info("Cluster built: ~p", [Nodes]),
    Nodes.

%% Helper that returns first successful application get_env result,
%% used when different versions of Riak use different app vars for
%% the same setting.
rpc_get_env(_, []) ->
    undefined;
rpc_get_env(Node, [{App,Var}|Others]) ->
    case rpc:call(Node, application, get_env, [App, Var]) of
        {ok, Value} ->
            {ok, Value};
        _ ->
            rpc_get_env(Node, Others)
    end.

-type interface() :: {http, tuple()} | {pb, tuple()}.
-type interfaces() :: [interface()].
-type conn_info() :: [{node(), interfaces()}].

-spec connection_info([node()]) -> conn_info().
connection_info(Nodes) ->
    [begin
         {ok, PB_IP} = rpc_get_env(Node, [{riak_api, pb_ip},
                                          {riak_kv, pb_ip}]),
         {ok, PB_Port} = rpc_get_env(Node, [{riak_api, pb_port},
                                            {riak_kv, pb_port}]),
         {ok, [{HTTP_IP, HTTP_Port}]} =
             rpc:call(Node, application, get_env, [riak_core, http]),
         {Node, [{http, {HTTP_IP, HTTP_Port}}, {pb, {PB_IP, PB_Port}}]}
     end || Node <- Nodes].

http_url(Nodes) when is_list(Nodes) ->
    [begin
         {Host, Port} = orddict:fetch(http, Connections),
         lists:flatten(io_lib:format("http://~s:~b", [Host, Port]))
     end || {_Node, Connections} <- connection_info(Nodes)];
http_url(Node) ->
    hd(http_url([Node])).

systest_write(Node, Size) ->
    systest_write(Node, Size, 2).

systest_write(Node, Size, W) ->
    systest_write(Node, 1, Size, <<"systest">>, W).

systest_write(Node, Start, End, Bucket, W) ->
    {ok, C} = riak:client_connect(Node),
    F = fun(N, Acc) ->
                Obj = riak_object:new(Bucket, <<N:32/integer>>, <<N:32/integer>>),
                case C:put(Obj, W) of
                    ok ->
                        Acc;
                    Other ->
                        [{N, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

systest_read(Node, Size) ->
    systest_read(Node, Size, 2).

systest_read(Node, Size, R) ->
    systest_read(Node, 1, Size, <<"systest">>, R).

systest_read(Node, Start, End, Bucket, R) ->
    {ok, C} = riak:client_connect(Node),
    F = fun(N, Acc) ->
                case C:get(Bucket, <<N:32/integer>>, R) of
                    {ok, Obj} ->
                        case riak_object:get_value(Obj) of
                            <<N:32/integer>> ->
                                Acc;
                            WrongVal ->
                                [{N, {wrong_val, WrongVal}} | Acc]
                        end;
                    Other ->
                        [{N, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

%% @doc get me a protobuf client process and hold the mayo!
-spec pbc(node()) -> pid().
pbc(Node) ->
    {ok, IP} = rpc:call(Node, application, get_env, [riak_api, pb_ip]),
    {ok, PBPort} = rpc:call(Node, application, get_env, [riak_api, pb_port]),
    {ok, Pid} = riakc_pb_socket:start_link(IP, PBPort),
    Pid.

%% @doc does a read via the erlang protobuf client
-spec pbc_read(pid(), binary(), binary()) -> binary().
pbc_read(Pid, Bucket, Key) -> 
    {ok, Value} = riakc_pb_socket:get(Pid, Bucket, Key),
    Value.

%% @doc does a write via the erlang protobuf client
-spec pbc_write(pid(), binary(), binary(), binary()) -> atom().
pbc_write(Pid, Bucket, Key, Value) -> 
    Object = riakc_obj:new(Bucket, Key, Value),
    riakc_pb_socket:put(Pid, Object).

%% @doc sets a bucket property/properties via the erlang protobuf client
-spec pbc_set_bucket_prop(pid(), binary(), [proplists:property()]) -> atom().
pbc_set_bucket_prop(Pid, Bucket, PropList) ->
    riakc_pb_socket:set_bucket(Pid, Bucket, PropList).

%% @doc get me an http client.
-spec httpc(node()) -> term().
httpc(Node) ->
    {ok, [{IP, Port}|_]} = rpc:call(Node, application, get_env, [riak_core, http]),
    rhc:create(IP, Port, "riak", []).

%% @doc does a read via the http erlang client.
-spec httpc_read(term(), binary(), binary()) -> binary().
httpc_read(C, Bucket, Key) -> 
    {ok, Value} = rhc:get(C, Bucket, Key),
    Value.


%% @doc does a write via the http erlang client.
-spec httpc_write(term(), binary(), binary(), binary()) -> atom().
httpc_write(C, Bucket, Key, Value) -> 
    Object = riakc_obj:new(Bucket, Key, Value),
    rhc:put(C, Object).

%% utility function
pmap(F, L) ->
    Parent = self(),
    lists:foldl(
      fun(X, N) ->
              spawn(fun() ->
                            Parent ! {pmap, N, F(X)}
                    end),
              N+1
      end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

%% @doc if String contains Substr, return true.
-spec str(string(), string()) -> boolean().
str(String, Substr) ->
    case string:str(String, Substr) of
        0 -> false;
        _ -> true
    end.

%% @doc Sets the backend of ALL nodes that could be available to riak_test.
%%      this is not limited to the nodes under test, but any node that
%%      riak_test is able to find. It then queries each available node
%%      for it's backend, and returns it if they're all equal. If different
%%      nodes have different backends, it returns a list of backends.
%%      Currently, there is no way to request multiple backends, so the
%%      list return type should be considered an error.
-spec set_backend(atom()) -> atom()|[atom()].
set_backend(bitcask) ->
    set_backend(riak_kv_bitcask_backend);
set_backend(eleveldb) -> 
    set_backend(riak_kv_eleveldb_backend);
set_backend(memory) ->
    set_backend(riak_kv_memory_backend);
set_backend(Backend) when Backend == riak_kv_bitcask_backend; Backend == riak_kv_eleveldb_backend; Backend == riak_kv_memory_backend ->
    lager:info("rt:set_backend(~p)", [Backend]),
    ?HARNESS:set_backend(Backend);
set_backend(Other) ->
    lager:warning("rt:set_backend doesn't recognize ~p as a legit backend, using the default.", [Other]),
    ?HARNESS:get_backends().

%% @doc Gets the current version under test. In the case of an upgrade test
%%      or something like that, it's the version you're upgrading to. 
-spec get_version() -> binary().
get_version() ->
    ?HARNESS:get_version().

%% @doc Shutdown every node, this is for after a test run is complete.
teardown() ->
    ?HARNESS:teardown().

%% @doc outputs some useful information about nodes that are up
whats_up() ->
    ?HARNESS:whats_up().
