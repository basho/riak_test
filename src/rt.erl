%% @doc
%% Implements the base `riak_test' API, providing the ability to control
%% nodes in a Riak cluster as well as perform commonly reused operations.
%% Please extend this module with new functions that prove useful between
%% multiple independent tests.
-module(rt).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-export([deploy_nodes/1,
         start/1,
         stop/1,
         join/2,
         leave/1,
         wait_until_pingable/1,
         wait_until_unpingable/1,
         wait_until_ready/1,
         wait_until_no_pending_changes/1,
         wait_until_nodes_ready/1,
         remove/2,
         down/2,
         check_singleton_node/1,
         owners_according_to/1,
         members_according_to/1,
         status_of_according_to/2,
         claimant_according_to/1,
         wait_until_all_members/1,
         wait_until_all_members/2,
         wait_until_ring_converged/1]).

-export([setup_harness/2,
         cleanup_harness/0,
         load_config/1,
         set_config/2,
         config/1,
         config/2
        ]).

-define(HARNESS, (rt:config(rt_harness))).

%% @doc Rewrite the given node's app.config file, overriding the varialbes
%%      in the existing app.config with those in Config.
update_app_config(Node, Config) ->
    stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    ?HARNESS:update_app_config(Node, Config),
    start(Node).

%% @doc Deploy a set of freshly installed Riak nodes, returning a list of the
%%      nodes deployed.
-spec deploy_nodes(NumNodes :: integer()) -> [node()].
deploy_nodes(NumNodes) ->
    ?HARNESS:deploy_nodes(NumNodes).

%% @doc Start the specified Riak node
start(Node) ->
    ?HARNESS:start(Node).

async_start(Node) ->
    spawn(fun() -> start(Node) end).

%% @doc Stop the specified Riak node
stop(Node) ->
    ?HARNESS:stop(Node).

%% @doc Have `Node' send a join request to `PNode'
join(Node, PNode) ->
    R = rpc:call(Node, riak_core, join, [PNode]),
    lager:debug("[join] ~p to (~p): ~p", [Node, PNode, R]),
%%    wait_until_ready(Node),
    ?assertEqual(ok, R),
    ok.

%% @doc Have the specified node leave the cluster
leave(Node) ->
    R = rpc:call(Node, riak_core, leave, []),
    lager:debug("[leave] ~p: ~p", [Node, R]),
    ?assertEqual(ok, R),
    ok.

%% @doc Have `Node' remove `OtherNode' from the cluster
remove(Node, OtherNode) ->
    ?assertEqual(ok,
                 rpc:call(Node, riak_kv_console, remove, [[atom_to_list(OtherNode)]])).

%% @doc Have `Node' mark `OtherNode' as down
down(Node, OtherNode) ->
    rpc:call(Node, riak_kv_console, down, [[atom_to_list(OtherNode)]]).

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

%% @private
setup_harness(Test, Args) ->
    ?HARNESS:setup_harness(Test, Args).

%% @private
cleanup_harness() ->
    ?HARNESS:cleanup_harness().

%% @private
load_config(File) ->
    case file:consult(File) of
        {ok, Terms} ->
            [set_config(Key, Value) || {Key, Value} <- Terms],
            ok;
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

%% @doc
%% Safely construct a `NumNode' size cluster and return a list of the
%% deployed nodes.
build_cluster(NumNodes) ->
    %% Deploy a set of new nodes
    Nodes = deploy_nodes(NumNodes),

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

connection_info(Nodes) ->
    [begin
         {ok, PB_IP} = rpc:call(Node, application, get_env, [riak_kv, pb_ip]),
         {ok, PB_Port} = rpc:call(Node, application, get_env, [riak_kv, pb_port]),
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
