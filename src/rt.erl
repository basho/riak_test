%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-include("rt.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-export([
         capability/2,
         capability/3,
         check_ibrowse/0,
         cmd/1,
         cmd/2,
         connection_info/1,
         enable_search_hook/2,
         expect_in_log/2,
         get_deps/0,
         get_ip/1,
         get_node_logs/0,
         get_replica/5,
         get_version/0,
         is_mixed_cluster/1,
         is_pingable/1,
         load_modules_on_nodes/2,
         log_to_nodes/2,
         log_to_nodes/3,
         pmap/2,
         post_result/2,
         priv_dir/0,
         product/1,
         rpc_get_env/2,
         setup_harness/2,
         setup_log_capture/1,
         stream_cmd/1, stream_cmd/2,
         spawn_cmd/1,
         spawn_cmd/2,
         str/2,
         systest_read/2,
         systest_read/3,
         systest_read/5,
         systest_read/6,
         systest_write/2,
         systest_write/3,
         systest_write/5,
         systest_write/6,
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
         whats_up/0
        ]).

-define(HARNESS, (rt_config:get(rt_harness))).

priv_dir() ->
LocalPrivDir = "./priv",
%% XXX for some reason, codew:priv_dir returns riak_test/riak_test/priv,
%% which is wrong, so fix it.
DepPrivDir = re:replace(code:priv_dir(riak_test), "riak_test(/riak_test)*",
"riak_test", [{return, list}]),
PrivDir = case {filelib:is_dir(LocalPrivDir), filelib:is_dir(DepPrivDir)} of
{true, _} ->
    lager:debug("Local ./priv detected, using that..."),
    %% we want an absolute path!
    filename:absname(LocalPrivDir);
{false, true} ->
    lager:debug("riak_test dependency priv_dir detected, using that..."),
    DepPrivDir;
_ ->
    ?assertEqual({true, bad_priv_dir}, {false, bad_priv_dir})
end,

lager:info("priv dir: ~p -> ~p", [code:priv_dir(riak_test), PrivDir]),
?assert(filelib:is_dir(PrivDir)),
PrivDir.

%% @doc gets riak deps from the appropriate harness
-spec get_deps() -> list().
get_deps() -> ?HARNESS:get_deps().

%% @doc if String contains Substr, return true.
-spec str(string(), string()) -> boolean().
str(String, Substr) ->
case string:str(String, Substr) of
0 -> false;
_ -> true
end.

%% @doc Helper that returns first successful application get_env result,
%%      used when different versions of Riak use different app vars for
%%      the same setting.
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

-spec connection_info(node() | [node()]) -> interfaces() | conn_info().
connection_info(Node) when is_atom(Node) ->
    {ok, [{PB_IP, PB_Port}]} = rt_pb:get_pb_conn_info(Node),
    {ok, [{HTTP_IP, HTTP_Port}]} = rt_http:get_http_conn_info(Node),
    case rt_http:get_https_conn_info(Node) of
        undefined ->
            [{http, {HTTP_IP, HTTP_Port}}, {pb, {PB_IP, PB_Port}}];
        {ok, [{HTTPS_IP, HTTPS_Port}]} ->
            [{http, {HTTP_IP, HTTP_Port}}, {https, {HTTPS_IP, HTTPS_Port}}, {pb, {PB_IP, PB_Port}}]
    end;
connection_info(Nodes) when is_list(Nodes) ->
[ {Node, connection_info(Node)} || Node <- Nodes].

maybe_wait_for_changes(Node) ->
    Ring = rt_ring:get_ring(Node),
    Changes = riak_core_ring:pending_changes(Ring),
    Joining = riak_core_ring:members(Ring, [joining]),
    lager:info("maybe_wait_for_changes, changes: ~p joining: ~p",
               [Changes, Joining]),
    if Changes =:= [] ->
            ok;
       Joining =/= [] ->
            ok;
       true ->
            ok = wait_until_no_pending_changes([Node])
    end.

%% @doc Spawn `Cmd' on the machine running the test harness
spawn_cmd(Cmd) ->
?HARNESS:spawn_cmd(Cmd).

%% @doc Spawn `Cmd' on the machine running the test harness
spawn_cmd(Cmd, Opts) ->
?HARNESS:spawn_cmd(Cmd, Opts).

%% @doc Wait for a command spawned by `spawn_cmd', returning
%%      the exit status and result
wait_for_cmd(CmdHandle) ->
?HARNESS:wait_for_cmd(CmdHandle).

%% @doc Spawn `Cmd' on the machine running the test harness, returning
%%      the exit status and result
cmd(Cmd) ->
?HARNESS:cmd(Cmd).

%% @doc Spawn `Cmd' on the machine running the test harness, returning
%%      the exit status and result
cmd(Cmd, Opts) ->
?HARNESS:cmd(Cmd, Opts).

%% @doc pretty much the same as os:cmd/1 but it will stream the output to lager.
%%      If you're running a long running command, it will dump the output
%%      once per second, as to not create the impression that nothing is happening.
-spec stream_cmd(string()) -> {integer(), string()}.
stream_cmd(Cmd) ->
Port = open_port({spawn, binary_to_list(iolist_to_binary(Cmd))}, [stream, stderr_to_stdout, exit_status]),
stream_cmd_loop(Port, "", "", now()).

%% @doc same as rt:stream_cmd/1, but with options, like open_port/2
-spec stream_cmd(string(), string()) -> {integer(), string()}.
stream_cmd(Cmd, Opts) ->
Port = open_port({spawn, binary_to_list(iolist_to_binary(Cmd))}, [stream, stderr_to_stdout, exit_status] ++ Opts),
stream_cmd_loop(Port, "", "", now()).

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
after rt:config(rt_max_wait_time) ->
    {-1, Buffer}
end.

%%%===================================================================
%%% Remote code management
%%%===================================================================
load_modules_on_nodes([], Nodes)
when is_list(Nodes) ->
ok;
load_modules_on_nodes([Module | MoreModules], Nodes)
when is_list(Nodes) ->
case code:get_object_code(Module) of
{Module, Bin, File} ->
    {_, []} = rpc:multicall(Nodes, code, load_binary, [Module, File, Bin]);
error ->
    error(lists:flatten(io_lib:format("unable to get_object_code(~s)", [Module])))
end,
load_modules_on_nodes(MoreModules, Nodes).


%%%===================================================================
%%% Status / Wait Functions
%%%===================================================================

%% @doc Is the `Node' up according to net_adm:ping
is_pingable(Node) ->
net_adm:ping(Node) =:= pong.

is_mixed_cluster(Nodes) when is_list(Nodes) ->
%% If the nodes are bad, we don't care what version they are
{Versions, _BadNodes} = rpc:multicall(Nodes, init, script_id, [], rt_config:get(rt_max_wait_time)),
length(lists:usort(Versions)) > 1;
is_mixed_cluster(Node) ->
Nodes = rpc:call(Node, erlang, nodes, []),
is_mixed_cluster(Nodes).

%% @private
is_ready(Node) ->
case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
{ok, Ring} ->
    case lists:member(Node, riak_core_ring:ready_members(Ring)) of
	true -> true;
	false -> {not_ready, Node}
    end;
Other ->
    Other
end.

%% @private
is_ring_ready(Node) ->
case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
{ok, Ring} ->
    riak_core_ring:ring_ready(Ring);
_ ->
    false
end.

%% @doc Utility function used to construct test predicates. Retries the
%%      function `Fun' until it returns `true', or until the maximum
%%      number of retries is reached. The retry limit is based on the
%%      provided `rt_max_wait_time' and `rt_retry_delay' parameters in
%%      specified `riak_test' config file.
wait_until(Fun) when is_function(Fun) ->
MaxTime = rt_config:get(rt_max_wait_time),
Delay = rt_config:get(rt_retry_delay),
Retry = MaxTime div Delay,
wait_until(Fun, Retry, Delay).

%% @doc Convenience wrapper for wait_until for the myriad functions that
%% take a node as single argument.
wait_until(Node, Fun) when is_atom(Node), is_function(Fun) ->
wait_until(fun() -> Fun(Node) end).

%% @doc Retry `Fun' until it returns `Retry' times, waiting `Delay'
%% milliseconds between retries. This is our eventual consistency bread
%% and butter
wait_until(Fun, Retry, Delay) when Retry > 0 ->
Res = Fun(),
case Res of
true ->
    ok;
_ when Retry == 1 ->
    {fail, Res};
_ ->
    timer:sleep(Delay),
    wait_until(Fun, Retry-1, Delay)
end.

%% @doc Wait until the specified node is considered ready by `riak_core'.
%%      As of Riak 1.0, a node is ready if it is in the `valid' or `leaving'
%%      states. A ready node is guaranteed to have current preflist/ownership
%%      information.
wait_until_ready(Node) ->
lager:info("Wait until ~p ready", [Node]),
?assertEqual(ok, wait_until(Node, fun is_ready/1)),
ok.

%% @doc Wait until status can be read from riak_kv_console
wait_until_status_ready(Node) ->
lager:info("Wait until status ready in ~p", [Node]),
?assertEqual(ok, wait_until(Node,
			fun(_) ->
				case rpc:call(Node, riak_kv_console, status, [[]]) of
				    ok ->
					true;
				    Res ->
					Res
				end
			end)).

%% @doc Given a list of nodes, wait until all nodes believe there are no
%% on-going or pending ownership transfers.
-spec wait_until_no_pending_changes([node()]) -> ok | fail.
wait_until_no_pending_changes(Nodes) ->
lager:info("Wait until no pending changes on ~p", [Nodes]),
F = fun() ->
	rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
	{Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
	Changes = [ riak_core_ring:pending_changes(Ring) =:= [] || {ok, Ring} <- Rings ],
	BadNodes =:= [] andalso length(Changes) =:= length(Nodes) andalso lists:all(fun(T) -> T end, Changes)
end,
?assertEqual(ok, wait_until(F)),
ok.

%% @doc Waits until no transfers are in-flight or pending, checked by
%% riak_core_status:transfers().
-spec wait_until_transfers_complete([node()]) -> ok | fail.
wait_until_transfers_complete([Node0|_]) ->
lager:info("Wait until transfers complete ~p", [Node0]),
F = fun(Node) ->
	{DownNodes, Transfers} = rpc:call(Node, riak_core_status, transfers, []),
	DownNodes =:= [] andalso Transfers =:= []
end,
?assertEqual(ok, wait_until(Node0, F)),
ok.

wait_for_service(Node, Services) when is_list(Services) ->
F = fun(N) ->
	case rpc:call(N, riak_core_node_watcher, services, [N]) of
	    {badrpc, Error} ->
		{badrpc, Error};
	    CurrServices when is_list(CurrServices) ->
		lists:all(fun(Service) -> lists:member(Service, CurrServices) end, Services);
	    Res ->
		Res
	end
end,
?assertEqual(ok, wait_until(Node, F)),
ok;
wait_for_service(Node, Service) ->
wait_for_service(Node, [Service]).

wait_for_cluster_service(Nodes, Service) ->
lager:info("Wait for cluster service ~p in ~p", [Service, Nodes]),
F = fun(N) ->
	UpNodes = rpc:call(N, riak_core_node_watcher, nodes, [Service]),
	(Nodes -- UpNodes) == []
end,
[?assertEqual(ok, wait_until(Node, F)) || Node <- Nodes],
ok.

%% @doc Given a list of nodes, wait until all nodes are considered ready.
%%      See {@link wait_until_ready/1} for definition of ready.
wait_until_nodes_ready(Nodes) ->
lager:info("Wait until nodes are ready : ~p", [Nodes]),
[?assertEqual(ok, wait_until(Node, fun is_ready/1)) || Node <- Nodes],
ok.

%% @doc Wait until all nodes in the list `Nodes' believe each other to be
%%      members of the cluster.
wait_until_all_members(Nodes) ->
wait_until_all_members(Nodes, Nodes).

%% @doc Wait until all nodes in the list `Nodes' believes all nodes in the
%%      list `Members' are members of the cluster.
wait_until_all_members(Nodes, ExpectedMembers) ->
    lager:info("Wait until all members ~p ~p", [Nodes, ExpectedMembers]),
    S1 = ordsets:from_list(ExpectedMembers),
    F = fun(Node) ->
                case rt_ring:members_according_to(Node) of
                    {badrpc, _} ->
                        false;
                    ReportedMembers ->
                        S2 = ordsets:from_list(ReportedMembers),
                        ordsets:is_subset(S1, S2)
                end
        end,
    [?assertEqual(ok, wait_until(Node, F)) || Node <- Nodes],
    ok.

%% @doc Given a list of nodes, wait until all nodes believe the ring has
%%      converged (ie. `riak_core_ring:is_ready' returns `true').
wait_until_ring_converged(Nodes) ->
lager:info("Wait until ring converged on ~p", [Nodes]),
[?assertEqual(ok, wait_until(Node, fun is_ring_ready/1)) || Node <- Nodes],
ok.

wait_until_legacy_ringready(Node) ->
lager:info("Wait until legacy ring ready on ~p", [Node]),
rt:wait_until(Node,
	  fun(_) ->
		  case rpc:call(Node, riak_kv_status, ringready, []) of
		      {ok, _Nodes} ->
			  true;
		      Res ->
			  Res
		  end
	  end).

%% @doc wait until each node in Nodes is disterl connected to each.
wait_until_connected(Nodes) ->
lager:info("Wait until connected ~p", [Nodes]),
F = fun(Node) ->
	Connected = rpc:call(Node, erlang, nodes, []),
	lists:sort(Nodes) == lists:sort([Node]++Connected)--[node()]
end,
[?assertEqual(ok, wait_until(Node, F)) || Node <- Nodes],
ok.

%% @doc Wait until the specified node is pingable
wait_until_pingable(Node) ->
lager:info("Wait until ~p is pingable", [Node]),
F = fun(N) ->
	net_adm:ping(N) =:= pong
end,
?assertEqual(ok, wait_until(Node, F)),
ok.

%% @doc Wait until the specified node is no longer pingable
wait_until_unpingable(Node) ->
lager:info("Wait until ~p is not pingable", [Node]),
_OSPidToKill = rpc:call(Node, os, getpid, []),
F = fun() -> net_adm:ping(Node) =:= pang end,
%% riak stop will kill -9 after 5 mins, so we try to wait at least that
%% amount of time.
Delay = rt_config:get(rt_retry_delay),
Retry = lists:max([360000, rt_config:get(rt_max_wait_time)]) div Delay,
case wait_until(F, Retry, Delay) of
ok -> ok;
_ ->
    lager:error("Timed out waiting for node ~p to shutdown", [Node]),
    ?assert(node_shutdown_timed_out)
end.


% Waits until a certain registered name pops up on the remote node.
wait_until_registered(Node, Name) ->
lager:info("Wait until ~p is up on ~p", [Name, Node]),

F = fun() ->
	Registered = rpc:call(Node, erlang, registered, []),
	lists:member(Name, Registered)
end,
case wait_until(F) of
ok ->
    ok;
_ ->
    lager:info("The server with the name ~p on ~p is not coming up.",
	       [Name, Node]),
    ?assert(registered_name_timed_out)
end.


%% Waits until the cluster actually detects that it is partitioned.
wait_until_partitioned(P1, P2) ->
lager:info("Waiting until partition acknowledged: ~p ~p", [P1, P2]),
[ begin
  lager:info("Waiting for ~p to be partitioned from ~p", [Node, P2]),
  wait_until(fun() -> is_partitioned(Node, P2) end)
end || Node <- P1 ],
[ begin
  lager:info("Waiting for ~p to be partitioned from ~p", [Node, P1]),
  wait_until(fun() -> is_partitioned(Node, P1) end)
end || Node <- P2 ].

is_partitioned(Node, Peers) ->
AvailableNodes = rpc:call(Node, riak_core_node_watcher, nodes, [riak_kv]),
lists:all(fun(Peer) -> not lists:member(Peer, AvailableNodes) end, Peers).

capability(Node, all) ->
rpc:call(Node, riak_core_capability, all, []);
capability(Node, Capability) ->
rpc:call(Node, riak_core_capability, get, [Capability]).

capability(Node, Capability, Default) ->
rpc:call(Node, riak_core_capability, get, [Capability, Default]).

wait_until_capability(Node, Capability, Value) ->
rt:wait_until(Node,
	  fun(_) ->
		  cap_equal(Value, capability(Node, Capability))
	  end).

wait_until_capability(Node, Capability, Value, Default) ->
rt:wait_until(Node,
	  fun(_) ->
		  Cap = capability(Node, Capability, Default),
	io:format("capability is ~p ~p",[Node, Cap]),
		  cap_equal(Value, Cap)
	  end).

cap_equal(Val, Cap) when is_list(Cap) ->
lists:sort(Cap) == lists:sort(Val);
cap_equal(Val, Cap) ->
Val == Cap.

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

%% AAE support
wait_until_aae_trees_built(Nodes) ->
lager:info("Wait until AAE builds all partition trees across ~p", [Nodes]),
BuiltFun = fun() -> lists:foldl(aae_tree_built_fun(), true, Nodes) end,
?assertEqual(ok, wait_until(BuiltFun)),
ok.

aae_tree_built_fun() ->
fun(Node, _AllBuilt = true) ->
    case get_aae_tree_info(Node) of
	{ok, TreeInfos} ->
	    case all_trees_have_build_times(TreeInfos) of
		true ->
		    Partitions = [I || {I, _} <- TreeInfos],
		    all_aae_trees_built(Node, Partitions);
		false ->
		    some_trees_not_built
	    end;
	Err ->
	    Err
    end;
(_Node, Err) ->
    Err
end.

% It is unlikely but possible to get a tree built time from compute_tree_info
% but an attempt to use the tree returns not_built. This is because the build
% process has finished, but the lock on the tree won't be released until it
% dies and the manager detects it. Yes, this is super freaking paranoid.
all_aae_trees_built(Node, Partitions) ->
%% Notice that the process locking is spawned by the
%% pmap. That's important! as it should die eventually
%% so the lock is released and the test can lock the tree.
IndexBuilts = rt:pmap(index_built_fun(Node), Partitions),
BadOnes = [R || R <- IndexBuilts, R /= true],
case BadOnes of
[] ->
    true;
_ ->
    BadOnes
end.

get_aae_tree_info(Node) ->
case rpc:call(Node, riak_kv_entropy_info, compute_tree_info, []) of
{badrpc, _} ->
    {error, {badrpc, Node}};
Info  ->
    lager:debug("Entropy table on node ~p : ~p", [Node, Info]),
    {ok, Info}
end.

all_trees_have_build_times(Info) ->
not lists:keymember(undefined, 2, Info).

index_built_fun(Node) ->
fun(Idx) ->
    case rpc:call(Node, riak_kv_vnode,
			     hashtree_pid, [Idx]) of
	{ok, TreePid} ->
	    case rpc:call(Node, riak_kv_index_hashtree,
			  get_lock, [TreePid, for_riak_test]) of
		{badrpc, _} ->
		    {error, {badrpc, Node}};
		TreeLocked when TreeLocked == ok;
				TreeLocked == already_locked ->
		    true;
		Err ->
		    % Either not_built or some unhandled result,
		    % in which case update this case please!
		    {error, {index_not_built, Node, Idx, Err}}
	    end;
	{error, _}=Err ->
	    Err;
	{badrpc, _} ->
	    {error, {badrpc, Node}}
    end
end.

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
BKey = {Bucket, Key},
Chash = rpc:call(Node, riak_core_util, chash_key, [BKey]),
Pl = rpc:call(Node, riak_core_apl, get_primary_apl, [Chash, N, riak_kv]),
{{Partition, PNode}, primary} = lists:nth(I, Pl),
Ref = Reqid = make_ref(),
Sender = {raw, Ref, self()},
rpc:call(PNode, riak_kv_vnode, get,
     [{Partition, PNode}, BKey, Ref, Sender]),
receive
{Ref, {r, Result, _, Reqid}} ->
    Result;
{Ref, Reply} ->
    Reply
after
60000 ->
    lager:error("Replica ~p get for ~p/~p timed out",
		[I, Bucket, Key]),
    ?assert(false)
end.

%%%===================================================================
%%% Search
%%%===================================================================

%% doc Enable the search KV hook for the given `Bucket'.  Any `Node'
%%     in the cluster may be used as the change is propagated via the
%%     Ring.
enable_search_hook(Node, Bucket) when is_binary(Bucket) ->
lager:info("Installing search hook for bucket ~p", [Bucket]),
?assertEqual(ok, rpc:call(Node, riak_search_kv_hook, install, [Bucket])).

%% @doc Gets the current version under test. In the case of an upgrade test
%%      or something like that, it's the version you're upgrading to.
-spec get_version() -> binary().
get_version() ->
?HARNESS:get_version().

%% @doc outputs some useful information about nodes that are up
whats_up() ->
?HARNESS:whats_up().

-spec get_ip(node()) -> string().
get_ip(Node) ->
?HARNESS:get_ip(Node).

%% @doc Log a message to the console of the specified test nodes.
%%      Messages are prefixed by the string "---riak_test--- "
%%      Uses lager:info/1 'Fmt' semantics
log_to_nodes(Nodes, Fmt) ->
log_to_nodes(Nodes, Fmt, []).

%% @doc Log a message to the console of the specified test nodes.
%%      Messages are prefixed by the string "---riak_test--- "
%%      Uses lager:info/2 'LFmt' and 'LArgs' semantics
log_to_nodes(Nodes0, LFmt, LArgs) ->
%% This logs to a node's info level, but if riak_test is running
%% at debug level, we want to know when we send this and what
%% we're saying
Nodes = lists:flatten(Nodes0),
lager:debug("log_to_nodes: " ++ LFmt, LArgs),
Module = lager,
Function = log,
Meta = [],
Args = case LArgs of
       [] -> [info, Meta, "---riak_test--- " ++ LFmt];
       _  -> [info, Meta, "---riak_test--- " ++ LFmt, LArgs]
   end,
[rpc:call(Node, Module, Function, Args) || Node <- lists:flatten(Nodes)].

%% @private utility function
pmap(F, L) ->
Parent = self(),
lists:foldl(
fun(X, N) ->
      spawn_link(fun() ->
		    Parent ! {pmap, N, F(X)}
	    end),
      N+1
end, 0, L),
L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
{_, L3} = lists:unzip(lists:keysort(1, L2)),
L3.

%% @private
setup_harness(Test, Args) ->
?HARNESS:setup_harness(Test, Args).

%% @doc Downloads any extant log files from the harness's running
%%   nodes.
get_node_logs() ->
?HARNESS:get_node_logs().

check_ibrowse() ->
try sys:get_status(ibrowse) of
{status, _Pid, {module, gen_server} ,_} -> ok
catch
Throws ->
    lager:error("ibrowse error ~p", [Throws]),
    lager:error("Restarting ibrowse"),
    application:stop(ibrowse),
    application:start(ibrowse)
end.

post_result(TestResult, #rt_webhook{url=URL, headers=HookHeaders, name=Name}) ->
lager:info("Posting result to ~s ~s", [Name, URL]),
try ibrowse:send_req(URL,
    [{"Content-Type", "application/json"}],
    post,
    mochijson2:encode(TestResult),
    [{content_type, "application/json"}] ++ HookHeaders,
    300000) of  %% 5 minute timeout

{ok, RC=[$2|_], Headers, _Body} ->
    {ok, RC, Headers};
{ok, ResponseCode, Headers, Body} ->
    lager:info("Test Result did not generate the expected 2XX HTTP response code."),
    lager:debug("Post"),
    lager:debug("Response Code: ~p", [ResponseCode]),
    lager:debug("Headers: ~p", [Headers]),
    lager:debug("Body: ~p", [Body]),
    error;
X ->
    lager:warning("Some error POSTing test result: ~p", [X]),
    error
catch
Class:Reason ->
    lager:error("Error reporting to ~s. ~p:~p", [Name, Class, Reason]),
    lager:error("Payload: ~p", [TestResult]),
    error
end.

%%%===================================================================
%%% Bucket Types Functions
%%%===================================================================

%% @doc Set up in memory log capture to check contents in a test.
setup_log_capture(Nodes) when is_list(Nodes) ->
    rt:load_modules_on_nodes([riak_test_lager_backend], Nodes),
    [?assertEqual({Node, ok},
                  {Node,
                   rpc:call(Node,
                            gen_event,
                            add_handler,
                            [lager_event,
                             riak_test_lager_backend,
                             [info, false]])}) || Node <- Nodes],
    [?assertEqual({Node, ok},
                  {Node,
                   rpc:call(Node,
                            lager,
                            set_loglevel,
                            [riak_test_lager_backend,
                             info])}) || Node <- Nodes];
setup_log_capture(Node) when not is_list(Node) ->
    setup_log_capture([Node]).


expect_in_log(Node, Pattern) ->
    CheckLogFun = fun() ->
            Logs = rpc:call(Node, riak_test_lager_backend, get_logs, []),
            lager:info("looking for pattern ~s in logs for ~p",
                       [Pattern, Node]),
            case re:run(Logs, Pattern, []) of
                {match, _} ->
                    lager:info("Found match"),
                    true;
                nomatch    ->
                    lager:info("No match"),
                    false
            end
    end,
    case rt:wait_until(CheckLogFun) of
        ok ->
            true;
        _ ->
            false
    end.

%% @doc Wait for Riak Control to start on a single node.
%%
%% Non-optimal check, because we're blocking for the gen_server to start
%% to ensure that the routes have been added by the supervisor.
%%
wait_for_control(_Vsn, Node) when is_atom(Node) ->
    lager:info("Waiting for riak_control to start on node ~p.", [Node]),

    %% Wait for the gen_server.
    rt:wait_until(Node, fun(N) ->
                case rpc:call(N,
                              riak_control_session,
                              get_version,
                              []) of
                    {ok, _} ->
                        true;
                    Error ->
                        lager:info("Error was ~p.", [Error]),
                        false
                end
        end),

    lager:info("Waiting for routes to be added to supervisor..."),

    %% Wait for routes to be added by supervisor.
    rt:wait_until(Node, fun(N) ->
                case rpc:call(N,
                              webmachine_router,
                              get_routes,
                              []) of
                    {badrpc, Error} ->
                        lager:info("Error was ~p.", [Error]),
                        false;
                    Routes ->
                        case is_control_gui_route_loaded(Routes) of
                            false ->
                                false;
                            _ ->
                                true
                        end
                end
        end).

%% @doc Is the riak_control GUI route loaded?
is_control_gui_route_loaded(Routes) ->
    lists:keymember(admin_gui, 2, Routes) orelse lists:keymember(riak_control_wm_gui, 2, Routes).

%% @doc Wait for Riak Control to start on a series of nodes.
wait_for_control(VersionedNodes) when is_list(VersionedNodes) ->
    [wait_for_control(Vsn, Node) || {Vsn, Node} <- VersionedNodes].

node_id(Node) ->
    ?HARNESS:node_id(Node).

node_version(Node) ->
    ?HARNESS:node_version(Node).

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
