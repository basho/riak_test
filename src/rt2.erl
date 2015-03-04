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
-module(rt2).
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
         stream_cmd/1,
         stream_cmd/2,
         spawn_cmd/1,
         spawn_cmd/2,
         str/2,
         wait_for_cluster_service/2,
         wait_for_cmd/1,
         wait_for_service/2,
         wait_for_control/1,
         wait_for_control/2,
         wait_until/3,
         wait_until/2,
         wait_until/1,
         wait_until_all_members/1,
         wait_until_all_members/2,
         wait_until_capability/3,
         wait_until_capability/4,
         wait_until_connected/1,
         wait_until_legacy_ringready/1,
         wait_until_no_pending_changes/1,
         wait_until_pingable/1,
         wait_until_ready/1,
         wait_until_registered/2,
         wait_until_ring_converged/1,
         wait_until_status_ready/1,
         wait_until_transfers_complete/1,
         wait_until_unpingable/1,
         whats_up/0
        ]).

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
get_deps() -> rt_harness:get_deps().

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
    rt_harness:spawn_cmd(Cmd).

%% @doc Spawn `Cmd' on the machine running the test harness
spawn_cmd(Cmd, Opts) ->
    rt_harness:spawn_cmd(Cmd, Opts).

%% @doc Wait for a command spawned by `spawn_cmd', returning
%%      the exit status and result
wait_for_cmd(CmdHandle) ->
    rt_harness:wait_for_cmd(CmdHandle).

%% @doc Spawn `Cmd' on the machine running the test harness, returning
%%      the exit status and result
cmd(Cmd) ->
    rt_harness:cmd(Cmd).

%% @doc Spawn `Cmd' on the machine running the test harness, returning
%%      the exit status and result
cmd(Cmd, Opts) ->
    rt_harness:cmd(Cmd, Opts).

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
    after rt_config:get(rt_max_receive_wait_time) ->
            {-1, Buffer}
    end.

%%%===================================================================
%%% Remote code management
%%%===================================================================
load_modules_on_nodes([], Nodes) when is_list(Nodes) ->
    ok;
load_modules_on_nodes([Module | MoreModules], Nodes) when is_list(Nodes) ->
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

is_mixed_cluster(Nodes) when is_list(Nodes) ->
    %% If the nodes are bad, we don't care what version they are
    {Versions, _BadNodes} = rpc:multicall(Nodes, init, script_id, [], rt_config:get(rt_max_receive_wait_time)),
    length(lists:usort(Versions)) > 1;
    is_mixed_cluster(Node) ->
    Nodes = rpc:call(Node, erlang, nodes, []),
    is_mixed_cluster(Nodes).

%% @private
is_ring_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
    {ok, Ring} ->
        riak_core_ring:ring_ready(Ring);
    _ ->
        false
    end.

-type products() :: riak | riak_ee | riak_cs | unknown.

-spec product(node()) -> products().
product(Node) ->
    Applications = rpc:call(Node, application, which_applications, []),
    
    HasRiakCS = proplists:is_defined(riak_cs, Applications),
    HasRiakEE = proplists:is_defined(riak_repl, Applications),
    HasRiak = proplists:is_defined(riak_kv, Applications),
        if HasRiakCS ->
                riak_cs;
                  HasRiakEE -> riak_ee;
       HasRiak -> riak;
       true -> unknown
    end.

%% @doc Utility function used to construct test predicates. Retries the
%%      function `Fun' until it returns `true', or until the maximum
%%      number of retries is reached. The retry limit is based on the
%%      provided `rt_max_receive_wait_time' and `rt_retry_delay' parameters in
%%      specified `riak_test' config file.
wait_until(Fun) when is_function(Fun) ->
    MaxTime = rt_config:get(rt_max_receive_wait_time),
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
    ?assertEqual(ok, wait_until(Node, fun rt_node:is_ready/1)),
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
    _OSPidToKill = rpc:call(Node, os, getpid, []),
    F = fun() -> net_adm:ping(Node) =:= pang end,
    %% riak stop will kill -9 after 5 mins, so we try to wait at least that
    %% amount of time.
    Delay = rt_config:get(rt_retry_delay),
    Retry = lists:max([360000, rt_config:get(rt_max_receive_wait_time)]) div Delay,
    lager:info("Wait until ~p is not pingable for ~p seconds with a retry of ~p", 
               [Node, Delay, Retry]),
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
    rt_harness:get_version().

%% @doc outputs some useful information about nodes that are up
whats_up() ->
    rt_harness:whats_up().

-spec get_ip(node()) -> string().
get_ip(Node) ->
    rt_harness:get_ip(Node).

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

%% @doc Parallel Map: Runs function F for each item in list L, then
%%      returns the list of results
-spec pmap(F :: fun(), L :: list()) -> list().
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
    rt_harness:setup_harness(Test, Args).

%% @doc Downloads any extant log files from the harness's running
%%   nodes.
get_node_logs() ->
    rt_harness:get_node_logs().

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
    rt_harness:node_id(Node).

node_version(Node) ->
    rt_harness:node_version(Node).

%% TODO: Is this the right location for this?
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
