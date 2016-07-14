%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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
         admin/2,
         admin/3,
         assert_nodes_agree_about_ownership/1,
         assert_capability/3,
         assert_supported/3,
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
         count_calls/2,
         create_and_activate_bucket_type/3,
         del_dir/1,
         deploy_nodes/1,
         deploy_nodes/2,
         deploy_nodes/3,
         deploy_clusters/1,
         down/2,
         enable_search_hook/2,
         expect_in_log/2,
         get_call_count/2,
         get_deps/0,
         get_ip/1,
         get_node_logs/0,
         get_replica/5,
         get_retry_settings/0,
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
         random_sublist/2,
         remove/2,
         riak/2,
         riak_repl/2,
         rpc_get_env/2,
         select_random/1,
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
         stop_tracing/0,
         stop_and_wait/1,
         str/2,
         str_mult/2,
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
         wait_for_any_webmachine_route/2,
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
-export_type([interfaces/0,
              conn_info/0,
              predicate/1]).

-type strings() :: [string(),...] | [].
-type capability() :: atom() | {atom(), tuple()}.
-type predicate(A) :: fun((A) -> boolean()).
-define(HARNESS, (rt_config:get(rt_harness))).
-define(RT_ETS, rt_ets).
-define(RT_ETS_OPTS, [public, named_table, {write_concurrency, true}]).

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

%% @doc if String contains any of Substrs, return true.
-spec str_mult(string(), [string()]) -> boolean().
str_mult(String, Substrs) ->
    lists:any(fun(S) -> str(String, S) end, Substrs).

-spec set_conf(atom(), [{string(), string()}]) -> ok.
set_conf(all, NameValuePairs) ->
    ?HARNESS:set_conf(all, NameValuePairs);
set_conf(Node, NameValuePairs) ->
    stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    ?HARNESS:set_conf(Node, NameValuePairs),
    start(Node).

-spec set_advanced_conf(atom(), [{string(), string()}]) -> ok.
set_advanced_conf(all, NameValuePairs) ->
    ?HARNESS:set_advanced_conf(all, NameValuePairs);
set_advanced_conf(Node, NameValuePairs) ->
    stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    ?HARNESS:set_advanced_conf(Node, NameValuePairs),
    start(Node).

%% @doc Rewrite the given node's app.config file, overriding the varialbes
%%      in the existing app.config with those in `Config'.
update_app_config(all, Config) ->
    ?HARNESS:update_app_config(all, Config);
update_app_config(Node, Config) ->
    stop(Node),
    ?assertEqual(ok, rt:wait_until_unpingable(Node)),
    ?HARNESS:update_app_config(Node, Config),
    start(Node).

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
    {ok, [{PB_IP, PB_Port}]} = get_pb_conn_info(Node),
    {ok, [{HTTP_IP, HTTP_Port}]} = get_http_conn_info(Node),
    case get_https_conn_info(Node) of
        undefined ->
            [{http, {HTTP_IP, HTTP_Port}}, {pb, {PB_IP, PB_Port}}];
        {ok, [{HTTPS_IP, HTTPS_Port}]} ->
            [{http, {HTTP_IP, HTTP_Port}}, {https, {HTTPS_IP, HTTPS_Port}}, {pb, {PB_IP, PB_Port}}]
    end;
connection_info(Nodes) when is_list(Nodes) ->
    [ {Node, connection_info(Node)} || Node <- Nodes].

-spec get_pb_conn_info(node()) -> [{inet:ip_address(), pos_integer()}].
get_pb_conn_info(Node) ->
    case rpc_get_env(Node, [{riak_api, pb},
                            {riak_api, pb_ip},
                            {riak_kv, pb_ip}]) of
        {ok, [{NewIP, NewPort}|_]} ->
            {ok, [{NewIP, NewPort}]};
        {ok, PB_IP} ->
            {ok, PB_Port} = rpc_get_env(Node, [{riak_api, pb_port},
                                               {riak_kv, pb_port}]),
            {ok, [{PB_IP, PB_Port}]};
        _ ->
            undefined
    end.

-spec get_http_conn_info(node()) -> [{inet:ip_address(), pos_integer()}].
get_http_conn_info(Node) ->
    case rpc_get_env(Node, [{riak_api, http},
                            {riak_core, http}]) of
        {ok, [{IP, Port}|_]} ->
            {ok, [{IP, Port}]};
        _ ->
            undefined
    end.

-spec get_https_conn_info(node()) -> [{inet:ip_address(), pos_integer()}].
get_https_conn_info(Node) ->
    case rpc_get_env(Node, [{riak_api, https},
                            {riak_core, https}]) of
        {ok, [{IP, Port}|_]} ->
            {ok, [{IP, Port}]};
        _ ->
            undefined
    end.

%% @doc Deploy a set of freshly installed Riak nodes, returning a list of the
%%      nodes deployed.
%% @todo Re-add -spec after adding multi-version support
deploy_nodes(Versions) when is_list(Versions) ->
    deploy_nodes(Versions, [riak_kv]);
deploy_nodes(NumNodes) when is_integer(NumNodes) ->
    deploy_nodes([ current || _ <- lists:seq(1, NumNodes)]).

%% @doc Deploy a set of freshly installed Riak nodes with the given
%%      `InitialConfig', returning a list of the nodes deployed.
-spec deploy_nodes(NumNodes :: integer(), any()) -> [node()].
deploy_nodes(NumNodes, InitialConfig) when is_integer(NumNodes) ->
    deploy_nodes(NumNodes, InitialConfig, [riak_kv]);
deploy_nodes(Versions, Services) ->
    NodeConfig = [ version_to_config(Version) || Version <- Versions ],
    Nodes = ?HARNESS:deploy_nodes(NodeConfig),
    lager:info("Waiting for services ~p to start on ~p.", [Services, Nodes]),
    [ ok = wait_for_service(Node, Service) || Node <- Nodes,
                                              Service <- Services ],
    Nodes.

deploy_nodes(NumNodes, InitialConfig, Services) when is_integer(NumNodes) ->
    NodeConfig = [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)],
    deploy_nodes(NodeConfig, Services).

version_to_config(Config) when is_tuple(Config)-> Config;
version_to_config(Version) -> {Version, default}.

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
    ?HARNESS:start(Node).

%% @doc Start the specified Riak `Node' and wait for it to be pingable
start_and_wait(Node) ->
    start(Node),
    ?assertEqual(ok, wait_until_pingable(Node)).

async_start(Node) ->
    spawn(fun() -> start(Node) end).

%% @doc Stop the specified Riak `Node'.
stop(Node) ->
    lager:info("Stopping riak on ~p", [Node]),
    timer:sleep(10000), %% I know, I know!
    ?HARNESS:stop(Node).
    %%rpc:call(Node, init, stop, []).

%% @doc Stop the specified Riak `Node' and wait until it is not pingable
stop_and_wait(Node) ->
    stop(Node),
    ?assertEqual(ok, wait_until_unpingable(Node)).

%% @doc Upgrade a Riak `Node' to the specified `NewVersion'.
upgrade(Node, NewVersion) ->
    ?HARNESS:upgrade(Node, NewVersion).

%% @doc Upgrade a Riak `Node' to the specified `NewVersion' and update
%% the config based on entries in `Config'.
upgrade(Node, NewVersion, Config) ->
    ?HARNESS:upgrade(Node, NewVersion, Config).

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

%% Ideally we'd use `wait_until' for join retries, but it isn't
%% currently flexible enough to only retry on a specific error
%% tuple. Rather than rework all of the various arities, steal the
%% concept and worry about this later if the need arises.
join_with_retry(Fun) ->
    MaxTime = rt_config:get(rt_max_wait_time),
    Delay = rt_config:get(rt_retry_delay),
    Retry = MaxTime div Delay,
    join_retry(Fun(), Fun, Retry, Delay).

join_retry(ok, _Fun, _Retry, _Delay) ->
    ok;
join_retry({error, node_still_starting}, _Fun, 0, _Delay) ->
    lager:warning("Too many retries, join failed"),
    {error, too_many_retries};
join_retry({error, node_still_starting}, Fun, RetryCount, Delay) ->
    lager:warning("Join error because node is not yet ready, retrying after ~Bms", [Delay]),
    timer:sleep(Delay),
    join_retry(Fun(), Fun, RetryCount - 1, Delay);
join_retry(Error, _Fun, _Retry, _Delay) ->
    Error.

%% @doc Have `Node' send a join request to `PNode'
join(Node, PNode) ->
    Fun = fun() -> rpc:call(Node, riak_core, join, [PNode]) end,
    lager:info("[join] ~p to (~p)", [Node, PNode]),
    ?assertEqual(ok, join_with_retry(Fun)),
    ok.

%% @doc Have `Node' send a join request to `PNode'
staged_join(Node, PNode) ->
    %% `riak_core:staged_join/1' can now return an `{error,
    %% node_still_starting}' tuple which indicates retry.
    Fun = fun() -> rpc:call(Node, riak_core, staged_join, [PNode]) end,
    lager:info("[join] ~p to (~p)", [Node, PNode]),
    ?assertEqual(ok, join_with_retry(Fun)),
    ok.

plan_and_commit(Node) ->
    timer:sleep(500),
    lager:info("planning cluster join"),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            lager:info("plan: ring not ready"),
            timer:sleep(100),
            plan_and_commit(Node);
        {ok, _, _} ->
            lager:info("plan: done"),
            do_commit(Node)
    end.

do_commit(Node) ->
    lager:info("planning cluster commit"),
    case rpc:call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            lager:info("commit: plan changed"),
            timer:sleep(100),
            maybe_wait_for_changes(Node),
            plan_and_commit(Node);
        {error, ring_not_ready} ->
            lager:info("commit: ring not ready"),
            timer:sleep(100),
            maybe_wait_for_changes(Node),
            do_commit(Node);
        {error, nothing_planned} ->
            %% Assume plan actually committed somehow
            lager:info("commit: nothing planned"),
            ok;
        ok ->
            ok
    end.

maybe_wait_for_changes(Node) ->
    Ring = get_ring(Node),
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

%% @doc Have the `Node' leave the cluster
leave(Node) ->
    R = rpc:call(Node, riak_core, leave, []),
    lager:info("[leave] ~p: ~p", [Node, R]),
    ?assertEqual(ok, R),
    ok.

%% @doc Have `Node' remove `OtherNode' from the cluster
remove(Node, OtherNode) ->
    ?assertEqual(ok,
                 rpc:call(Node, riak_kv_console, remove, [[atom_to_list(OtherNode)]])).

%% @doc Have `Node' mark `OtherNode' as down
down(Node, OtherNode) ->
    rpc:call(Node, riak_kv_console, down, [[atom_to_list(OtherNode)]]).

%% @doc partition the `P1' from `P2' nodes
%%      note: the nodes remained connected to riak_test@local,
%%      which is how `heal/1' can still work.
partition(P1, P2) ->
    OldCookie = rpc:call(hd(P1), erlang, get_cookie, []),
    NewCookie = list_to_atom(lists:reverse(atom_to_list(OldCookie))),
    [true = rpc:call(N, erlang, set_cookie, [N, NewCookie]) || N <- P1],
    [[true = rpc:call(N, erlang, disconnect_node, [P2N]) || N <- P1] || P2N <- P2],
    wait_until_partitioned(P1, P2),
    {NewCookie, OldCookie, P1, P2}.

%% @doc heal the partition created by call to `partition/2'
%%      `OldCookie' is the original shared cookie
heal({_NewCookie, OldCookie, P1, P2}) ->
    Cluster = P1 ++ P2,
    % set OldCookie on P1 Nodes
    [true = rpc:call(N, erlang, set_cookie, [N, OldCookie]) || N <- P1],
    wait_until_connected(Cluster),
    {_GN, []} = rpc:sbcast(Cluster, riak_core_node_watcher, broadcast),
    ok.

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
            {ResList, []} = rpc:multicall(Nodes, code, load_binary, [Module, File, Bin]),
            lager:info("Loading modules on Nodes ~p, results were ~p", [Nodes, ResList]);
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
    {Delay, Retry} = get_retry_settings(),
    wait_until(Fun, Retry, Delay).

get_retry_settings() ->
    MaxTime = rt_config:get(rt_max_wait_time),
    Delay = rt_config:get(rt_retry_delay),
    Retry = MaxTime div Delay,
    {Delay, Retry}.

%% @doc Convenience wrapper for wait_until for the myriad functions that
%% take a node as single argument.
-spec wait_until(node(), fun(() -> boolean())) -> ok | {fail, Result :: term()}.
wait_until(Node, Fun) when is_atom(Node), is_function(Fun) ->
    wait_until(fun() -> Fun(Node) end);

%% @doc Wrapper to verify `F' against multiple nodes. The function `F' is passed
%%      one of the `Nodes' as argument and must return a `boolean()' declaring
%%      whether the success condition has been met or not.
wait_until(Nodes, Fun) when is_list(Nodes), is_function(Fun) ->
    [?assertEqual(ok, wait_until(Node, Fun)) || Node <- Nodes],
    ok.

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
                case BadNodes =:= [] andalso length(Changes) =:= length(Nodes) andalso lists:all(fun(T) -> T end, Changes) of
                    true -> true;
                    false ->
                        NodesWithChanges = [Node || {Node, false} <- lists:zip(Nodes -- BadNodes, Changes)],
                        lager:info("Changes not yet complete, or bad nodes. BadNodes=~p, Nodes with Pending Changes=~p~n", [BadNodes, NodesWithChanges]),
                        false
                end
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
                case members_according_to(Node) of
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
    NodeSet = sets:from_list(Nodes),
    F = fun(Node) ->
                Connected = rpc:call(Node, erlang, nodes, []),
                sets:is_subset(NodeSet, sets:from_list(([Node] ++ Connected) -- [node()]))
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

% when you just can't wait
brutal_kill(Node) ->
    rt_cover:maybe_stop_on_node(Node),
    lager:info("Killing node ~p", [Node]),
    OSPidToKill = rpc:call(Node, os, getpid, []),
    %% try a normal kill first, but set a timer to
    %% kill -9 after 5 seconds just in case
    rpc:cast(Node, timer, apply_after,
             [5000, os, cmd, [io_lib:format("kill -9 ~s", [OSPidToKill])]]),
    rpc:cast(Node, os, cmd, [io_lib:format("kill -15 ~s", [OSPidToKill])]),
    ok.

capability(Node, all) ->
    rpc:call(Node, riak_core_capability, all, []);
capability(Node, Capability) ->
    rpc:call(Node, riak_core_capability, get, [Capability]).

capability(Node, Capability, Default) ->
    rpc:call(Node, riak_core_capability, get, [Capability, Default]).

wait_until_capability(Node, Capability, Value) ->
    rt:wait_until(Node,
                  fun(_) ->
                      Cap = capability(Node, Capability),
                      lager:info("Capability on node ~p is ~p~n",[Node, Cap]),
                      cap_equal(Value, Cap)
                  end).

wait_until_capability(Node, Capability, Value, Default) ->
    rt:wait_until(Node,
                  fun(_) ->
                          Cap = capability(Node, Capability, Default),
                          lager:info("Capability on node ~p is ~p~n",[Node, Cap]),
                          cap_equal(Value, Cap)
                  end).

-spec wait_until_capability_contains(node(), atom() | {atom(), atom()}, list()) -> ok.
wait_until_capability_contains(Node, Capability, Value) ->
    rt:wait_until(Node,
                fun(_) ->
                    Cap = capability(Node, Capability),
                    lager:info("Capability on node ~p is ~p~n",[Node, Cap]),
                    cap_subset(Value, Cap)
                end).

cap_equal(Val, Cap) when is_list(Cap) ->
    lists:sort(Cap) == lists:sort(Val);
cap_equal(Val, Cap) ->
    Val == Cap.

cap_subset(Val, Cap) when is_list(Cap) ->
    sets:is_subset(sets:from_list(Val), sets:from_list(Cap)).

wait_until_owners_according_to(Node, Nodes) ->
    SortedNodes = lists:usort(Nodes),
    F = fun(N) ->
        owners_according_to(N) =:= SortedNodes
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
%%% Ring Functions
%%%===================================================================

%% @doc Ensure that the specified node is a singleton node/cluster -- a node
%%      that owns 100% of the ring.
check_singleton_node(Node) ->
    lager:info("Check ~p is a singleton", [Node]),
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Owners = lists:usort([Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)]),
    ?assertEqual([Node], Owners),
    ok.

% @doc Get list of partitions owned by node (primary).
partitions_for_node(Node) ->
    Ring = get_ring(Node),
    [Idx || {Idx, Owner} <- riak_core_ring:all_owners(Ring), Owner == Node].

%% @doc Get the raw ring for `Node'.
get_ring(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Ring.

assert_nodes_agree_about_ownership(Nodes) ->
    ?assertEqual(ok, wait_until_ring_converged(Nodes)),
    ?assertEqual(ok, wait_until_all_members(Nodes)),
    [ ?assertEqual({Node, Nodes}, {Node, owners_according_to(Node)}) || Node <- Nodes].

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
owners_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Owners = [Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)],
            lists:usort(Owners);
        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%% @doc Return a list of cluster members according to the ring retrieved from
%%      the specified node.
members_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Members = riak_core_ring:all_members(Ring),
            Members;
        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%% @doc Return an appropriate ringsize for the node count passed
%%      in. 24 is the number of cores on the bigger intel machines, but this
%%      may be too large for the single-chip machines.
nearest_ringsize(Count) ->
    nearest_ringsize(Count * 24, 2).

nearest_ringsize(Count, Power) ->
    case Count < trunc(Power * 0.9) of
        true ->
            Power;
        false ->
            nearest_ringsize(Count, Power * 2)
    end.

%% @doc Return the cluster status of `Member' according to the ring
%%      retrieved from `Node'.
status_of_according_to(Member, Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Status = riak_core_ring:member_status(Ring, Member),
            Status;
        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%% @doc Return a list of nodes that own partitions according to the ring
%%      retrieved from the specified node.
claimant_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Claimant = riak_core_ring:claimant(Ring),
            Claimant;
        {badrpc, _}=BadRpc ->
            BadRpc
    end.

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

systest_delete(Node, Size) ->
    systest_delete(Node, Size, 2).

systest_delete(Node, Size, W) ->
    systest_delete(Node, 1, Size, <<"systest">>, W).

%% @doc Delete `(End-Start)+1' objects on `Node'. Keys deleted will be
%% `Start', `Start+1' ... `End', each key being encoded as a 32-bit binary
%% (`<<Key:32/integer>>').
%%
%% The return value of this function is a list of errors
%% encountered. If all deletes were successful, return value is an
%% empty list. Each error has the form `{N :: integer(), Error :: term()}',
%% where `N' is the unencoded key of the object that failed to store.
systest_delete(Node, Start, End, Bucket, W) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    F = fun(N, Acc) ->
                Key = <<N:32/integer>>,
                try C:delete(Bucket, Key, W) of
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

systest_verify_delete(Node, Size) ->
    systest_verify_delete(Node, Size, 2).

systest_verify_delete(Node, Size, R) ->
    systest_verify_delete(Node, 1, Size, <<"systest">>, R).

%% @doc Read a series of keys on `Node' and verify that the objects
%% do not exist. This could, for instance, be used as a followup to
%% `systest_delete' to ensure that the objects were actually deleted.
systest_verify_delete(Node, Start, End, Bucket, R) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    F = fun(N, Acc) ->
                Key = <<N:32/integer>>,
                try C:get(Bucket, Key, R) of
                    {error, notfound} ->
                        [];
                    Other ->
                        [{N, Other} | Acc]
                catch
                    What:Why ->
                        [{N, {What, Why}} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

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

%% @doc PBC-based version of {@link systest_write/1}
pbc_systest_write(Node, Size) ->
    pbc_systest_write(Node, Size, 2).

pbc_systest_write(Node, Size, W) ->
    pbc_systest_write(Node, 1, Size, <<"systest">>, W).

pbc_systest_write(Node, Start, End, Bucket, W) ->
    rt:wait_for_service(Node, riak_kv),
    Pid = pbc(Node),
    F = fun(N, Acc) ->
                Obj = riakc_obj:new(Bucket, <<N:32/integer>>, <<N:32/integer>>),
                try riakc_pb_socket:put(Pid, Obj, W) of
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

pbc_systest_read(Node, Size) ->
    pbc_systest_read(Node, Size, 2).

pbc_systest_read(Node, Size, R) ->
    pbc_systest_read(Node, 1, Size, <<"systest">>, R).

pbc_systest_read(Node, Start, End, Bucket, R) ->
    rt:wait_for_service(Node, riak_kv),
    Pid = pbc(Node),
    F = fun(N, Acc) ->
                case riakc_pb_socket:get(Pid, Bucket, <<N:32/integer>>, R) of
                    {ok, Obj} ->
                        case riakc_obj:get_value(Obj) of
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

%%%===================================================================
%%% PBC & HTTPC Functions
%%%===================================================================

%% @doc get me a protobuf client process and hold the mayo!
-spec pbc(node()) -> pid().
pbc(Node) ->
    pbc(Node, [{auto_reconnect, true}]).

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
    {ok, Value} = riakc_pb_socket:get(Pid, Bucket, Key, Options),
    Value.

-spec pbc_read_check(pid(), binary(), binary(), [any()]) -> boolean().
pbc_read_check(Pid, Bucket, Key, Allowed) ->
    pbc_read_check(Pid, Bucket, Key, Allowed, []).

-spec pbc_read_check(pid(), binary(), binary(), [any()], [any()]) -> boolean().
pbc_read_check(Pid, Bucket, Key, Allowed, Options) ->
    case riakc_pb_socket:get(Pid, Bucket, Key, Options) of
        {ok, _} ->
            true = lists:member(ok, Allowed);
        Other ->
            lists:member(Other, Allowed) orelse throw({failed, Other, Allowed})
    end.

%% @doc does a write via the erlang protobuf client
-spec pbc_write(pid(), binary(), binary(), binary()) -> atom().
pbc_write(Pid, Bucket, Key, Value) ->
    Object = riakc_obj:new(Bucket, Key, Value),
    riakc_pb_socket:put(Pid, Object).

%% @doc does a write via the erlang protobuf client plus content-type
-spec pbc_write(pid(), binary(), binary(), binary(), list()) -> atom().
pbc_write(Pid, Bucket, Key, Value, CT) ->
    Object = riakc_obj:new(Bucket, Key, Value, CT),
    riakc_pb_socket:put(Pid, Object).

%% @doc sets a bucket property/properties via the erlang protobuf client
-spec pbc_set_bucket_prop(pid(), binary(), [proplists:property()]) -> atom().
pbc_set_bucket_prop(Pid, Bucket, PropList) ->
    riakc_pb_socket:set_bucket(Pid, Bucket, PropList).

%% @doc Puts the contents of the given file into the given bucket using the
%% filename as a key and assuming a plain text content type.
pbc_put_file(Pid, Bucket, Key, Filename) ->
    {ok, Contents} = file:read_file(Filename),
    riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Contents, "text/plain")).

%% @doc Puts all files in the given directory into the given bucket using the
%% filename as a key and assuming a plain text content type.
pbc_put_dir(Pid, Bucket, Dir) ->
    lager:info("Putting files from dir ~p into bucket ~p", [Dir, Bucket]),
    {ok, Files} = file:list_dir(Dir),
    [pbc_put_file(Pid, Bucket, list_to_binary(F), filename:join([Dir, F]))
     || F <- Files].

%% @doc True if the given keys have been really, really deleted.
%% Useful when you care about the keys not being there. Delete simply writes
%% tombstones under the given keys, so those are still seen by key folding
%% operations.
pbc_really_deleted(Pid, Bucket, Keys) ->
    StillThere =
    fun(K) ->
            Res = riakc_pb_socket:get(Pid, Bucket, K,
                                      [{r, 1},
                                      {notfound_ok, false},
                                      {basic_quorum, false},
                                      deletedvclock]),
            case Res of
                {error, notfound} ->
                    false;
                _ ->
                    %% Tombstone still around
                    true
            end
    end,
    [] == lists:filter(StillThere, Keys).

%% @doc Returns HTTPS URL information for a list of Nodes
https_url(Nodes) when is_list(Nodes) ->
    [begin
         {Host, Port} = orddict:fetch(https, Connections),
         lists:flatten(io_lib:format("https://~s:~b", [Host, Port]))
     end || {_Node, Connections} <- connection_info(Nodes)];
https_url(Node) ->
    hd(https_url([Node])).

%% @doc Returns HTTP URL information for a list of Nodes
http_url(Nodes) when is_list(Nodes) ->
    [begin
         {Host, Port} = orddict:fetch(http, Connections),
         lists:flatten(io_lib:format("http://~s:~b", [Host, Port]))
     end || {_Node, Connections} <- connection_info(Nodes)];
http_url(Node) ->
    hd(http_url([Node])).

%% @doc get me an http client.
-spec httpc(node()) -> term().
httpc(Node) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, [{IP, Port}]} = get_http_conn_info(Node),
    rhc:create(IP, Port, "riak", []).

%% @doc does a read via the http erlang client.
-spec httpc_read(term(), binary(), binary()) -> binary().
httpc_read(C, Bucket, Key) ->
    {_, Value} = rhc:get(C, Bucket, Key),
    Value.

%% @doc does a write via the http erlang client.
-spec httpc_write(term(), binary(), binary(), binary()) -> atom().
httpc_write(C, Bucket, Key, Value) ->
    Object = riakc_obj:new(Bucket, Key, Value),
    rhc:put(C, Object).

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
    ?HARNESS:admin(Node, Args, Options).

%% @doc Call 'bin/riak' command on `Node' with arguments `Args'
riak(Node, Args) ->
    ?HARNESS:riak(Node, Args).


%% @doc Call 'bin/riak-repl' command on `Node' with arguments `Args'
riak_repl(Node, Args) ->
    ?HARNESS:riak_repl(Node, Args).

search_cmd(Node, Args) ->
    {ok, Cwd} = file:get_cwd(),
    rpc:call(Node, riak_search_cmd, command, [[Cwd | Args]]).

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
    ?HARNESS:attach(Node, Expected).

%% @doc Runs 'riak attach-direct' on a specific node
%% @see rt:attach/2
attach_direct(Node, Expected) ->
    ?HARNESS:attach_direct(Node, Expected).

%% @doc Runs `riak console' on a specific node
%% @see rt:attach/2
console(Node, Expected) ->
    ?HARNESS:console(Node, Expected).

%%%===================================================================
%%% Search
%%%===================================================================

%% doc Enable the search KV hook for the given `Bucket'.  Any `Node'
%%     in the cluster may be used as the change is propagated via the
%%     Ring.
enable_search_hook(Node, Bucket) when is_binary(Bucket) ->
    lager:info("Installing search hook for bucket ~p", [Bucket]),
    ?assertEqual(ok, rpc:call(Node, riak_search_kv_hook, install, [Bucket])).

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

%% @doc Gets the string flavor of the version tag specified
%% (e.g. current, legacy, previous, etc).
-spec(get_version(atom()) -> binary()).
get_version(Vsn) ->
    ?HARNESS:get_version(Vsn).

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

%% @doc Performs a search against the log files on `Node' and returns all
%% matching lines.
-spec search_logs(node(), Pattern::iodata()) ->
    [{Path::string(), LineNum::pos_integer(), MatchingLine::string()}].
search_logs(Node, Pattern) ->
    ?HARNESS:search_logs(Node, Pattern).

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

%% @doc create and immediately activate a bucket type
create_and_activate_bucket_type(Node, Type, Props) ->
    ok = rpc:call(Node, riak_core_bucket_type, create, [Type, Props]),
    wait_until_bucket_type_status(Type, ready, Node),
    ok = rpc:call(Node, riak_core_bucket_type, activate, [Type]),
    wait_until_bucket_type_status(Type, active, Node).

create_activate_and_wait_for_bucket_type([Node|_Rest]=Cluster, Type, Props) ->
    create_and_activate_bucket_type(Node, Type, Props),
    wait_until_bucket_type_visible(Cluster, Type).

wait_until_bucket_type_status(Type, ExpectedStatus, Nodes) when is_list(Nodes) ->
    [wait_until_bucket_type_status(Type, ExpectedStatus, Node) || Node <- Nodes];
wait_until_bucket_type_status(Type, ExpectedStatus, Node) ->
    F = fun() ->
                ActualStatus = rpc:call(Node, riak_core_bucket_type, status, [Type]),
                ExpectedStatus =:= ActualStatus
        end,
    ?assertEqual(ok, rt:wait_until(F)).

-spec bucket_type_visible([atom()], binary()|{binary(), binary()}) -> boolean().
bucket_type_visible(Nodes, Type) ->
    MaxTime = rt_config:get(rt_max_wait_time),
    IsVisible = fun erlang:is_list/1,
    {Res, NodesDown} = rpc:multicall(Nodes, riak_core_bucket_type, get, [Type], MaxTime),
    NodesDown == [] andalso lists:all(IsVisible, Res).

wait_until_bucket_type_visible(Nodes, Type) ->
    F = fun() -> bucket_type_visible(Nodes, Type) end,
    ?assertEqual(ok, rt:wait_until(F)).

-spec see_bucket_props([atom()], binary()|{binary(), binary()},
                       proplists:proplist()) -> boolean().
see_bucket_props(Nodes, Bucket, ExpectProps) ->
    MaxTime = rt_config:get(rt_max_wait_time),
    IsBad = fun({badrpc, _}) -> true;
               ({error, _}) -> true;
               (Res) when is_list(Res) -> false
            end,
    HasProps = fun(ResProps) ->
                       lists:all(fun(P) -> lists:member(P, ResProps) end,
                                 ExpectProps)
               end,
    case rpc:multicall(Nodes, riak_core_bucket, get_bucket, [Bucket], MaxTime) of
        {Res, []} ->
            % No nodes down, check no errors
            case lists:any(IsBad, Res) of
                true  ->
                    false;
                false ->
                    lists:all(HasProps, Res)
            end;
        {_, _NodesDown} ->
            false
    end.

wait_until_bucket_props(Nodes, Bucket, Props) ->
    F = fun() ->
                see_bucket_props(Nodes, Bucket, Props)
        end,
    ?assertEqual(ok, rt:wait_until(F)).


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
    {Delay, Retry} = get_retry_settings(),
    expect_in_log(Node, Pattern, Retry, Delay).

expect_in_log(Node, Pattern, Retry, Delay) ->
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
    case rt:wait_until(CheckLogFun, Retry, Delay) of
        ok ->
            true;
        _ ->
            false
    end.

%% @doc Returns `true' if Pattern is _not_ found in the logs for `Node',
%% `false' if it _is_ found.
-spec expect_not_in_logs(Node::node(), Pattern::iodata()) -> boolean().
expect_not_in_logs(Node, Pattern) ->
    case search_logs(Node, Pattern) of
        [] ->
            true;
        _Matches ->
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

    %% Wait for routes to be added by supervisor.
    wait_for_any_webmachine_route(Node, [admin_gui, riak_control_wm_gui]).

wait_for_any_webmachine_route(Node, Routes) ->
    lager:info("Waiting for routes ~p to be added to webmachine.", [Routes]),
    rt:wait_until(Node, fun(N) ->
        case rpc:call(N, webmachine_router, get_routes, []) of
            {badrpc, Error} ->
                lager:info("Error was ~p.", [Error]),
                false;
            RegisteredRoutes ->
                case is_any_route_loaded(Routes, RegisteredRoutes) of
                    false ->
                        false;
                    _ ->
                        true
                end
        end
    end).

is_any_route_loaded(SearchRoutes, RegisteredRoutes) ->
    lists:any(fun(Route) -> is_route_loaded(Route, RegisteredRoutes) end, SearchRoutes).

is_route_loaded(Route, Routes) ->
    lists:keymember(Route, 2, Routes).

%% @doc Wait for Riak Control to start on a series of nodes.
wait_for_control(VersionedNodes) when is_list(VersionedNodes) ->
    [wait_for_control(Vsn, Node) || {Vsn, Node} <- VersionedNodes].

%% @doc Choose random in cluster, for example.
-spec select_random([any()]) -> any().
select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

%% @doc Returns a random element from a given list.
-spec random_sublist([any()], integer()) -> [any()].
random_sublist(List, N) ->
    % Properly seeding the process.
    <<A:32, B:32, C:32>> = crypto:rand_bytes(12),
    random:seed({A, B, C}),
    % Assign a random value for each element in the list.
    List1 = [{random:uniform(), E} || E <- List],
    % Sort by the random number.
    List2 = lists:sort(List1),
    % Take the first N elements.
    List3 = lists:sublist(List2, N),
    % Remove the random numbers.
    [ E || {_,E} <- List3].

%% @doc Recusively delete files in a directory.
-spec del_dir(string()) -> strings().
del_dir(Dir) ->
    lists:foreach(fun(D) ->
                          ok = file:del_dir(D)
                  end, del_all_files([Dir], [])).

-spec del_all_files(strings(), strings()) -> strings().
del_all_files([], EmptyDirs) ->
   EmptyDirs;
del_all_files([Dir | T], EmptyDirs) ->
    {ok, FilesInDir} = file:list_dir(Dir),
    {Files, Dirs} = lists:foldl(
                      fun(F, {Fs, Ds}) ->
                              Path = Dir ++ "/" ++ F,
                              case filelib:is_dir(Path) of
                                  true ->
                                      {Fs, [Path | Ds]};
                                  false ->
                                      {[Path | Fs], Ds}
                              end
                      end, {[],[]}, FilesInDir),
    lists:foreach(fun(F) ->
                          ok = file:delete(F)
                  end, Files),
    del_all_files(T ++ Dirs, [Dir | EmptyDirs]).

%% @doc Get call count from ETS table, key being a {Module, Fun, Arity}.
-spec get_call_count([node()], {atom(), atom(), non_neg_integer()}) ->
                            non_neg_integer().
get_call_count(Cluster, MFA) when is_list(Cluster) ->
    case ets:lookup(?RT_ETS, MFA) of
        [{_,Count}] ->
            Count;
        [] ->
            0
    end.

%% @doc Count calls in a dbg:tracer process for various {Module, Fun, Arity}
%%      traces.
-spec count_calls([node()], [{atom(), atom(), non_neg_integer()}]) -> ok.
count_calls(Cluster, MFAs) when is_list(Cluster) ->
    lager:info("count all calls to MFA ~p across the cluster ~p",
               [MFAs, Cluster]),
    RiakTestNode = node(),
    maybe_create_ets(),
    dbg:tracer(process, {fun trace_count/2, {RiakTestNode, Cluster}}),
    [{ok, Node} = dbg:n(Node) || Node <- Cluster],
    dbg:p(all, call),
    [{ok, _} = dbg:tpl(M, F, A, [{'_', [], [{return_trace}]}]) || {M, F, A} <- MFAs],
    ok.

%% @doc Maybe create an ETS table for storing function calls and counts.
-spec maybe_create_ets() -> ok.
maybe_create_ets() ->
    case ets:info(?RT_ETS) of
        undefined ->
            ets:new(?RT_ETS, ?RT_ETS_OPTS),
            ok;
        _ ->
            ets:delete(?RT_ETS),
            ets:new(?RT_ETS, ?RT_ETS_OPTS),
            ok
    end.

%% @doc Stop dbg tracing.
-spec stop_tracing() -> ok.
stop_tracing() ->
    lager:info("stop all dbg tracing"),
    dbg:stop_clear(),
    ok.

%% @doc Trace fun calls and store their count state into an ETS table.
-spec trace_count({trace, pid(), call|return_from,
                   {atom(), atom(), non_neg_integer()}}, {node(), [node()]}) ->
                         {node(), [node()]}.
trace_count({trace, _Pid, call, {_M, _F, _A}}, Acc) ->
    Acc;
trace_count({trace, _Pid, return_from, MFA, _Result}, {RTNode, Cluster}) ->
    Count = get_call_count(Cluster, MFA),
    Count2 = Count + 1,
    rpc:call(RTNode, ets, insert, [?RT_ETS, {MFA, Count2}]),
    {RTNode, Cluster}.

-spec assert_capability(node(), capability(), atom()) -> ok.
assert_capability(CNode, Capability, Value) ->
    lager:info("Checking Capability Setting ~p =:= ~p on ~p",
               [Capability, Value, CNode]),
    ?assertEqual(ok, rt:wait_until_capability(CNode, Capability, Value)),
    ok.

-spec assert_supported([capability()], capability(), atom()|[atom()]) -> ok.
assert_supported(Capabilities, Capability, Value) ->
    lager:info("Checking Capability Supported Values ~p =:= ~p",
               [Capability, Value]),
    ?assertEqual(Value, proplists:get_value(
                          Capability,
                          proplists:get_value('$supported', Capabilities))),
    ok.


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
