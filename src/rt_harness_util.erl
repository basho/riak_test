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
%% @doc The purpose of rt_harness_util is to provide common functions
%% to harness modules implementing the test_harness behaviour.
-module(rt_harness_util).

-include_lib("eunit/include/eunit.hrl").
-define(DEVS(N), lists:concat([N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(PATH, (rt_config:get(root_path))).

-export([admin/2,
         attach/2,
         attach_direct/2,
         cmd/1,
         cmd/2,
         console/2,
         deploy_nodes/5,
         get_ip/1,
         node_id/1,
         node_version/1,
         riak/2,
         set_conf/2,
         set_advanced_conf/2,
         setup_harness/3,
         get_advanced_riak_conf/1,
         update_app_config_file/2,
         spawn_cmd/1,
         spawn_cmd/2,
         whats_up/0]).

admin(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = riak_admin_cmd(Path, N, Args),
    lager:info("Running: ~s", [Cmd]),
    Result = os:cmd(Cmd),
    lager:info("~s", [Result]),
    {ok, Result}.

attach(Node, Expected) ->
    interactive(Node, "attach", Expected).

attach_direct(Node, Expected) ->
    interactive(Node, "attach-direct", Expected).

console(Node, Expected) ->
    interactive(Node, "console", Expected).

%% deploy_clusters(ClusterConfigs) ->
%%     NumNodes = rt_config:get(num_nodes, 6),
%%     RequestedNodes = lists:flatten(ClusterConfigs),

%%     case length(RequestedNodes) > NumNodes of
%%         true ->
%%             erlang:error("Requested more nodes than available");
%%         false ->
%%             Nodes = deploy_nodes(RequestedNodes),
%%             {DeployedClusters, _} = lists:foldl(
%%                     fun(Cluster, {Clusters, RemNodes}) ->
%%                         {A, B} = lists:split(length(Cluster), RemNodes),
%%                         {Clusters ++ [A], B}
%%                 end, {[], Nodes}, ClusterConfigs),
%%             DeployedClusters
%%     end.

%% deploy_nodes(NodeConfig) ->
deploy_nodes(NodeIds, _NodeMap, _Version, _Config, _Services) when NodeIds =:= [] ->
    NodeIds;
deploy_nodes(NodeIds, NodeMap, Version, Config, Services) ->
    %% create snmp dirs, for EE
    create_dirs(Version, NodeIds),

    %% Set initial config
    ConfigUpdateFun =
        fun(Node) ->
                rt_harness:update_app_config(Node, Version, Config)
        end,
    rt:pmap(ConfigUpdateFun, NodeIds),

    %% Start nodes
    RunRiakFun =
        fun(Node) ->
            rt_harness:run_riak(Node, Version, "start")
        end,
    rt:pmap(RunRiakFun, NodeIds),

    %% Ensure nodes started
    lager:debug("Wait until pingable: ~p", [NodeIds]),
    [ok = rt:wait_until_pingable(rt_node:node_name(NodeId, NodeMap))
                                 || NodeId <- NodeIds],

    %% TODO Rubbish! Fix this.
    %% %% Enable debug logging
    %% [rpc:call(N, lager, set_loglevel, [lager_console_backend, debug])
    %% || N <- Nodes],

    %% We have to make sure that riak_core_ring_manager is running
    %% before we can go on.
    [ok = rt:wait_until_registered(rt_node:node_name(NodeId, NodeMap),
                                   riak_core_ring_manager)
     || NodeId <- NodeIds],

    %% Ensure nodes are singleton clusters
    case Version =/= "0.14.2" of
        true ->
            [ok = rt_ring:check_singleton_node(rt_node:node_name(NodeId, NodeMap))
             || NodeId <- NodeIds];
        false ->
            ok
    end,

    %% Wait for services to start
    lager:debug("Waiting for services ~p to start on ~p.", [Services, NodeIds]),
    [ ok = rt:wait_for_service(rt_node:node_name(NodeId, NodeMap), Service)
      || NodeId <- NodeIds,
         Service <- Services ],

    lager:debug("Deployed nodes: ~p", [NodeIds]),
    NodeIds.

interactive(Node, Command, Exp) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = riakcmd(Path, N, Command),
    lager:info("Opening a port for riak ~s.", [Command]),
    lager:debug("Calling open_port with cmd ~s", [binary_to_list(iolist_to_binary(Cmd))]),
    P = open_port({spawn, binary_to_list(iolist_to_binary(Cmd))},
                  [stream, use_stdio, exit_status, binary, stderr_to_stdout]),
    interactive_loop(P, Exp).

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
                    case {Type, rt:str(X, Text)} of
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
        after rt_config:get(rt_max_wait_time) ->
            %% interactive_loop is going to wait until it matches expected behavior
            %% If it doesn't, the test should fail; however, without a timeout it
            %% will just hang forever in search of expected behavior. See also: Parenting
            ?assertEqual([], Expected)
    end.

node_to_host(Node) ->
    case string:tokens(atom_to_list(Node), "@") of
        ["riak", Host] -> Host;
        _ ->
            throw(io_lib:format("rtssh:node_to_host couldn't figure out the host of ~p", [Node]))
    end.

spawn_cmd(Cmd) ->
    spawn_cmd(Cmd, []).
spawn_cmd(Cmd, Opts) ->
    Port = open_port({spawn, lists:flatten(Cmd)}, [stream, in, exit_status] ++ Opts),
    Port.

wait_for_cmd(Port) ->
    rt:wait_until(node(),
                  fun(_) ->
                          receive
                              {Port, Msg={exit_status, _}} ->
                                  catch port_close(Port),
                                  self() ! {Port, Msg},
                                  true
                          after 0 ->
                                  false
                          end
                  end),
    get_cmd_result(Port, []).

cmd(Cmd) ->
    cmd(Cmd, []).

cmd(Cmd, Opts) ->
    wait_for_cmd(spawn_cmd(Cmd, Opts)).

get_cmd_result(Port, Acc) ->
    receive
        {Port, {data, Bytes}} ->
            get_cmd_result(Port, [Bytes|Acc]);
        {Port, {exit_status, Status}} ->
            Output = lists:flatten(lists:reverse(Acc)),
            {Status, Output}
    after 0 ->
            timeout
    end.


get_host(Node) when is_atom(Node) ->
    try orddict:fetch(Node, rt_config:get(rt_hosts)) of
        Host -> Host
    catch _:_ ->
        %% Let's try figuring this out from the node name
        node_to_host(Node)
    end;
get_host(Host) -> Host.

get_ip(Node) when is_atom(Node) ->
    get_ip(get_host(Node));
get_ip(Host) ->
    {ok, IP} = inet:getaddr(Host, inet),
    string:join([integer_to_list(X) || X <- tuple_to_list(IP)], ".").

node_id(Node) ->
    NodeMap = rt_config:get(rt_nodes),
    orddict:fetch(Node, NodeMap).

node_version(N) ->
    VersionMap = rt_config:get(rt_versions),
    orddict:fetch(N, VersionMap).

riak(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Result = run_riak(N, Path, Args),
    lager:info("~s", [Result]),
    {ok, Result}.

-spec set_conf(atom() | string(), [{string(), string()}]) -> ok.
set_conf(all, NameValuePairs) ->
    lager:info("rtdev:set_conf(all, ~p)", [NameValuePairs]),
    [ set_conf(DevPath, NameValuePairs) || DevPath <- devpaths()],
    ok;
set_conf(Node, NameValuePairs) when is_atom(Node) ->
    append_to_conf_file(get_riak_conf(Node), NameValuePairs),
    ok;
set_conf(DevPath, NameValuePairs) ->
    [append_to_conf_file(RiakConf, NameValuePairs) || RiakConf <- all_the_files(DevPath, "etc/riak.conf")],
    ok.

whats_up() ->
    io:format("Here's what's running...~n"),

    Up = [rpc:call(Node, os, cmd, ["pwd"]) || Node <- nodes()],
    [io:format("  ~s~n",[string:substr(Dir, 1, length(Dir)-1)]) || Dir <- Up].

riak_admin_cmd(Path, N, Args) ->
    Quoted =
        lists:map(fun(Arg) when is_list(Arg) ->
                          lists:flatten([$", Arg, $"]);
                     (_) ->
                          erlang:error(badarg)
                  end, Args),
    ArgStr = string:join(Quoted, " "),
    ExecName = rt_config:get(exec_name, "riak"),
    io_lib:format("~s/dev/dev~b/bin/~s-admin ~s", [Path, N, ExecName, ArgStr]).

% Private functions

relpath(Vsn) ->
    Path = ?PATH,
    relpath(Vsn, Path).

relpath(Vsn, Paths=[{_,_}|_]) ->
    orddict:fetch(Vsn, orddict:from_list(Paths));
relpath(current, Path) ->
    Path;
relpath(root, Path) ->
    Path;
relpath(_, _) ->
    throw("Version requested but only one path provided").

riakcmd(Path, N, Cmd) ->
    ExecName = rt_config:get(exec_name, "riak"),
    io_lib:format("~s/dev/dev~b/bin/~s ~s", [Path, N, ExecName, Cmd]).

run_riak(N, Path, Cmd) ->
    lager:info("Running: ~s", [riakcmd(Path, N, Cmd)]),
    R = os:cmd(riakcmd(Path, N, Cmd)),
    case Cmd of
        "start" ->
            rt_cover:maybe_start_on_node(?DEV(N), node_version(N)),
            %% Intercepts may load code on top of the cover compiled
            %% modules. We'll just get no coverage info then.
            case rt_intercept:are_intercepts_loaded(?DEV(N)) of
                false ->
                    ok = rt_intercept:load_intercepts([?DEV(N)]);
                true ->
                    ok
            end,
            R;
        "stop" ->
            rt_cover:maybe_stop_on_node(?DEV(N)),
            R;
        _ ->
            R
    end.

append_to_conf_file(File, NameValuePairs) ->
    Settings = lists:flatten(
        [io_lib:format("~n~s = ~s~n", [Name, Value]) || {Name, Value} <- NameValuePairs]),
    file:write_file(File, Settings, [append]).

get_riak_conf(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    io_lib:format("~s/dev/dev~b/etc/riak.conf", [Path, N]).

all_the_files(DevPath, File) ->
    case filelib:is_dir(DevPath) of
        true ->
            Wildcard = io_lib:format("~s/dev/dev*/~s", [DevPath, File]),
            filelib:wildcard(Wildcard);
        _ ->
            lager:debug("~s is not a directory.", [DevPath]),
            []
    end.

devpaths() ->
    lists:usort([ DevPath || {_Name, DevPath} <- proplists:delete(root, rt_config:get(rtdev_path))]).

create_dirs(Version, NodeIds) ->
    VersionPath = filename:join(?PATH, Version),
    Snmp = [filename:join([VersionPath, NodeId, "data/snmp/agent/db"]) ||
               NodeId <- NodeIds],
    [?assertCmd("mkdir -p " ++ Dir) || Dir <- Snmp].

%% check_node({_N, Version}) ->
%%     case proplists:is_defined(Version, rt_config:get(rtdev_path)) of
%%         true -> ok;
%%         _ ->
%%             lager:error("You don't have Riak ~s installed or configured", [Version]),
%%             erlang:error("You don't have Riak " ++ atom_to_list(Version) ++ " installed or configured")
%%     end.

%% add_default_node_config(Nodes) ->
%%     case rt_config:get(rt_default_config, undefined) of
%%         undefined -> ok;
%%         Defaults when is_list(Defaults) ->
%%             rt:pmap(fun(Node) ->
%%                             rt_config:update_app_config(Node, Defaults)
%%                     end, Nodes),
%%             ok;
%%         BadValue ->
%%             lager:error("Invalid value for rt_default_config : ~p", [BadValue]),
%%             throw({invalid_config, {rt_default_config, BadValue}})
%%     end.

%% node_path(Node) ->
%%     N = node_id(Node),
%%     Path = relpath(node_version(N)),
%%     lists:flatten(io_lib:format("~s/dev/dev~b", [Path, N])).

set_advanced_conf(all, NameValuePairs) ->
    lager:info("rtdev:set_advanced_conf(all, ~p)", [NameValuePairs]),
    [ set_advanced_conf(DevPath, NameValuePairs) || DevPath <- devpaths()],
    ok;
set_advanced_conf(Node, NameValuePairs) when is_atom(Node) ->
    append_to_conf_file(get_advanced_riak_conf(Node), NameValuePairs),
    ok;
set_advanced_conf(DevPath, NameValuePairs) ->
    [update_app_config_file(RiakConf, NameValuePairs) || RiakConf <- all_the_files(DevPath, "etc/advanced.config")],
    ok.

get_advanced_riak_conf(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    io_lib:format("~s/dev/dev~b/etc/advanced.config", [Path, N]).

update_app_config_file(ConfigFile, Config) ->
    lager:info("rtdev:update_app_config_file(~s, ~p)", [ConfigFile, Config]),

    BaseConfig = case file:consult(ConfigFile) of
        {ok, [ValidConfig]} ->
            ValidConfig;
        {error, enoent} ->
            []
    end,
    MergeA = orddict:from_list(Config),
    MergeB = orddict:from_list(BaseConfig),
    NewConfig =
        orddict:merge(fun(_, VarsA, VarsB) ->
                              MergeC = orddict:from_list(VarsA),
                              MergeD = orddict:from_list(VarsB),
                              orddict:merge(fun(_, ValA, _ValB) ->
                                                    ValA
                                            end, MergeC, MergeD)
                      end, MergeA, MergeB),
    NewConfigOut = io_lib:format("~p.", [NewConfig]),
    ?assertEqual(ok, file:write_file(ConfigFile, NewConfigOut)),
    ok.

%% TODO: This made sense in an earlier iteration, but probably is no
%% longer needed. Original idea was to provide a place for setup that
%% was general to all harnesses to happen.
setup_harness(VersionMap, NodeIds, NodeMap) ->
    %% rt_config:set(rt_nodes, Nodes),
    %% rt_config:set(rt_nodes_available, Nodes),
    %% rt_config:set(rt_version_map, VersionMap),
    %% rt_config:set(rt_versions, VersionMap),
    %% [create_dirs(Version, VersionNodes) || {Version, VersionNodes} <- VersionMap],
    {NodeIds, NodeMap, VersionMap}.

%% %% @doc Stop nodes and wipe out their data directories
%% stop_and_clean_nodes(Nodes, Version) when is_list(Nodes) ->
%%     [rt_node:stop_and_wait(Node) || Node <- Nodes],
%%     clean_data_dir(Nodes).

%% clean_data_dir(Nodes) ->
%%     clean_data_dir(Nodes, "").

%% clean_data_dir(Nodes, SubDir) when not is_list(Nodes) ->
%%     clean_data_dir([Nodes], SubDir);
%% clean_data_dir(Nodes, SubDir) when is_list(Nodes) ->
%%     rt_harness:clean_data_dir(Nodes, SubDir).
