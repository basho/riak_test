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

-module(rtdev).
%% -behaviour(test_harness).
-export([start/2,
         stop/2,
         deploy_clusters/1,
         clean_data_dir/2,
         clean_data_dir/3,
         spawn_cmd/1,
         spawn_cmd/2,
         cmd/1,
         cmd/2,
         setup_harness/0,
         %% setup_harness/2,
         get_version/0,
         get_backends/0,
         set_backend/1,
         whats_up/0,
         get_ip/1,
         node_id/1,
         node_version/1,
         admin/2,
         riak/2,
         attach/2,
         attach_direct/2,
         console/2,
         update_app_config/3,
         teardown/0,
         set_conf/2,
         set_advanced_conf/2,
         rm_dir/1]).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(DEVS(N), lists:concat([N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(PATH, (rt_config:get(root_path))).
-define(SCRATCH_DIR, (rt_config:get(rt_scratch_dir))).

get_deps() ->
    lists:flatten(io_lib:format("~s/dev1/lib", [filename:join(?PATH, "head")])).

riakcmd(Path, N, Cmd) ->
    ExecName = rt_config:get(exec_name, "riak"),
    io_lib:format("~s/~s/bin/~s ~s", [Path, N, ExecName, Cmd]).

riakreplcmd(Path, N, Cmd) ->
    io_lib:format("~s/~s/bin/riak-repl ~s", [Path, N, Cmd]).

gitcmd(Path, Cmd) ->
    io_lib:format("git --git-dir=\"~s/.git\" --work-tree=\"~s/\" ~s",
                  [Path, Path, Cmd]).

riak_admin_cmd(Path, N, Args) ->
    Quoted =
        lists:map(fun(Arg) when is_list(Arg) ->
                          lists:flatten([$", Arg, $"]);
                     (_) ->
                          erlang:error(badarg)
                  end, Args),
    ArgStr = string:join(Quoted, " "),
    ExecName = rt_config:get(exec_name, "riak"),
    io_lib:format("~s/~s/bin/~s-admin ~s", [Path, N, ExecName, ArgStr]).

run_git(Path, Cmd) ->
    lager:info("Running: ~s", [gitcmd(Path, Cmd)]),
    {0, Out} = cmd(gitcmd(Path, Cmd)),
    Out.

run_riak(Node, Version, "start") ->
    VersionPath = filename:join(?PATH, Version),
    RiakCmd = riakcmd(VersionPath, Node, "start"),
    lager:info("Running: ~s", [RiakCmd]),
    CmdRes = os:cmd(RiakCmd),
    %% rt_cover:maybe_start_on_node(?DEV(Node), Version),
    %% Intercepts may load code on top of the cover compiled
    %% modules. We'll just get no coverage info then.
    case rt_intercept:are_intercepts_loaded(?DEV(Node)) of
        false ->
            ok = rt_intercept:load_intercepts([?DEV(Node)]);
        true ->
            ok
    end,
    CmdRes;
run_riak(Node, Version, "stop") ->
    VersionPath = filename:join(?PATH, Version),
    %% rt_cover:maybe_stop_on_node(?DEV(Node)),
    os:cmd(riakcmd(VersionPath, Node, "stop"));
run_riak(Node, Version, Cmd) ->
    VersionPath = filename:join(?PATH, Version),
    os:cmd(riakcmd(VersionPath, Node, Cmd)).

run_riak_repl(N, Path, Cmd) ->
    lager:info("Running: ~s", [riakcmd(Path, N, Cmd)]),
    os:cmd(riakreplcmd(Path, N, Cmd)).
    %% don't mess with intercepts and/or coverage,
    %% they should already be setup at this point

-spec versions() -> [string()].
versions() ->
    RootPath = ?PATH,
    case file:list_dir(RootPath) of
        {ok, RootFiles} ->
            [Version || Version <- RootFiles,
                        filelib:is_dir(filename:join(RootPath, Version)),
                        hd(Version) =/= $.];
        {error, _} ->
            []
    end.

-spec harness_node_ids(string()) -> [string()].
harness_node_ids(Version) ->
    VersionPath = filename:join(?PATH, Version),
    case file:list_dir(VersionPath) of
        {ok, VersionFiles} ->
            SortedVersionFiles = lists:sort(VersionFiles),
            [Node || Node <- SortedVersionFiles,
                     filelib:is_dir(filename:join(VersionPath, Node))];
        {error, _} ->
            []
    end.

-spec harness_nodes([string()]) -> [atom()].
harness_nodes(NodeIds) ->
    [list_to_atom(NodeId ++ "@127.0.0.1") || NodeId <- NodeIds].

so_fresh_so_clean(VersionMap) ->
    %% make sure we stop any cover processes on any nodes otherwise,
    %% if the next test boots a legacy node we'll end up with cover
    %% incompatabilities and crash the cover server
    %% rt_cover:maybe_stop_on_nodes(),
    %% Path = relpath(root),
    %% Stop all discoverable nodes, not just nodes we'll be using for
    %% this test.
    StopAllFun =
        fun({Version, VersionNodes}) ->
                VersionPath = filename:join([?PATH, Version]),
                stop_nodes(VersionPath, VersionNodes)
        end,
    rt:pmap(StopAllFun, VersionMap),

    %% Reset nodes to base state
    lager:info("Resetting nodes to fresh state"),
    _ = run_git(?PATH, "reset HEAD --hard"),
    _ = run_git(?PATH, "clean -fd"),

    lager:info("Cleaning up lingering pipe directories"),
    rt:pmap(fun({Version, _}) ->
                    %% when joining two absolute paths, filename:join intentionally
                    %% throws away the first one. ++ gets us around that, while
                    %% keeping some of the security of filename:join.
                    %% the extra slashes will be pruned by filename:join, but this
                    %% ensures that there will be at least one between "/tmp" and Dir
                    %% TODO: Double check this is correct
                    PipeDir = filename:join("/tmp" ++ ?PATH, Version),
                    {0, _} = cmd("rm -rf " ++ PipeDir)
            end, VersionMap),
    ok.

setup_harness() ->
    %% Get node names and populate node map
    VersionMap = [{Version, harness_node_ids(Version)} || Version <- versions()],
    NodeIds = harness_node_ids(rt_config:get(default_version, "head")),
    NodeMap = lists:zip(NodeIds, harness_nodes(NodeIds)),
    so_fresh_so_clean(VersionMap),
    rm_dir(filename:join(?SCRATCH_DIR, "gc")),
    rt_harness_util:setup_harness(VersionMap, NodeIds, NodeMap).

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

%% upgrade(Node, CurrentVersion, NewVersion) ->
%%     upgrade(Node, CurrentVersion, NewVersion, same).

upgrade(Node, CurrentVersion, NewVersion, Config) ->
    lager:info("Upgrading ~p : ~p -> ~p", [Node, CurrentVersion, NewVersion]),
    stop(Node, CurrentVersion),
    rt:wait_until_unpingable(Node),
    CurrentPath = filename:join([?PATH, CurrentVersion, Node]),
    NewPath = filename:join([?PATH, NewVersion, Node]),
    Commands = [
        io_lib:format("cp -p -P -R \"~s\" \"~s\"",
                       [filename:join(CurrentPath, "data"),
                        NewPath]),
        %% io_lib:format("rm -rf ~s/dev/dev~b/data/*",
        %%                [CurrePath, N]),
        io_lib:format("cp -p -P -R \"~s\" \"~s\"",
                       [filename:join(CurrentPath, "etc"),
                        NewPath])
    ],
    [begin
         lager:debug("Running: ~s", [Cmd]),
         os:cmd(Cmd)
     end || Cmd <- Commands],
    clean_data_dir(Node, CurrentVersion, ""),

    %% VersionMap = orddict:store(N, NewVersion, rt_config:get(rt_versions)),
    %% rt_config:set(rt_versions, VersionMap),

    case Config of
        same -> ok;
        _ -> update_app_config(Node, NewVersion, Config)
    end,
    start(Node, NewVersion),
    rt:wait_until_pingable(Node),
    ok.

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

get_riak_conf(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    io_lib:format("~s/dev/dev~b/etc/riak.conf", [Path, N]).

get_advanced_riak_conf(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    io_lib:format("~s/dev/dev~b/etc/advanced.config", [Path, N]).

append_to_conf_file(File, NameValuePairs) ->
    Settings = lists:flatten(
        [io_lib:format("~n~s = ~s~n", [Name, Value]) || {Name, Value} <- NameValuePairs]),
    file:write_file(File, Settings, [append]).

all_the_files(DevPath, File) ->
    case filelib:is_dir(DevPath) of
        true ->
            Wildcard = io_lib:format("~s/dev/dev*/~s", [DevPath, File]),
            filelib:wildcard(Wildcard);
        _ ->
            lager:debug("~s is not a directory.", [DevPath]),
            []
    end.

all_the_app_configs(DevPath) ->
    AppConfigs = all_the_files(DevPath, "etc/app.config"),
    case length(AppConfigs) =:= 0 of
        true ->
            AdvConfigs = filelib:wildcard(DevPath ++ "/dev/dev*/etc"),
            [ filename:join(AC, "advanced.config") || AC <- AdvConfigs];
        _ ->
            AppConfigs
    end.

%% update_app_config(all, Config) ->
%%     lager:info("rtdev:update_app_config(all, ~p)", [Config]),
%%     [ update_app_config(DevPath, Config) || DevPath <- devpaths()];
update_app_config(Node, Version, Config) ->
    VersionPath = filename:join(?PATH, Version),
    FileFormatString = "~s/~s/etc/~s.config",
    AppConfigFile = io_lib:format(FileFormatString,
                                  [VersionPath, Node, "app"]),
    AdvConfigFile = io_lib:format(FileFormatString,
                                  [VersionPath, Node, "advanced"]),

    %% If there's an app.config, do it old style
    %% if not, use cuttlefish's advanced.config
    case filelib:is_file(AppConfigFile) of
        true ->
            update_app_config_file(AppConfigFile, Config);
        _ ->
            update_app_config_file(AdvConfigFile, Config)
    end.

update_app_config_file(ConfigFile, Config) ->
    lager:debug("rtdev:update_app_config_file(~s, ~p)", [ConfigFile, Config]),

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
get_backends() ->
    lists:usort(
        lists:flatten([ get_backends(DevPath) || DevPath <- devpaths()])).

get_backends(DevPath) ->
    rt:pmap(fun get_backend/1, all_the_app_configs(DevPath)).

get_backend(AppConfig) ->
    lager:info("get_backend(~s)", [AppConfig]),
    Tokens = lists:reverse(filename:split(AppConfig)),
    ConfigFile = case Tokens of
        ["app.config"| _ ] ->
            AppConfig;
        ["advanced.config" | T] ->
            ["etc", [$d, $e, $v | N], "dev" | RPath] = T,
            Path = filename:join(lists:reverse(RPath)),
            %% Why chkconfig? It generates an app.config from cuttlefish
            %% without starting riak.

            ChkConfigOutput = string:tokens(run_riak(list_to_integer(N), Path, "chkconfig"), "\n"),

            ConfigFileOutputLine = lists:last(ChkConfigOutput),

            %% ConfigFileOutputLine looks like this:
            %% -config /path/to/app.config -args_file /path/to/vm.args -vm_args /path/to/vm.args
            Files =[ Filename || Filename <- string:tokens(ConfigFileOutputLine, "\s"),
                                 ".config" == filename:extension(Filename) ],

            case Files of
                [] -> %% No file generated by chkconfig. this isn't great
                    lager:error("Cuttlefish Failure."),
                    lager:info("chkconfig:"),
                    [ lager:info("~s", [Line]) || Line <- ChkConfigOutput ],
                    ?assert(false);
                _ ->
                    File = hd(Files),
                    case filename:pathtype(Files) of
                        absolute -> File;
                        relative ->
                            io_lib:format("~s/dev/dev~s/~s", [Path, N, tl(hd(Files))])
                    end
                end
    end,

    case file:consult(ConfigFile) of
        {ok, [Config]} ->
            rt_backend:get_backend(Config);
        E ->
            lager:error("Error reading ~s, ~p", [ConfigFile, E]),
            error
    end.

node_path(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    lists:flatten(io_lib:format("~s/dev/dev~b", [Path, N])).

get_ip(_Node) ->
    %% localhost 4 lyfe
    "127.0.0.1".

create_dirs(Nodes) ->
    Snmp = [node_path(Node) ++ "/data/snmp/agent/db" || Node <- Nodes],
    [?assertCmd("mkdir -p " ++ Dir) || Dir <- Snmp].

clean_data_dir(Nodes, SubDir) when is_list(Nodes) ->
    DataDirs = [node_path(Node) ++ "/data/" ++ SubDir || Node <- Nodes],
    lists:foreach(fun rm_dir/1, DataDirs).

%% Blocking to delete files is not the best use of time. Generally it
%% is quicker to move directories than to recursively delete them so
%% move the directory to a GC subdirectory in the riak_test scratch
%% directory, recreate the subdirectory, and asynchronously remove the
%% files from the scratch directory.
clean_data_dir(Node, Version, "") ->
    DataDir = filename:join([?PATH, Version, Node, "data"]),
    TmpDir = filename:join([?SCRATCH_DIR, "gc", Version, Node]),
    filelib:ensure_dir(filename:join(TmpDir, "child")),
    mv_dir(DataDir, TmpDir),
    Pid = spawn(?MODULE, rm_dir, [TmpDir]),
    mk_dir(DataDir),
    Pid;
clean_data_dir(Node, Version, SubDir) ->
    DataDir = filename:join([?PATH, Version, Node, "data", SubDir]),
    TmpDir = filename:join([?SCRATCH_DIR, "gc", Version, Node, "data"]),
    filelib:ensure_dir(filename:join(TmpDir, "child")),
    mv_dir(DataDir, TmpDir),
    Pid = spawn(?MODULE, rm_dir, [TmpDir]),
    mk_dir(DataDir),
    Pid.

mk_dir(Dir) ->
    lager:debug("Making directory ~s", [Dir]),
    ?assertCmd("mkdir " ++ Dir),
    ?assertEqual(true, filelib:is_dir(Dir)).

mv_dir(Src, Dest) ->
    lager:debug("Moving directory ~s to ~s", [Src, Dest]),
    ?assertCmd("mv " ++ Src ++ " " ++ Dest),
    ?assertEqual(false, filelib:is_dir(Src)).

rm_dir(Dir) ->
    lager:debug("Removing directory ~s", [Dir]),
    ?assertCmd("rm -rf " ++ Dir),
    ?assertEqual(false, filelib:is_dir(Dir)).

add_default_node_config(Nodes) ->
    case rt_config:get(rt_default_config, undefined) of
        undefined -> ok;
        Defaults when is_list(Defaults) ->
            rt:pmap(fun(Node) ->
                            update_app_config(Node, version_here, Defaults)
                    end, Nodes),
            ok;
        BadValue ->
            lager:error("Invalid value for rt_default_config : ~p", [BadValue]),
            throw({invalid_config, {rt_default_config, BadValue}})
    end.

deploy_clusters(_ClusterConfigs) ->
    ok.
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

configure_nodes(Nodes, Configs) ->
    %% Set initial config
    add_default_node_config(Nodes),
    rt:pmap(fun({_, default}) ->
                    ok;
               ({Node, {cuttlefish, Config}}) ->
                    set_conf(Node, Config);
               ({Node, Config}) ->
                    update_app_config(Node, version_here, Config)
            end,
            lists:zip(Nodes, Configs)).

%% deploy_nodes(NodeConfig) ->
%%     Path = relpath(root),
%%     lager:info("Riak path: ~p", [Path]),
%%     NumNodes = length(NodeConfig),
%%     %% TODO: The starting index should not be fixed to 1
%%     NodesN = lists:seq(1, NumNodes),
%%     Nodes = [?DEV(N) || N <- NodesN],
%%     NodeMap = orddict:from_list(lists:zip(Nodes, NodesN)),
%%     {Versions, Configs} = lists:unzip(NodeConfig),
%%     VersionMap = lists:zip(NodesN, Versions),

%%     %% Check that you have the right versions available
%%     [ check_node(Version) || Version <- VersionMap ],
%%     rt_config:set(rt_nodes, NodeMap),
%%     rt_config:set(rt_versions, VersionMap),

%%     create_dirs(Nodes),

%%     %% Set initial config
%%     add_default_node_config(Nodes),
%%     rt:pmap(fun({_, default}) ->
%%                     ok;
%%                ({Node, {cuttlefish, Config}}) ->
%%                     set_conf(Node, Config);
%%                ({Node, Config}) ->
%%                     update_app_config(Node, Config)
%%             end,
%%             lists:zip(Nodes, Configs)),

%%     %% create snmp dirs, for EE
%%     create_dirs(Nodes),

%%     %% Start nodes
%%     %%[run_riak(N, relpath(node_version(N)), "start") || N <- Nodes],
%%     rt:pmap(fun(N) -> run_riak(N, relpath(node_version(N)), "start") end, NodesN),

%%     %% Ensure nodes started
%%     [ok = rt:wait_until_pingable(N) || N <- Nodes],

%%     %% %% Enable debug logging
%%     %% [rpc:call(N, lager, set_loglevel, [lager_console_backend, debug]) || N <- Nodes],

%%     %% We have to make sure that riak_core_ring_manager is running before we can go on.
%%     [ok = rt:wait_until_registered(N, riak_core_ring_manager) || N <- Nodes],

%%     %% Ensure nodes are singleton clusters
%%     [ok = rt_ring:check_singleton_node(?DEV(N)) || {N, Version} <- VersionMap,
%%                                               Version /= "0.14.2"],

%%     lager:info("Deployed nodes: ~p", [Nodes]),
%%     Nodes.

gen_stop_fun(Path, Timeout) ->
    fun(Node) ->
            NodeName = ?DEV(Node),
            NodePath = filename:join(Path, Node),
            net_kernel:hidden_connect_node(NodeName),
            case rpc:call(NodeName, os, getpid, []) of
                PidStr when is_list(PidStr) ->
                    lager:debug("Preparing to stop node ~p (process ID ~s) with init:stop/0...",
                               [NodePath, PidStr]),
                    rpc:call(NodeName, init, stop, []),
                    %% If init:stop/0 fails here, the wait_for_pid/2 call
                    %% below will timeout and the process will get cleaned
                    %% up by the kill_stragglers/2 function
                    wait_for_pid(PidStr, Timeout);
                BadRpc ->
                    Cmd =  filename:join([Path, Node, "bin/riak stop"]),
                    lager:debug("RPC to node ~p returned ~p, will try stop anyway... ~s",
                               [NodeName, BadRpc, Cmd]),
                    Output = os:cmd(Cmd),
                    Status = case Output of
                                 "ok\n" ->
                                     %% Telling the node to stop worked,
                                     %% but now we must wait the full node
                                     %% shutdown_time to allow it to
                                     %% properly shut down, otherwise it
                                     %% will get prematurely killed by
                                     %% kill_stragglers/2 below.
                                     timer:sleep(Timeout),
                                     "ok";
                                 _ ->
                                     "wasn't running"
                             end,
                    lager:debug("Stopped node ~p, stop status: ~s.", [NodePath, Status])
            end
    end.

kill_stragglers(Path, Timeout) ->
    {ok, Re} = re:compile("^\\s*\\S+\\s+(\\d+).+\\d+\\s+"++Path++"\\S+/beam"),
    ReOpts = [{capture,all_but_first,list}],
    Pids = tl(string:tokens(os:cmd("ps -ef"), "\n")),
    Fold = fun(Proc, Acc) ->
                   case re:run(Proc, Re, ReOpts) of
                       nomatch ->
                           Acc;
                       {match,[Pid]} ->
                           lager:debug("Process ~s still running, killing...",
                                      [Pid]),
                           os:cmd("kill -15 "++Pid),
                           case wait_for_pid(Pid, Timeout) of
                               ok -> ok;
                               fail ->
                                   lager:debug("Process ~s still hasn't stopped, "
                                              "resorting to kill -9...", [Pid]),
                                   os:cmd("kill -9 "++Pid)
                           end,
                           [Pid|Acc]
                   end
           end,
    lists:foldl(Fold, [], Pids).

wait_for_pid(PidStr, Timeout) ->
    F = fun() ->
                os:cmd("kill -0 "++PidStr) =/= []
        end,
    Retries = Timeout div 1000,
    case rt:wait_until(F, Retries, 1000) of
        {fail, _} -> fail;
        _ -> ok
    end.

stop_nodes(Path, Nodes) ->
    MyNode = 'riak_test@127.0.0.1',
    case net_kernel:start([MyNode, longnames]) of
        {ok, _} ->
            true = erlang:set_cookie(MyNode, riak);
        {error,{already_started,_}} ->
            ok
    end,
    lager:debug("Trying to obtain node shutdown_time via RPC..."),
    Tmout = case rpc:call(?DEV(hd(Nodes)), init, get_argument, [shutdown_time]) of
                {ok,[[Tm]]} -> list_to_integer(Tm)+10000;
                _ -> 20000
            end,
    lager:debug("Using node shutdown_time of ~w", [Tmout]),
    rt:pmap(gen_stop_fun(Path, Tmout), Nodes),
    kill_stragglers(Path, Tmout),
    ok.

stop(Node, Version) ->
    case rpc:call(?DEV(Node), os, getpid, []) of
        {badrpc, nodedown} ->
            ok;
        RiakPid ->
            %% rt_cover:maybe_stop_on_node(Node),
            run_riak(Node, Version, "stop"),
            F = fun(_N) ->
                        os:cmd("kill -0 " ++ RiakPid) =/= []
                end,
            ?assertEqual(ok, rt:wait_until(?DEV(Node), F)),
            ok
    end.

start(Node, Version) ->
    %% N = node_id(Node),
    run_riak(Node, Version, "start"),
    ok.

attach(Node, Expected) ->
    interactive(Node, "attach", Expected).

attach_direct(Node, Expected) ->
    interactive(Node, "attach-direct", Expected).

console(Node, Expected) ->
    interactive(Node, "console", Expected).

interactive(Node, Command, Exp) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = riakcmd(Path, N, Command),
    lager:debug("Opening a port for riak ~s.", [Command]),
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

admin(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = riak_admin_cmd(Path, N, Args),
    lager:info("Running: ~s", [Cmd]),
    Result = os:cmd(Cmd),
    lager:info("~s", [Result]),
    {ok, Result}.

riak(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Result = run_riak(N, Path, Args),
    lager:info("~s", [Result]),
    {ok, Result}.


riak_repl(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Result = run_riak_repl(N, Path, Args),
    lager:info("~s", [Result]),
    {ok, Result}.

node_id(Node) ->
    NodeMap = rt_config:get(rt_nodes),
    orddict:fetch(Node, NodeMap).

node_version(N) ->
    VersionMap = rt_config:get(rt_versions),
    orddict:fetch(N, VersionMap).

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

check_node({_N, Version}) ->
    case proplists:is_defined(Version, rt_config:get(rtdev_path)) of
        true -> ok;
        _ ->
            lager:error("You don't have Riak ~s installed or configured", [Version]),
            erlang:error("You don't have Riak " ++ atom_to_list(Version) ++ " installed or configured")
    end.

set_backend(Backend) ->
    set_backend(Backend, []).

set_backend(Backend, OtherOpts) ->
    lager:debug("rtdev:set_backend(~p, ~p)", [Backend, OtherOpts]),
    Opts = [{storage_backend, Backend} | OtherOpts],
    update_app_config(all, version_here, [{riak_kv, Opts}]),
    get_backends().

get_version() ->
    case file:read_file(relpath(current) ++ "/VERSION") of
        {error, enoent} -> unknown;
        {ok, Version} -> Version
    end.

teardown() ->
    %% rt_cover:maybe_stop_on_nodes(),
    %% Stop all discoverable nodes, not just nodes we'll be using for this test.
    %% rt:pmap(fun(X) -> stop_all(X ++ "/dev") end, devpaths()).
    ok.

whats_up() ->
    io:format("Here's what's running...~n"),

    Up = [rpc:call(Node, os, cmd, ["pwd"]) || Node <- nodes()],
    [io:format("  ~s~n",[string:substr(Dir, 1, length(Dir)-1)]) || Dir <- Up].

devpaths() ->
    lists:usort([ DevPath || {_Name, DevPath} <- proplists:delete(root, rt_config:get(rtdev_path))]).

%% versions() ->
%%     proplists:get_keys(rt_config:get(rtdev_path)) -- [root].

get_node_logs() ->
    Root = filename:absname(proplists:get_value(root, ?PATH)),
    RootLen = length(Root) + 1, %% Remove the leading slash
    [ begin
          {ok, Port} = file:open(Filename, [read, binary]),
          {lists:nthtail(RootLen, Filename), Port}
      end || Filename <- filelib:wildcard(Root ++ "/*/dev/dev*/log/*") ].
