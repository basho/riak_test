%% @private
-module(rtdev).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(PATH, (rt:config(rtdev_path))).

riakcmd(Path, N, Cmd) ->
    io_lib:format("~s/dev/dev~b/bin/riak ~s", [Path, N, Cmd]).

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
    io_lib:format("~s/dev/dev~b/bin/riak-admin ~s", [Path, N, ArgStr]).

run_git(Path, Cmd) ->
    lager:debug("Running: ~s", [gitcmd(Path, Cmd)]),
    os:cmd(gitcmd(Path, Cmd)).

run_riak(N, Path, Cmd) ->
    lager:info("Running: ~s", [riakcmd(Path, N, Cmd)]),
    os:cmd(riakcmd(Path, N, Cmd)).

setup_harness(_Test, _Args) ->
    Path = relpath(root),
    %% Stop all discoverable nodes, not just nodes we'll be using for this test.
    RTDevPaths = [ DevPath || {_Name, DevPath} <- proplists:delete(root, rt:config(rtdev_path))],
    rt:pmap(fun(X) -> stop_all(X ++ "/dev") end, RTDevPaths),
    
    %% Reset nodes to base state
    lager:info("Resetting nodes to fresh state"),
    run_git(Path, "reset HEAD --hard"),
    run_git(Path, "clean -fd"),
    ok.

cleanup_harness() ->
    ok.

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

upgrade(Node, NewVersion) ->
    N = node_id(Node),
    Version = node_version(N),
    lager:info("Upgrading ~p : ~p -> ~p", [Node, Version, NewVersion]),
    stop(Node),
    OldPath = relpath(Version),
    NewPath = relpath(NewVersion),
    C1 = io_lib:format("cp -a \"~s/dev/dev~b/data\" \"~s/dev/dev~b\"",
                       [OldPath, N, NewPath, N]),
    C2 = io_lib:format("cp -a \"~s/dev/dev~b/etc\" \"~s/dev/dev~b\"",
                       [OldPath, N, NewPath, N]),
    lager:info("Running: ~s", [C1]),
    os:cmd(C1),
    lager:info("Running: ~s", [C2]),
    os:cmd(C2),
    VersionMap = orddict:store(N, NewVersion, rt:config(rt_versions)),
    rt:set_config(rt_versions, VersionMap),
    start(Node),
    ok.

all_the_app_configs(DevPath) ->
    case filelib:is_dir(DevPath) of
        true ->
            Devs = filelib:wildcard(DevPath ++ "/dev/dev*"),
            [ Dev ++ "/etc/app.config" || Dev <- Devs];
        _ -> 
            lager:debug("~s is not a directory.", [DevPath]),
            []
    end.

update_app_config(all, Config) ->
    lager:info("rtdev:update_app_config(all, ~p)", [Config]),
    [ update_app_config(DevPath, Config) || {_Name, DevPath} <- proplists:delete(root, rt:config(rtdev_path))];
update_app_config(Node, Config) when is_atom(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    ConfigFile = io_lib:format("~s/dev/dev~b/etc/app.config", [Path, N]),
    update_app_config_file(ConfigFile, Config);
update_app_config(DevPath, Config) ->
    [update_app_config_file(AppConfig, Config) || AppConfig <- all_the_app_configs(DevPath)].

update_app_config_file(ConfigFile, Config) ->
    lager:info("rtdev:update_app_config_file(~s, ~p)", [ConfigFile, Config]),
    {ok, [BaseConfig]} = file:consult(ConfigFile),
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
    Backends = lists:usort(
        lists:flatten([ get_backends(DevPath) || {_Name, DevPath} <- proplists:delete(root, rt:config(rtdev_path))])),
    case Backends of
        [riak_kv_bitcask_backend] -> bitcask;
        [riak_kv_eleveldb_backend] -> eleveldb;
        [riak_kv_memory_backend] -> memory;
        [Other] -> Other;
        MoreThanOne -> MoreThanOne
    end.

get_backends(DevPath) ->
    [get_backend(AppConfig) || AppConfig <- all_the_app_configs(DevPath)].

get_backend(AppConfig) ->
    {ok, [Config]} = file:consult(AppConfig),
    kvc:path(riak_kv.storage_backend, Config).

node_path(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    lists:flatten(io_lib:format("~s/dev/dev~b", [Path, N])).

create_dirs(Nodes) ->
    Snmp = [node_path(Node) ++ "/data/snmp/agent/db" || Node <- Nodes],
    [?assertCmd("mkdir -p " ++ Dir) || Dir <- Snmp].

deploy_nodes(NodeConfig) ->
    Path = relpath(root),
    lager:info("Riak path: ~p", [Path]),
    NumNodes = length(NodeConfig),
    NodesN = lists:seq(1, NumNodes),
    Nodes = [?DEV(N) || N <- NodesN],
    NodeMap = orddict:from_list(lists:zip(Nodes, NodesN)),
    {Versions, Configs} = lists:unzip(NodeConfig),
    VersionMap = lists:zip(NodesN, Versions),
    
    %% Check that you have the right versions available
    [ check_node(Version) || Version <- VersionMap ],
    rt:set_config(rt_nodes, NodeMap),
    rt:set_config(rt_versions, VersionMap),

    create_dirs(Nodes),

    %% Set initial config
    rt:pmap(fun({_, default}) ->
                    ok;
               ({Node, Config}) ->
                    update_app_config(Node, Config)
            end,
            lists:zip(Nodes, Configs)),

    %% Start nodes
    %%[run_riak(N, relpath(node_version(N)), "start") || N <- Nodes],
    rt:pmap(fun(N) -> run_riak(N, relpath(node_version(N)), "start") end, NodesN),

    %% Ensure nodes started
    [ok = rt:wait_until_pingable(N) || N <- Nodes],

    %% %% Enable debug logging
    %% [rpc:call(N, lager, set_loglevel, [lager_console_backend, debug]) || N <- Nodes],

    %% Ensure nodes are singleton clusters
    [ok = rt:check_singleton_node(?DEV(N)) || {N, Version} <- VersionMap,
                                              Version /= "0.14.2"],

    lager:info("Deployed nodes: ~p", [Nodes]),
    Nodes.

stop_all(DevPath) ->
    case filelib:is_dir(DevPath) of
        true ->
            Devs = filelib:wildcard(DevPath ++ "/dev*"),
            %% Works, but I'd like it to brag a little more about it.
            Stop = fun(C) ->
                Cmd = C ++ "/bin/riak stop",
                [Output | _Tail] = string:tokens(os:cmd(Cmd), "\n"),
                Status = case Output of
                    "ok" -> "ok";
                    _ -> "wasn't running"
                end,
                lager:debug("Stopping Node... ~s ~~ ~s.", [Cmd, Status])
            end,
            rt:pmap(Stop, Devs);
        _ -> lager:debug("~s is not a directory.", [DevPath])
    end,
    ok.
    
stop(Node) ->
    N = node_id(Node),
    run_riak(N, relpath(node_version(N)), "stop"),
    ok.

start(Node) ->
    N = node_id(Node),
    run_riak(N, relpath(node_version(N)), "start"),
    ok.

admin(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = riak_admin_cmd(Path, N, Args),
    lager:debug("Running: ~s", [Cmd]),
    Result = os:cmd(Cmd),
    lager:debug("~s", [Result]),
    io:format("~s", [Result]),
    {ok, Result}.

node_id(Node) ->
    NodeMap = rt:config(rt_nodes),
    orddict:fetch(Node, NodeMap).

node_version(N) ->
    VersionMap = rt:config(rt_versions),
    orddict:fetch(N, VersionMap).

spawn_cmd(Cmd) ->
    Port = open_port({spawn, Cmd}, [stream, in, exit_status]),
    Port.

wait_for_cmd(Port) ->
    rt:wait_until(node(),
                  fun(_) ->
                          receive
                              {Port, Msg={data, _}} ->
                                  self() ! {Port, Msg},
                                  false;
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
    wait_for_cmd(spawn_cmd(Cmd)).

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
    case proplists:is_defined(Version, rt:config(rtdev_path)) of
        true -> ok;
        _ ->
            lager:error("You don't have Riak ~s installed", [Version]), 
            erlang:error("You don't have Riak " ++ Version ++ " installed" )
    end.

set_backend(Backend) ->
    lager:info("rtdev:set_backend(~p)", [Backend]),
    update_app_config(all, [{riak_kv, [{storage_backend, Backend}]}]),
    get_backends().
    
get_version() ->
    case file:read_file(relpath(current) ++ "/VERSION") of
        {error, enoent} -> unknown;
        {ok, Version} -> Version
    end.

teardown() ->
    %% Stop all discoverable nodes, not just nodes we'll be using for this test.
    RTDevPaths = [ DevPath || {_Name, DevPath} <- proplists:delete(root, rt:config(rtdev_path))],
    rt:pmap(fun(X) -> stop_all(X ++ "/dev") end, RTDevPaths).

whats_up() ->
    io:format("Here's what's running...~n"),
    
    Up = [rpc:call(Node, os, cmd, ["pwd"]) || Node <- nodes()],
    [io:format("  ~s~n",[string:substr(Dir, 1, length(Dir)-1)]) || Dir <- Up].

