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

run_git(Path, Cmd) ->
    lager:debug("Running: ~s", [gitcmd(Path, Cmd)]),
    os:cmd(gitcmd(Path, Cmd)).

run_riak(N, Path, Cmd) ->
    %% io:format("~p~n", [riakcmd(Path, N, Cmd)]),
    %%?debugFmt("RR: ~p~n", [[N,Path,Cmd]]),
    %%?debugFmt("~p~n", [os:cmd(riakcmd(Path, N, Cmd))]).
    lager:info("Running: ~s", [riakcmd(Path, N, Cmd)]),
    os:cmd(riakcmd(Path, N, Cmd)).

setup_harness(_Test, _Args) ->
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

update_app_config(Node, Config) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    ConfigFile = io_lib:format("~s/dev/dev~b/etc/app.config", [Path, N]),
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

deploy_nodes(NumNodes) ->
    Versions = [current || _ <- lists:seq(1,NumNodes)],
    deploy_nodes(NumNodes, Versions).

deploy_nodes(NumNodes, Versions) ->
    Path = relpath(root),
    lager:info("Riak path: ~p", [Path]),
    NodesN = lists:seq(1, NumNodes),
    Nodes = [?DEV(N) || N <- NodesN],
    NodeMap = orddict:from_list(lists:zip(Nodes, NodesN)),
    VersionMap = lists:zip(NodesN, Versions),
    rt:set_config(rt_nodes, NodeMap),
    rt:set_config(rt_versions, VersionMap),

    %% Stop nodes if already running
    %% [run_riak(N, relpath(node_version(N)), "stop") || N <- Nodes],
    pmap(fun(N) -> run_riak(N, relpath(node_version(N)), "stop") end, NodesN),
    %% ?debugFmt("Shutdown~n", []),

    %% Reset nodes to base state
    lager:info("Resetting nodes to fresh state"),
    %% run_git(Path, "status"),
    run_git(Path, "reset HEAD --hard"),
    run_git(Path, "clean -fd"),
    %% run_git(Path, "status"),
    %% ?debugFmt("Reset~n", []),

    %% Start nodes
    %%[run_riak(N, relpath(node_version(N)), "start") || N <- Nodes],
    pmap(fun(N) -> run_riak(N, relpath(node_version(N)), "start") end, NodesN),

    %% Ensure nodes started
    [ok = rt:wait_until_pingable(N) || N <- Nodes],

    %% %% Enable debug logging
    %% [rpc:call(N, lager, set_loglevel, [lager_console_backend, debug]) || N <- Nodes],

    %% Ensure nodes are singleton clusters
    [ok = rt:check_singleton_node(?DEV(N)) || {N, Version} <- VersionMap,
                                              Version /= "0.14.2"],

    lager:info("Deployed nodes: ~p", [Nodes]),
    Nodes.

stop(Node) ->
    N = node_id(Node),
    run_riak(N, relpath(node_version(N)), "stop"),
    ok.

start(Node) ->
    N = node_id(Node),
    run_riak(N, relpath(node_version(N)), "start"),
    ok.

node_id(Node) ->
    NodeMap = rt:config(rt_nodes),
    orddict:fetch(Node, NodeMap).

node_version(N) ->
    VersionMap = rt:config(rt_versions),
    orddict:fetch(N, VersionMap).

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
