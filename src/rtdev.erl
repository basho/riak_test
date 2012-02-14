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
    io_lib:format("git --git-dir=\"~s/dev/.git\" --work-tree=\"~s/dev\" ~s",
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

update_app_config(Node, Config) ->
    N = node_id(Node),
    ConfigFile = io_lib:format("~s/dev/dev~b/etc/app.config", [?PATH, N]),
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
    Path = ?PATH,
    lager:info("Riak path: ~p", [Path]),
    NodesN = lists:seq(1, NumNodes),
    Nodes = [?DEV(N) || N <- NodesN],
    NodeMap = orddict:from_list(lists:zip(Nodes, NodesN)),
    rt:set_config(rt_nodes, NodeMap),

    %% Stop nodes if already running
    %% [run_riak(N, Path, "stop") || N <- Nodes],
    %%rpc:pmap({?MODULE, run_riak}, [Path, "stop"], Nodes),
    pmap(fun(N) -> run_riak(N, Path, "stop") end, NodesN),
    %% ?debugFmt("Shutdown~n", []),

    %% Reset nodes to base state
    lager:info("Resetting nodes to fresh state"),
    %% run_git(Path, "status"),
    run_git(Path, "reset HEAD --hard"),
    run_git(Path, "clean -fd"),
    %% run_git(Path, "status"),
    %% ?debugFmt("Reset~n", []),

    %% Start nodes
    %%[run_riak(N, Path, "start") || N <- Nodes],
    %%rpc:pmap({?MODULE, run_riak}, [Path, "start"], Nodes),
    pmap(fun(N) -> run_riak(N, Path, "start") end, NodesN),

    %% Ensure nodes started
    [ok = rt:wait_until_pingable(N) || N <- Nodes],

    %% %% Enable debug logging
    %% [rpc:call(N, lager, set_loglevel, [lager_console_backend, debug]) || N <- Nodes],

    %% Ensure nodes are singleton clusters
    [ok = rt:check_singleton_node(N) || N <- Nodes],

    lager:info("Deployed nodes: ~p", [Nodes]),
    Nodes.

stop(Node) ->
    run_riak(node_id(Node), ?PATH, "stop"),
    ok.

start(Node) ->
    run_riak(node_id(Node), ?PATH, "start"),
    ok.

node_id(Node) ->
    NodeMap = rt:config(rt_nodes),
    orddict:fetch(Node, NodeMap).

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
