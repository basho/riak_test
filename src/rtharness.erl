-module(rtharness).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

setup_harness(_Test, [Platform]) ->
    lager:info("Using stableboy platform: ~p", [Platform]),
    h:process_option({platform, Platform}, [], []),
    Env = basho_harness_app:get_env(env),
    TmpEnv = basho_harness_app:get_env(tmp_env),
    ok = application:start(crypto),
    ok = application:start(ssh),
    basho_harness_app:load_dot_config(),
    stableboy_harness_factory:start_link(Env, TmpEnv),
    ok = riak_harness:start([]),
    ok.

load_project(Project) ->
    application:set_env(basho_harness, project_file, Project),
    basho_harness_app:load_cli_opts().

cleanup_harness() ->
    ok.

spawn_cmd(_Cmd) ->
    throw(todo).

wait_for_cmd(_Idx) ->
    throw(todo).

deploy_nodes(NodeConfig) ->
    %% Need to change in the future to support Riak versions in config
    NumNodes = length(NodeConfig),
    {ok, VMs} = stableboy_harness_factory:create_vms(NumNodes),
    %% lager:info("VMs: ~p", [VMs]),
    NodeMap = [begin
                   IP = proplists:get_value(ip, H:environment()),
                   Name = list_to_atom("riak@" ++ IP),
                   {Name, H}
               end || H <- VMs],
    {Nodes, _} = lists:unzip(NodeMap),

    case (length(Nodes) < NumNodes) of
        true ->
            lager:info("Not enough basho_harness nodes available"),
            throw(not_enough_nodes);
        false ->
            Nodes2 = lists:sublist(Nodes, NumNodes),
            deploy_nodes(NodeConfig, NodeMap, Nodes2)
    end.

deploy_nodes(NodeConfig, NodeMap, Nodes) ->
    {Versions, Configs} = lists:unzip(NodeConfig),
    VersionMap = lists:zip(Nodes, Versions),
    rt:set_config(rt_nodes, NodeMap),
    rt:set_config(rt_versions, VersionMap),

    {_, Hs} = lists:unzip(NodeMap),

    lager:info("Re-installing riak on nodes"),
    [begin
         load_version(node_version(Node)),
         ok = riak_harness:uninstall(node_harness(Node)),
         ok = riak_harness:install(node_harness(Node))
     end || Node <- Nodes],

    lager:info("Exposing Riak to outside hosts"),
    [ok = riak_harness:expose(H) || H <- Hs],

    %% Set initial config
    rt:pmap(fun({_, default}) ->
                    ok;
               ({Node, Config}) ->
                    update_app_config(Node, Config)
            end,
            lists:zip(Nodes, Configs)),

    %% Start nodes
    lager:info("Starting riak on nodes"),
    [ok = riak_harness:start_node(H) || H <- Hs],

    %% Ensure nodes started
    [ok = rt:wait_until_pingable(N) || N <- Nodes],

    %% %% %% Enable debug logging
    %% %% [rpc:call(N, lager, set_loglevel, [lager_console_backend, debug]) || N <- Nodes],

    %% Ensure nodes are singleton clusters
    [ok = rt:check_singleton_node(N) || N <- Nodes,
                                        node_version(N) /= "0.14.2"],

    lager:info("Deployed nodes: ~p", [Nodes]),
    Nodes.

load_version(Vsn) ->
    Project = find_project(Vsn, rt:config(rtharness_projects)),
    HarnessPath = rt:config(rtharness_path),
    ProjectPath = HarnessPath ++ "/projects/" ++ Project,
    lager:info("Project: ~p", [ProjectPath]),
    load_project(ProjectPath),
    ok.

find_project(Vsn, Projects=[{_,_}|_]) ->
    orddict:fetch(Vsn, orddict:from_list(Projects));
find_project(current, Project) ->
    Project;
find_project(_, _) ->
    throw("Version requested but only one project provided").

node_harness(Node) ->
    NodeMap = rt:config(rt_nodes),
    orddict:fetch(Node, NodeMap).

node_version(Node) ->
    VersionMap = rt:config(rt_versions),
    orddict:fetch(Node, VersionMap).

start(Node) ->
    H = node_harness(Node),
    ok = H:cmd("riak start").

stop(Node) ->
    H = node_harness(Node),
    ok = H:cmd("riak stop").

command(Cmd, Args) ->
    Quoted =
        lists:map(fun(Arg) when is_list(Arg) ->
                          lists:flatten([$", Arg, $"]);
                     (_) ->
                          erlang:error(badarg)
                  end, Args),
    ArgStr = string:join(Quoted, " "),
    io_lib:format("~s ~s", [Cmd, ArgStr]).

admin(Node, Args) ->
    Cmd = command("riak-admin", Args),
    lager:debug("Running: ~s", [Cmd]),
    H = node_harness(Node),
    ok = H:cmd(Cmd).

rsync(_Node, _Source, _Dest) ->
    throw(todo).

update_app_config(Node, Config) ->
    %% riak_harness expects flat config
    %% ie. riak_test uses [{riak_core, [{A, B}, {X, Y}]}]
    %%     harness   wants [{riak_core, A, B}, {riak_core, X, Y}]
    FlatConfig = [{App, Var, Value} || {App, Vars} <- Config,
                                       {Var, Value} <- Vars],
    riak_harness:configure(node_harness(Node), FlatConfig),
    ok.

upgrade(Node, NewVersion) ->
    Version = node_version(Node),
    lager:info("Upgrading ~p : ~p -> ~p", [Node, Version, NewVersion]),
    stop(Node),
    load_version(NewVersion),
    %% Install package on-top of existing Riak install
    ok = riak_harness:install(node_harness(Node)),
    VersionMap = orddict:store(Node, NewVersion, rt:config(rt_versions)),
    rt:set_config(rt_versions, VersionMap),
    start(Node),
    ok.
