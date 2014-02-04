-module(rtssh).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

get_version() ->
    unknown.

get_deps() ->
    "deps".

setup_harness(_Test, _Args) ->
    Path = relpath(root),
    Hosts = load_hosts(),
    Bench = load_bench(),
    rt:set_config(rt_hostnames, Hosts),
    %% [io:format("R: ~p~n", [wildcard(Host, "/tmp/*")]) || Host <- Hosts],

    case rt:config(rtssh_bench) of
        undefined ->
            ok;
        BenchPath ->
            code:add_path(BenchPath ++ "/ebin"),
            riak_test:add_deps(BenchPath ++ "/deps")
    end,

    sync_bench(Bench),
    sync_proxy(Bench),

    %% Stop all discoverable nodes, not just nodes we'll be using for this test.
    stop_all(Hosts),
    stop_all_bench(Bench),

    %% Reset nodes to base state
    lager:info("Resetting nodes to fresh state"),
    rt:pmap(fun(Host) ->
                    run_git(Host, Path, "reset HEAD --hard"),
                    run_git(Host, Path, "clean -fd")
            end, Hosts),
    ok.

get_backends() ->
    [].

cmd(Cmd) ->
    cmd(Cmd, []).

cmd(Cmd, Opts) ->
    wait_for_cmd(spawn_cmd(Cmd, Opts)).

deploy_nodes(NodeConfig) ->
    Hosts = rt:config(rtssh_hosts),
    NumNodes = length(NodeConfig),
    NumHosts = length(Hosts),
    case NumNodes > NumHosts of
        true ->
            erlang:error("Not enough hosts available to deploy nodes",
                         [NumNodes, NumHosts]);
        false ->
            Hosts2 = lists:sublist(Hosts, NumNodes),
            deploy_nodes(NodeConfig, Hosts2)
    end.

deploy_nodes(NodeConfig, Hosts) ->
    Path = relpath(root),
    lager:info("Riak path: ~p", [Path]),
    %% NumNodes = length(NodeConfig),
    %% NodesN = lists:seq(1, NumNodes),
    %% Nodes = [?DEV(N) || N <- NodesN],
    Nodes = [list_to_atom("dev1@" ++ Host) || Host <- Hosts],
    HostMap = lists:zip(Nodes, Hosts),

    %% NodeMap = orddict:from_list(lists:zip(Nodes, NodesN)),
    %% TODO: Add code to set initial app.config
    {Versions, Configs} = lists:unzip(NodeConfig),
    VersionMap = lists:zip(Nodes, Versions),

    rt:set_config(rt_hosts, HostMap),
    rt:set_config(rt_versions, VersionMap),

    %% io:format("~p~n", [Nodes]),

    rt:pmap(fun({_, default}) ->
                    ok;
               ({Node, Config}) ->
                    update_app_config(Node, Config)
            end,
            lists:zip(Nodes, Configs)),
    timer:sleep(500),

    rt:pmap(fun(Node) ->
                    Host = get_host(Node),
                    IP = get_ip(Host),
                    Config = [{riak_api, [{pb, fun([{_, Port}]) ->
                                                       [{IP, Port}]
                                               end},
                                          {pb_ip, fun(_) ->
                                                          IP
                                                  end}]},
                              {riak_core, [{http, fun([{_, Port}]) ->
                                                          [{IP, Port}]
                                                  end}]}],
                    update_app_config(Node, Config)
            end, Nodes),
    timer:sleep(500),

    rt:pmap(fun(Node) ->
                    update_vm_args(Node, [{"-name", Node}])
            end, Nodes),
    timer:sleep(500),

    rt:pmap(fun start/1, Nodes),

    Nodes.

start(Node) ->
    run_riak(Node, "start"),
    ok.

stop(Node) ->
    run_riak(Node, "stop"),
    ok.

run_riak(Node, Cmd) ->
    Exec = riakcmd(Node, Cmd),
    lager:info("Running: ~s :: ~s", [get_host(Node), Exec]),
    ssh_cmd(Node, Exec).

run_git(Host, Path, Cmd) ->
    Exec = gitcmd(Path, Cmd),
    lager:info("Running: ~s :: ~s", [Host, Exec]),
    ssh_cmd(Host, Exec).

admin(Node, Args) ->
    Cmd = riak_admin_cmd(Node, Args),
    lager:info("Running: ~s :: ~s", [get_host(Node), Cmd]),
    {0, Result} = ssh_cmd(Node, Cmd),
    lager:info("~s", [Result]),
    {ok, Result}.

riak(Node, Args) ->
    Result = run_riak(Node, Args),
    lager:info("~s", [Result]),
    {ok, Result}.

riakcmd(Node, Cmd) ->
    node_path(Node) ++ "/bin/riak " ++ Cmd.

gitcmd(Path, Cmd) ->
    io_lib:format("git --git-dir=\"~s/.git\" --work-tree=\"~s/\" ~s",
                  [Path, Path, Cmd]).

riak_admin_cmd(Node, Args) ->
    Quoted =
        lists:map(fun(Arg) when is_list(Arg) ->
                          lists:flatten([$", Arg, $"]);
                     (_) ->
                          erlang:error(badarg)
                  end, Args),
    ArgStr = string:join(Quoted, " "),
    node_path(Node) ++ "/bin/riak-admin " ++ ArgStr.

load_hosts() ->
    {HostsIn, Aliases} = read_hosts_file("hosts"),
    Hosts = lists:sort(HostsIn),
    rt:set_config(rtssh_hosts, Hosts),
    rt:set_config(rtssh_aliases, Aliases),
    Hosts.

load_bench() ->
    {HostsIn, _Aliases} = read_hosts_file("bench"),
    Hosts = lists:sort(HostsIn),
    rt:set_config(rtssh_bench_hosts, Hosts),
    io:format("Bench: ~p~n", [Hosts]),
    Hosts.

read_hosts_file(File) ->
    case file:consult(File) of
        {ok, Terms} ->
            lists:mapfoldl(fun({Alias, Host}, Aliases) ->
                                   Aliases2 = orddict:store(Host, Host, Aliases),
                                   Aliases3 = orddict:store(Alias, Host, Aliases2),
                                   {Host, Aliases3};
                              (Host, Aliases) ->
                                   Aliases2 = orddict:store(Host, Host, Aliases),
                                   {Host, Aliases2}
                           end, orddict:new(), Terms);
        _ ->
            erlang:error({"Missing or invalid rtssh hosts file", file:get_cwd()})
    end.

get_host(Node) ->
    orddict:fetch(Node, rt:config(rt_hosts)).

get_ip(Host) ->
    {ok, IP} = inet:getaddr(Host, inet),
    string:join([integer_to_list(X) || X <- tuple_to_list(IP)], ".").

%%%===================================================================
%%% Remote file operations
%%%===================================================================

wildcard(Node, Path) ->
    Cmd = "find " ++ Path ++ " -maxdepth 0 -print",
    case ssh_cmd(Node, Cmd) of
        {0, Result} ->
            string:tokens(Result, "\n");
        _ ->
            error
    end.

spawn_ssh_cmd(Node, Cmd) ->
    spawn_ssh_cmd(Node, Cmd, []).
spawn_ssh_cmd(Node, Cmd, Opts) when is_atom(Node) ->
    Host = get_host(Node),
    spawn_ssh_cmd(Host, Cmd, Opts);
spawn_ssh_cmd(Host, Cmd, Opts) ->
    SSHCmd = format("ssh -o 'StrictHostKeyChecking no' ~s '~s'", [Host, Cmd]),
    spawn_cmd(SSHCmd, Opts).

ssh_cmd(Node, Cmd) ->
    wait_for_cmd(spawn_ssh_cmd(Node, Cmd)).

remote_read_file(Node, File) ->
    case ssh_cmd(Node, "cat " ++ File) of
        {0, Text} ->
            %% io:format("~p/~p: read: ~p~n", [Node, File, Text]),

            %% Note: remote_read_file sometimes returns "" for some
            %% reason, however printing out to debug things (as in the
            %% above io:format) makes error go away. Going to assume
            %% race condition and throw in timer:sleep here.
            %% TODO: debug for real.
            timer:sleep(500),
            list_to_binary(Text);
        Error ->
            erlang:error("Failed to read remote file", [Node, File, Error])
    end.

remote_write_file(NodeOrHost, File, Data) ->
    Port = spawn_ssh_cmd(NodeOrHost, "cat > " ++ File, [out]),
    true = port_command(Port, Data),
    true = port_close(Port),
    ok.

format(Msg, Args) ->
    lists:flatten(io_lib:format(Msg, Args)).

update_vm_args(_Node, []) ->
    ok;
update_vm_args(Node, Props) ->
    %% TODO: Make non-matched options be appended to file
    VMArgs = node_path(Node) ++ "/etc/vm.args",
    Bin = remote_read_file(Node, VMArgs),
    Output =
        lists:foldl(fun({Config, Value}, Acc) ->
                            CBin = to_binary(Config),
                            VBin = to_binary(Value),
                            re:replace(Acc,
                                       <<"((^|\\n)", CBin/binary, ").+\\n">>,
                                       <<"\\1 ", VBin/binary, $\n>>)
                    end, Bin, Props),
    %% io:format("~p~n", [iolist_to_binary(Output)]),
    remote_write_file(Node, VMArgs, Output),
    ok.

update_app_config(Node, Config) ->
    ConfigFile = node_path(Node) ++ "/etc/app.config",
    update_app_config_file(Node, ConfigFile, Config).

update_app_config_file(Node, ConfigFile, Config) ->
    lager:info("rtssh:update_app_config_file(~p, ~s, ~p)",
               [Node, ConfigFile, Config]),
    Bin = remote_read_file(Node, ConfigFile),
    BaseConfig =
        try
            {ok, BC} = consult_string(Bin),
            BC
        catch
            _:_ ->
                erlang:error({"Failed to parse app.config for", Node, Bin})
        end,
    %% io:format("BaseConfig: ~p~n", [BaseConfig]),
    MergeA = orddict:from_list(Config),
    MergeB = orddict:from_list(BaseConfig),
    NewConfig =
        orddict:merge(fun(_, VarsA, VarsB) ->
                              MergeC = orddict:from_list(VarsA),
                              MergeD = orddict:from_list(VarsB),
                              Props =
                                  orddict:merge(fun(_, Fun, ValB) when is_function(Fun) ->
                                                        Fun(ValB);
                                                   (_, ValA, _ValB) ->
                                                        ValA
                                                end, MergeC, MergeD),
                              [{K,V} || {K,V} <- Props,
                                        not is_function(V)]
                      end, MergeA, MergeB),
    NewConfigOut = io_lib:format("~p.", [NewConfig]),
    ?assertEqual(ok, remote_write_file(Node, ConfigFile, NewConfigOut)),
    ok.

consult_string(Bin) when is_binary(Bin) ->
    consult_string(binary_to_list(Bin));
consult_string(Str) ->
    {ok, Tokens, _} = erl_scan:string(Str),
    erl_parse:parse_term(Tokens).

%%%===================================================================
%%% Riak devrel path utilities
%%%===================================================================

-define(PATH, (rt:config(rtdev_path))).

dev_path(Path, N) ->
    format("~s/dev/dev~b", [Path, N]).

dev_bin_path(Path, N) ->
    dev_path(Path, N) ++ "/bin".

dev_etc_path(Path, N) ->
    dev_path(Path, N) ++ "/etc".

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

node_path(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(Node)),
    lists:flatten(io_lib:format("~s/dev/dev~b", [Path, N])).

node_id(_Node) ->
    %% NodeMap = rt:config(rt_nodes),
    %% orddict:fetch(Node, NodeMap).
    1.

node_version(Node) ->
    orddict:fetch(Node, rt:config(rt_versions)).

%%%===================================================================
%%% Local command spawning
%%%===================================================================

spawn_cmd(Cmd) ->
    spawn_cmd(Cmd, []).
spawn_cmd(Cmd, Opts) ->
    Port = open_port({spawn, Cmd}, [stream, in, exit_status] ++ Opts),
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

%%%===================================================================
%%% rtdev stuff
%%%===================================================================

devpaths() ->
    Paths = proplists:delete(root, rt:config(rtdev_path)),
    lists:usort([DevPath || {_Name, DevPath} <- Paths]).

stop_all(Hosts) ->
    %% [stop_all(Host, DevPath ++ "/dev") || Host <- Hosts,
    %%                                       DevPath <- devpaths()].
    All = [{Host, DevPath} || Host <- Hosts,
                              DevPath <- devpaths()],
    rt:pmap(fun({Host, DevPath}) ->
                    stop_all(Host, DevPath ++ "/dev")
            end, All).

stop_all(Host, DevPath) ->
    case wildcard(Host, DevPath ++ "/dev*") of
        error ->
            lager:info("~s is not a directory.", [DevPath]);
        Devs ->
            [begin
                 Cmd = D ++ "/bin/riak stop",
                 {_, Result} = ssh_cmd(Host, Cmd),
                 [Output | _Tail] = string:tokens(Result, "\n"),
                 Status = case Output of
                              "ok" -> "ok";
                              _ -> "wasn't running"
                          end,
                 lager:info("Stopping Node... ~s :: ~s ~~ ~s.",
                            [Host, Cmd, Status])
             end || D <- Devs]
    end,
    ok.

sync_bench(Hosts) ->
    case rt:config(rtssh_bench) of
        undefined ->
            ok;
        Path ->
            Paths = filename:split(Path),
            Root = filename:join(lists:sublist(Paths, length(Paths)-1)),
            rt:pmap(fun(Host) ->
                            Cmd = "rsync -tr " ++ Path ++ " " ++ Host ++ ":" ++ Root,
                            Result = cmd(Cmd),
                            lager:info("Syncing bench :: ~p :: ~p :: ~p~n", [Host, Cmd, Result])
                    end, Hosts)
    end.

sync_proxy(Hosts) ->
    case rt:config(rtssh_proxy) of
        undefined ->
            ok;
        Path ->
            Paths = filename:split(Path),
            Root = filename:join(lists:sublist(Paths, length(Paths)-1)),
            rt:pmap(fun(Host) ->
                            Cmd = "rsync -tr " ++ Path ++ " " ++ Host ++ ":" ++ Root,
                            Result = cmd(Cmd),
                            lager:info("Syncing proxy :: ~p :: ~p :: ~p~n", [Host, Cmd, Result])
                    end, Hosts)
    end.

stop_all_bench(Hosts) ->
    case rt:config(rtssh_bench) of
        undefined ->
            ok;
        Path ->
            rt:pmap(fun(Host) ->
                            Cmd = "cd " ++ Path ++ " && bash ./bb.sh stop",
                            %% Result = ssh_cmd(Host, Cmd),
                            %% lager:info("Stopping basho_bench... ~s :: ~s ~~ ~p.",
                            %%            [Host, Cmd, Result])
                            {_, Result} = ssh_cmd(Host, Cmd),
                            [Output | _Tail] = string:tokens(Result, "\n"),
                            Status = case Output of
                                         "ok" -> "ok";
                                         _ -> "wasn't running"
                                     end,
                            lager:info("Stopping basho_bench... ~s :: ~s ~~ ~s.",
                                       [Host, Cmd, Status])
                    end, Hosts)
    end.

deploy_bench() ->
    deploy_bench(rt:config(rtssh_bench_hosts)).

deploy_bench(Hosts) ->
    case rt:config(rtssh_bench) of
        undefined ->
            ok;
        Path ->
            rt:pmap(fun(Host) ->
                            Cookie = "riak",
                            This = lists:flatten(io_lib:format("~s", [node()])),
                            Cmd =
                                "cd " ++ Path ++ " && bash ./bb.sh"
                                " -N bench@" ++ Host ++
                                " -C " ++ Cookie ++
                                " -J " ++ This ++
                                " -D",
                            spawn_ssh_cmd(Host, Cmd),
                            lager:info("Starting basho_bench... ~s :: ~s",
                                       [Host, Cmd])
                    end, Hosts),
            [rt:wait_until_pingable(list_to_atom("bench@" ++ Host)) || Host <- Hosts],
            timer:sleep(1000),
            ok
    end.

deploy_proxy(Seed) ->
    deploy_proxy(Seed, rt:config(rtssh_bench_hosts)).

deploy_proxy(Seed, Hosts) ->
    SeedStr = atom_to_list(Seed),
    case rt:config(rtssh_proxy) of
        undefined ->
            ok;
        Path ->
            rt:pmap(fun(Host) ->
                            Cmd = "cd " ++ Path ++ " && bash go.sh \"" ++ SeedStr ++ "\"",
                            spawn_ssh_cmd(Host, Cmd),
                            lager:info("Starting riak_proxycfg... ~s :: ~s",
                                       [Host, Cmd])
                    end, Hosts),
            timer:sleep(2000),
            ok
    end.

teardown() ->
    stop_all(rt:config(rt_hostnames)).

%%%===================================================================
%%% Collector stuff
%%%===================================================================

collector_group_start(Name) ->
    collector_call({group_start, timestamp(), Name}).

collector_group_end() ->
    collector_call({group_end, timestamp()}).

collector_bench_start(Name, Config, Desc) ->
    collector_call({bench_start, timestamp(), Name, Config, Desc}).

collector_bench_end() ->
    collector_call({bench_end, timestamp()}).

collector_call(Msg) ->
    {Node, _, _} = rt:config(rtssh_collector),
    gen_server:call({collector, Node}, Msg, 30000).

timestamp() ->
    timestamp(os:timestamp()).

timestamp({Mega, Secs, Micro}) ->
    Mega*1000*1000*1000 + Secs * 1000 + (Micro div 1000).

%%%===================================================================
%%% Utilities
%%%===================================================================

to_list(X) when is_integer(X) -> integer_to_list(X);
to_list(X) when is_float(X)   -> float_to_list(X);
to_list(X) when is_atom(X)    -> atom_to_list(X);
to_list(X) when is_list(X)    -> X.	%Assumed to be a string

to_binary(X) when is_binary(X) ->
    X;
to_binary(X) ->
    list_to_binary(to_list(X)).

