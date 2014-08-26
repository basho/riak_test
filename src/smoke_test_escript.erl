-module(smoke_test_escript).
-include_lib("kernel/include/file.hrl").

-export([main/1, get_version/0, worker/4]).

get_version() ->
    list_to_binary(string:strip(os:cmd("git describe"), right, $\n)).

cli_options() ->
%% Option Name, Short Code, Long Code, Argument Spec, Help Message
[
 {project,   $p, "project",   string,    "specifies which project"},
 {debug,     $v, "debug",     undefined, "debug?"},
 {directory, $d, "directory", string,    "source tree directory"},
 {jobs,      $j, "jobs",      integer,   "jobs?"},
 {tasks,     $T, "tasks",     string,     "What task(s) to run (eunit|dialyzer|xref)"}
].


main(Args) ->
    Parsed = case getopt:parse(cli_options(), Args) of
                 {ok, {Parsed0, _Other}} -> Parsed0;
                 _ ->
                     getopt:usage(cli_options(), escript:script_name()),
                     halt(1),
                     []
             end,
    application:start(ibrowse),
    lager:start(),
    rt_config:load("default", filename:join([os:getenv("HOME"), ".riak_test.config"])),
    case lists:keyfind(project, 1, Parsed) of
        false ->
            getopt:usage(cli_options(), escript:script_name()),
            lager:error("Must specify project!"),
            application:stop(lager),
            halt(1);
        {project, Project} ->
            rt_config:set(rt_project, Project)
    end,
    case lists:keyfind(directory, 1, Parsed) of
        false ->
            %% run in current working directory
            ok;
        {directory, Dir} ->
            lager:info("Changing working dir to ~s", [Dir]),
            ok = file:set_cwd(filename:absname(Dir))
    end,
    Tasks = case lists:keyfind(tasks, 1, Parsed) of
        false ->
            ["xref", "dialyzer", "eunit"];
        {tasks, List} ->
            string:tokens(List, ",")
    end,

    case lists:member(debug, Parsed) of
        true ->
            lager:set_loglevel(lager_console_backend, debug);
        _ ->
            ok
    end,
    rt_config:set(rt_harness, ?MODULE),
    lager:debug("ParsedArgs ~p", [Parsed]),
    Suites = giddyup:get_suite(rt_config:get(platform)),
    Jobs = case lists:keyfind(jobs, 1, Parsed) of
        false ->
            1;
        {jobs, J} ->
            J
    end,

    {ok, PWD} = file:get_cwd(),
    Rebar = filename:join(PWD, "rebar"),


    setup_deps(Rebar, PWD, [filename:join([PWD, "deps", F])
                            || F <- element(2, file:list_dir(filename:join(PWD, "deps"))),
                               filelib:is_dir(filename:join([PWD, "deps", F]))]),

    case Jobs > 1 of
        true ->
            %% partiton the suite list by the number of jobs
            SplitSuites = dict:to_list(element(2, lists:foldl(fun(S, {Counter, Dict}) ->
                                    {Counter + 1, dict:append(Counter rem Jobs, S, Dict)}
                            end, {0, dict:new()}, Suites))),
            lager:debug("Split into ~p lists", [length(SplitSuites)]),
            Workers = [spawn_monitor(?MODULE, worker, [Rebar, PWD, SS, Tasks]) || {_, SS} <- SplitSuites],
            wait_for_workers([P || {P, _} <- Workers]);
        _ ->
            worker(Rebar, PWD, Suites, Tasks)
    end.

worker(Rebar, PWD, Suites, Tasks) ->
    lists:foreach(fun({Suite, Config}) ->
                lager:info("Suite ~p config ~p", [Suite, Config]),
                [Dep, Task] = string:tokens(atom_to_list(Suite), ":"),
                FDep = filename:join([PWD, deps, Dep]),
                case filelib:is_dir(FDep) of
                    true ->
                        case {Task, lists:member(Task, Tasks)} of
                            {"eunit", true} ->
                                %% make rebar spit out the coverdata
                                file:write_file(filename:join(FDep, "rebar.config"),
                                                "\n{cover_export_enabled, true}.", [append]),
                                %% set up a symlink so that each dep has deps
                                P = erlang:open_port({spawn_executable, Rebar},
                                                     [{args, ["eunit", "skip_deps=true"]},
                                                      {cd, FDep}, exit_status,
                                                      {line, 1024}, stderr_to_stdout, binary]),
                                {Res, Log} = accumulate(P, []),
                                {ok, Base} = giddyup:post_result([{test, Suite}, {status, get_status(Res)} | Config]),
                                giddyup:post_artifact(Base, {"eunit.log", Log}),
                                CoverFile = filename:join(FDep, ".eunit/eunit.coverdata"),
                                case filelib:is_regular(CoverFile) of
                                    true ->
                                        giddyup:post_artifact(Base, {"eunit.coverdata.gz", zlib:gzip(element(2, file:read_file(CoverFile)))});
                                    _ -> ok
                                end,
                                Res;
                            {"dialyzer", true} ->
                                P = erlang:open_port({spawn_executable, "/usr/bin/make"},
                                                     [{args, ["dialyzer"]},
                                                      {cd, FDep}, exit_status,
                                                      {line, 1024}, stderr_to_stdout, binary]),
                                {Res, Log} = accumulate(P, []),
                                %% TODO split the logs so that the PLT stuff is elided
                                {ok, Base} = giddyup:post_result([{test, Suite}, {status, get_status(Res)}| Config]),
                                giddyup:post_artifact(Base, {"dialyzer.log", Log}),
                                Res;
                            {"xref", true} ->
                                P = erlang:open_port({spawn_executable, Rebar},
                                                     [{args, ["xref", "skip_deps=true"]},
                                                      {cd, FDep}, exit_status,
                                                      {line, 1024}, stderr_to_stdout, binary]),
                                {Res, Log} = accumulate(P, []),
                                {ok, Base} = giddyup:post_result([{test, Suite}, {status, get_status(Res)}| Config]),
                                giddyup:post_artifact(Base, {"xref.log", Log}),
                                Res;
                            _ ->
                                lager:info("Skipping suite ~p", [Suite]),
                                ok

                        end;
                    false ->
                        lager:debug("Not a dep: ~p", [FDep])
                end
        end, Suites).

setup_deps(_, _, []) -> ok;
setup_deps(Rebar, PWD, [Dep|Deps]) ->
    %% clean up an old deps dir, if present
    remove_deps_dir(Dep),
    %% symlink ALL the deps in
    file:make_symlink(filename:join(PWD, "deps"), filename:join(Dep, "deps")),
    lager:debug("ln -sf ~s ~s", [filename:join(PWD, "deps"),
                                 filename:join(Dep, "deps")]),
    %% run rebar list deps, to find out which ones to keep
    P = erlang:open_port({spawn_executable, Rebar},
                         [{args, ["list-deps"]},
                          {cd, Dep}, exit_status,
                          {line, 1024}, stderr_to_stdout, binary]),
    {0, Log} = accumulate(P, []),
    %% find all the deps, amongst the noise
    case re:run(Log, "([a-zA-Z0-9_]+) (?:BRANCH|TAG|REV)",
                [global, {capture, all_but_first, list}]) of
        {match, Matches} ->
            lager:info("Deps for ~p are ~p", [Dep, Matches]),
            ok = file:delete(filename:join(Dep, "deps")),
            ok = filelib:ensure_dir(filename:join(Dep, "deps")++"/"),
            [file:make_symlink(filename:join([PWD, "deps", M]),
                               filename:join([Dep, "deps", M]))
             || M <- Matches];
        nomatch ->
            %% remove the symlink
            file:delete(filename:join(Dep, "deps")),
            lager:info("~p has no deps", [Dep])
    end,
    setup_deps(Rebar, PWD, Deps).

remove_deps_dir(Dep) ->
    DepDir = filename:join(Dep, "deps"),
    case filelib:is_dir(DepDir) of
        true ->
            {ok, DI} = file:read_link_info(DepDir),
            case DI#file_info.type of
                symlink ->
                    %% leftover symlink, probably from an aborted run
                    ok = file:delete(DepDir);
                _ ->
                    %% there should ONLY be a deps dir leftover from a previous run,
                    %% so it should be a directory filled with symlinks
                    {ok, Files} = file:list_dir(DepDir),
                    lists:foreach(fun(F) ->
                                File = filename:join(DepDir, F),
                                {ok, FI} = file:read_link_info(File),
                                case FI#file_info.type of
                                    symlink ->
                                        ok = file:delete(File);
                                    _ ->
                                        ok
                                end
                        end, Files),
                    %% this will fail if the directory is not now empty
                    ok = file:del_dir(DepDir),
                    ok
            end;
        false ->
            ok
    end.

wait_for_workers([]) ->
    ok;
wait_for_workers(Workers) ->
    receive
        {'DOWN', _, _, Pid, normal} ->
            lager:info("Worker ~p exited normally, ~p left", [Pid, length(Workers)-1]),
            wait_for_workers(Workers -- [Pid]);
        {'DOWN', _, _, Pid, Reason} ->
            lager:info("Worker ~p exited abnormally: ~p, ~p left", [Pid, Reason,
                                                                 length(Workers)-1]),
            wait_for_workers(Workers -- [Pid])
    end.

maybe_eol(eol) ->
    "\n";
maybe_eol(noeol) ->
    "".

get_status(0) ->
    pass;
get_status(_) ->
    fail.

accumulate(P, Acc) ->
    receive
        {P, {data, {EOL, Data}}} ->
            accumulate(P, [[Data,maybe_eol(EOL)]|Acc]);
        {P, {exit_status, Status}} ->
            lager:debug("Exited with status ~b", [Status]),
            {Status, list_to_binary(lists:reverse(Acc))};
        {P, Other} ->
            lager:warning("Unexpected return from port: ~p", [Other]),
            accumulate(P, Acc)
    end.
