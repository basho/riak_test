%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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

%% @private
-module(riak_test_escript).
-include("rt.hrl").
-export([main/1]).
-export([add_deps/1]).

main(Args) ->
    {ParsedArgs, HarnessArgs, Tests} = prepare(Args),
    OutDir = proplists:get_value(outdir, ParsedArgs),
    Results = execute(Tests, OutDir, report(ParsedArgs), HarnessArgs),
    finalize(Results, ParsedArgs).

prepare(Args) ->
    {ParsedArgs, _, Tests} = ParseResults = parse_args(Args),
    io:format("Tests to run: ~p~n", [Tests]),
    ok = erlang_setup(ParsedArgs),
    ok = test_setup(ParsedArgs),
    ParseResults.

execute(Tests, Outdir, Report, HarnessArgs) ->
    TestCount = length(Tests),
    TestResults = [run_test(Test,
                            Outdir,
                            TestMetaData,
                            Report,
                            HarnessArgs,
                            TestCount) ||
                      {Test, TestMetaData} <- Tests],
    lists:filter(fun results_filter/1, TestResults).

finalize(TestResults, Args) ->
    [rt_cover:maybe_import_coverage(proplists:get_value(coverdata, R)) ||
        R <- TestResults],
    CoverDir = rt_config:get(cover_output, "coverage"),
    Coverage = rt_cover:maybe_write_coverage(all, CoverDir),

    Verbose = proplists:is_defined(verbose, Args),
    Teardown = not proplists:get_value(keep, Args, false),
    maybe_teardown(Teardown, TestResults, Coverage, Verbose),
    ok.

%% Option Name, Short Code, Long Code, Argument Spec, Help Message
cli_options() ->
[
 {help,               $h, "help",     undefined,  "Print this usage page"},
 {config,             $c, "conf",     string,     "specifies the project configuration"},
 {tests,              $t, "tests",    string,     "specifies which tests to run"},
 {suites,             $s, "suites",   string,     "which suites to run"},
 {dir,                $d, "dir",      string,     "run all tests in the specified directory"},
 {skip,               $x, "skip",     string,     "list of tests to skip in a directory"},
 {verbose,            $v, "verbose",  undefined,  "verbose output"},
 {outdir,             $o, "outdir",   string,     "output directory"},
 {backend,            $b, "backend",  atom,       "backend to test [memory | bitcask | eleveldb]"},
 {upgrade_version,    $u, "upgrade",  atom,       "which version to upgrade from [ previous | legacy ]"},
 {keep,        undefined, "keep",     boolean,    "do not teardown cluster"},
 {report,             $r, "report",   string,     "you're reporting an official test run, provide platform info (e.g. ubuntu-1204-64)\nUse 'config' if you want to pull from ~/.riak_test.config"},
 {file,               $F, "file",     string,     "use the specified file instead of ~/.riak_test.config"}
].

print_help() ->
    getopt:usage(cli_options(), escript:script_name()),
    halt(0).

add_deps(Path) ->
    {ok, Deps} = file:list_dir(Path),
    [code:add_path(lists:append([Path, "/", Dep, "/ebin"])) || Dep <- Deps],
    ok.

test_setup(ParsedArgs) ->
    %% File output
    OutDir = proplists:get_value(outdir, ParsedArgs),
    ensure_dir(OutDir),

    lager_setup(OutDir),

    %% Ensure existence of scratch_dir
    case ensure_dir(rt_config:get(rt_scratch_dir) ++ "/test.file") of
        ok ->
            great;
        {error, ErrorReason} ->
            lager:error("Could not create scratch dir, ~p",
                        [ErrorReason])
    end,
    ok.

report(ParsedArgs) ->
    case proplists:get_value(report, ParsedArgs, undefined) of
        undefined ->
            undefined;
        "config" ->
            rt_config:get(platform, undefined);
        R ->
            R
    end.

parse_args(Args) ->
    help_or_parse_args(getopt:parse(cli_options(), Args)).

help_or_parse_args({ok, {[], _}}) ->
    print_help();
help_or_parse_args({ok, {ParsedArgs, HarnessArgs}}) ->
    help_or_parse_tests(ParsedArgs, HarnessArgs, lists:member(help, ParsedArgs));
help_or_parse_args(_) ->
    print_help().

help_or_parse_tests(_, _, true) ->
    print_help();
help_or_parse_tests(ParsedArgs, HarnessArgs, false) ->
    %% Have to load the `riak_test' config prior to assembling the
    %% test metadata
    load_initial_config(ParsedArgs),

    TestData = compose_test_data(ParsedArgs),
    Tests = which_tests_to_run(report(ParsedArgs), TestData),
    Offset = rt_config:get(offset, undefined),
    Workers = rt_config:get(workers, undefined),
    shuffle_tests(ParsedArgs, HarnessArgs, Tests, Offset, Workers).

load_initial_config(ParsedArgs) ->
    %% Loads application defaults
    application:load(riak_test),

    %% Loads from ~/.riak_test.config
    rt_config:load(proplists:get_value(config, ParsedArgs),
                   proplists:get_value(file, ParsedArgs)).

shuffle_tests(_, _, [], _, _) ->
    lager:warning("No tests are scheduled to run"),
    init:stop(1);
shuffle_tests(ParsedArgs, HarnessArgs, Tests, undefined, _) ->
    {ParsedArgs, HarnessArgs, Tests};
shuffle_tests(ParsedArgs, HarnessArgs, Tests, _, undefined) ->
    {ParsedArgs, HarnessArgs, Tests};
shuffle_tests(ParsedArgs, HarnessArgs, Tests, Offset, Workers) ->
    TestCount = length(Tests),
    %% Avoid dividing by zero, computers hate that
    Denominator = case Workers rem (TestCount+1) of
                      0 -> 1;
                      D -> D
                  end,
    ActualOffset = ((TestCount div Denominator) * Offset) rem (TestCount+1),
    {TestA, TestB} = lists:split(ActualOffset, Tests),
    lager:info("Offsetting ~b tests by ~b (~b workers, ~b offset)",
               [TestCount, ActualOffset, Workers, Offset]),
    {ParsedArgs, HarnessArgs, TestB ++ TestA}.

erlang_setup(_ParsedArgs) ->
    register(riak_test, self()),
    maybe_add_code_path("./ebin"),

    %% ibrowse
    load_and_start(ibrowse),

    %% Sets up extra paths earlier so that tests can be loadable
    %% without needing the -d flag.
    code:add_paths(rt_config:get(test_paths, [])),

    %% Two hard-coded deps...
    add_deps(rt:get_deps()),
    add_deps("deps"),

    [add_deps(Dep) || Dep <- rt_config:get(rt_deps, [])],
    [] = os:cmd("epmd -daemon"),
    net_kernel:start([rt_config:get(rt_nodename, 'riak_test@127.0.0.1')]),
    erlang:set_cookie(node(), rt_config:get(rt_cookie, riak)),
    ok.

maybe_add_code_path(Path) ->
    maybe_add_code_path(Path, filelib:is_dir(Path)).

maybe_add_code_path(Path, true) ->
    code:add_patha(Path);
maybe_add_code_path(_, false) ->
    meh.

load_and_start(Application) ->
    application:load(Application),
    application:start(Application).

ensure_dir(undefined) ->
    ok;
ensure_dir(Dir) ->
    filelib:ensure_dir(Dir).

lager_setup(undefined) ->
    set_lager_env(rt_config:get(lager_level, info)),
    lager:start();
lager_setup(_) ->
    set_lager_env(notice),
    lager:start().

set_lager_env(LagerLevel) ->
    application:load(lager),
    HandlerConfig = [{lager_console_backend, LagerLevel},
                     {lager_file_backend, [{file, "log/test.log"},
                                           {level, LagerLevel}]}],
    application:set_env(lager, handlers, HandlerConfig).

maybe_teardown(false, TestResults, Coverage, Verbose) ->
    print_summary(TestResults, Coverage, Verbose),
    lager:info("Keeping cluster running as requested");
maybe_teardown(true, TestResults, Coverage, Verbose) ->
    case {length(TestResults), proplists:get_value(status, hd(TestResults))} of
        {1, fail} ->
            print_summary(TestResults, Coverage, Verbose),
            so_kill_riak_maybe();
        _ ->
            lager:info("Multiple tests run or no failure"),
            rt_cluster:teardown(),
            print_summary(TestResults, Coverage, Verbose)
    end,
    ok.

compose_test_data(ParsedArgs) ->
    RawTestList = proplists:get_all_values(tests, ParsedArgs),
    TestList = lists:foldl(fun(X, Acc) -> string:tokens(X, ", ") ++ Acc end, [], RawTestList),
    %% Parse Command Line Tests
    {CodePaths, SpecificTests} =
        lists:foldl(fun extract_test_names/2,
                    {[], []},
                    TestList),

    [code:add_patha(CodePath) || CodePath <- CodePaths,
                                 CodePath /= "."],

    Dirs = proplists:get_all_values(dir, ParsedArgs),
    SkipTests = string:tokens(proplists:get_value(skip, ParsedArgs, []), [$,]),
    DirTests = lists:append([load_tests_in_dir(Dir, SkipTests) || Dir <- Dirs]),
    Project = list_to_binary(rt_config:get(rt_project, "undefined")),

    Backends = case proplists:get_all_values(backend, ParsedArgs) of
        [] -> [undefined];
        Other -> Other
    end,
    Upgrades = case proplists:get_all_values(upgrade_version, ParsedArgs) of
                   [] -> [undefined];
                   UpgradeList -> UpgradeList
               end,
    TestFoldFun = test_data_fun(rt:get_version(), Project, Backends, Upgrades),
    lists:foldl(TestFoldFun, [], lists:usort(DirTests ++ SpecificTests)).

test_data_fun(Version, Project, Backends, Upgrades) ->
    fun(Test, Tests) ->
            [{list_to_atom(Test),
              compose_test_datum(Version, Project, Backend, Upgrade)}
             || Backend <- Backends, Upgrade <- Upgrades ] ++ Tests
    end.

compose_test_datum(Version, Project, undefined, undefined) ->
    [{id, -1},
     {platform, <<"local">>},
     {version, Version},
     {project, Project}];
compose_test_datum(Version, Project, undefined, Upgrade) ->
    compose_test_datum(Version, Project, undefined, undefined) ++
        [{upgrade_version, Upgrade}];
compose_test_datum(Version, Project, Backend, undefined) ->
    compose_test_datum(Version, Project, undefined, undefined) ++
        [{backend, Backend}];
compose_test_datum(Version, Project, Backend, Upgrade) ->
    compose_test_datum(Version, Project, undefined, undefined) ++
        [{backend, Backend}, {upgrade_version, Upgrade}].

extract_test_names(Test, {CodePaths, TestNames}) ->
    {[filename:dirname(Test) | CodePaths],
     [filename:rootname(filename:basename(Test)) | TestNames]}.

which_tests_to_run(undefined, CommandLineTests) ->
    {Tests, NonTests} =
        lists:partition(fun is_runnable_test/1, CommandLineTests),
    lager:info("These modules are not runnable tests: ~p",
               [[NTMod || {NTMod, _} <- NonTests]]),
    Tests;
which_tests_to_run(Platform, []) ->
    giddyup:get_suite(Platform);
which_tests_to_run(Platform, CommandLineTests) ->
    Suite = filter_zip_suite(Platform, CommandLineTests),
    {Tests, NonTests} =
        lists:partition(fun is_runnable_test/1,
                        lists:foldr(fun filter_merge_tests/2, [], Suite)),

    lager:info("These modules are not runnable tests: ~p",
               [[NTMod || {NTMod, _} <- NonTests]]),
    Tests.

filter_zip_suite(Platform, CommandLineTests) ->
    [ {SModule, SMeta, CMeta} || {SModule, SMeta} <- giddyup:get_suite(Platform),
                                 {CModule, CMeta} <- CommandLineTests,
                                 SModule =:= CModule].

filter_merge_tests({Module, SMeta, CMeta}, Tests) ->
    case filter_merge_meta(SMeta, CMeta, [backend, upgrade_version]) of
        false ->
            Tests;
        Meta ->
            [{Module, Meta}|Tests]
    end.

filter_merge_meta(SMeta, _CMeta, []) ->
    SMeta;
filter_merge_meta(SMeta, CMeta, [Field|Rest]) ->
    case {kvc:value(Field, SMeta, undefined), kvc:value(Field, CMeta, undefined)} of
        {X, X} ->
            filter_merge_meta(SMeta, CMeta, Rest);
        {_, undefined} ->
            filter_merge_meta(SMeta, CMeta, Rest);
        {undefined, X} ->
            filter_merge_meta(lists:keystore(Field, 1, SMeta, {Field, X}), CMeta, Rest);
        _ ->
            false
    end.

%% Check for api compatibility
is_runnable_test({TestModule, _}) ->
    {Mod, Fun} = riak_test_runner:function_name(confirm, TestModule),
    code:ensure_loaded(Mod),
    erlang:function_exported(Mod, Fun, 0) orelse
        erlang:function_exported(Mod, Fun, 2).

run_test(Test, Outdir, TestMetaData, Report, HarnessArgs, NumTests) ->
    rt_cover:maybe_start(Test),
    SingleTestResult = riak_test_runner:run(Test, Outdir, TestMetaData, HarnessArgs),
    CoverDir = rt_config:get(cover_output, "coverage"),
    case NumTests of
        1 -> keep_them_up;
        _ -> rt_cluster:teardown()
    end,
    CoverFile = rt_cover:maybe_export_coverage(Test, CoverDir, erlang:phash2(TestMetaData)),
    publish_report(SingleTestResult, CoverFile, Report),
    rt_cover:stop(),
    [{coverdata, CoverFile} | SingleTestResult].

publish_report(_SingleTestResult, _CoverFile, undefined) ->
    ok;
publish_report(SingleTestResult, CoverFile, _Report) ->
    {value, {log, Log}, TestResult} = lists:keytake(log, 1, SingleTestResult),
    publish_artifacts(TestResult,
                      Log,
                      CoverFile,
                      giddyup:post_result(TestResult)).

publish_artifacts(_TestResult, _Log, _CoverFile, error) ->
    whoomp; %% there it is
publish_artifacts(TestResult, Log, CoverFile, {ok, Base}) ->
            %% Now push up the artifacts, starting with the test log
            giddyup:post_artifact(Base, {"riak_test.log", Log}),
            [giddyup:post_artifact(Base, File) || File <- rt:get_node_logs()],
            post_cover_artifact(Base, CoverFile),
            ResultPlusGiddyUp = TestResult ++ [{giddyup_url, list_to_binary(Base)}],
            [rt:post_result(ResultPlusGiddyUp, WebHook) || WebHook <- get_webhooks()].

post_cover_artifact(_Base, cover_disabled) ->
    ok;
post_cover_artifact(Base, CoverFile) ->
    CoverArchiveName = filename:basename(CoverFile) ++ ".gz",
    CoverArchive = zlib:gzip(element(2, file:read_file(CoverFile))),
    giddyup:post_artifact(Base, {CoverArchiveName, CoverArchive}).

get_webhooks() ->
    Hooks = lists:foldl(fun(E, Acc) -> [parse_webhook(E) | Acc] end,
                        [],
                        rt_config:get(webhooks, [])),
    lists:filter(fun(E) -> E =/= undefined end, Hooks).

parse_webhook(Props) ->
    Url = proplists:get_value(url, Props),
    case is_list(Url) of
        true ->
            #rt_webhook{url= Url,
                        name=proplists:get_value(name, Props, "Webhook"),
                        headers=proplists:get_value(headers, Props, [])};
        false ->
            lager:error("Invalid configuration for webhook : ~p", Props),
            undefined
    end.

print_summary(TestResults, CoverResult, Verbose) ->
    io:format("~nTest Results:~n"),

    Results = [
                [ atom_to_list(proplists:get_value(test, SingleTestResult)) ++ "-" ++
                      backend_list(proplists:get_value(backend, SingleTestResult)),
                  proplists:get_value(status, SingleTestResult),
                  proplists:get_value(reason, SingleTestResult)]
                || SingleTestResult <- TestResults],
    Width = test_name_width(Results),

    Print = fun(Test, Status, Reason) ->
        case {Status, Verbose} of
            {fail, true} -> io:format("~s: ~s ~p~n", [string:left(Test, Width), Status, Reason]);
            _ -> io:format("~s: ~s~n", [string:left(Test, Width), Status])
        end
    end,
    [ Print(Test, Status, Reason) || [Test, Status, Reason] <- Results],

    PassCount = length(lists:filter(fun(X) -> proplists:get_value(status, X) =:= pass end, TestResults)),
    FailCount = length(lists:filter(fun(X) -> proplists:get_value(status, X) =:= fail end, TestResults)),
    io:format("---------------------------------------------~n"),
    io:format("~w Tests Failed~n", [FailCount]),
    io:format("~w Tests Passed~n", [PassCount]),
    Percentage = case PassCount == 0 andalso FailCount == 0 of
        true -> 0;
        false -> (PassCount / (PassCount + FailCount)) * 100
    end,
    io:format("That's ~w% for those keeping score~n", [Percentage]),

    case CoverResult of
        cover_disabled ->
            ok;
        {Coverage, AppCov} ->
            io:format("Coverage : ~.1f%~n", [Coverage]),
            [io:format("    ~s : ~.1f%~n", [App, Cov])
             || {App, Cov, _} <- AppCov]
    end,
    ok.

test_name_width(Results) ->
    lists:max([ length(X) || [X | _T] <- Results ]).

backend_list(Backend) when is_atom(Backend) ->
    atom_to_list(Backend);
backend_list(Backends) when is_list(Backends) ->
    FoldFun = fun(X, []) ->
                      atom_to_list(X);
                 (X, Acc) ->
                      Acc ++ "," ++ atom_to_list(X)
              end,
    lists:foldl(FoldFun, [], Backends).

results_filter(Result) ->
    case proplists:get_value(status, Result) of
        not_a_runnable_test ->
            false;
        _ ->
            true
    end.

load_tests_in_dir(Dir, SkipTests) ->
    case filelib:is_dir(Dir) of
        true ->
            code:add_path(Dir),
            lists:sort(
              lists:foldl(load_tests_folder(SkipTests),
                          [],
                          filelib:wildcard("*.beam", Dir)));
        _ ->
            io:format("~s is not a dir!~n", [Dir])
    end.

load_tests_folder(SkipTests) ->
    fun(X, Acc) ->
            Test = string:substr(X, 1, length(X) - 5),
            case lists:member(Test, SkipTests) of
                true ->
                    Acc;
                false ->
                    [Test | Acc]
            end
    end.

so_kill_riak_maybe() ->
    io:format("~n~nSo, we find ourselves in a tricky situation here. ~n"),
    io:format("You've run a single test, and it has failed.~n"),
    io:format("Would you like to leave Riak running in order to debug?~n"),
    Input = io:get_chars("[Y/n] ", 1),
    case Input of
        "n" ->
            rt_cluster:teardown();
        "N" ->
            rt_cluster:teardown();
        _ ->
            io:format("Leaving Riak Up... "),
            rt:whats_up()
    end.
