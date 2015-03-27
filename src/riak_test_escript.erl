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
%% TODO: Temporary build workaround, remove!!
-compile(export_all).
-export([main/1]).
-export([add_deps/1]).

-define(HEADER, [<<"Test">>, <<"Result">>, <<"Reason">>, <<"Test Duration">>]).

main(Args) ->
    %% TODO Should we use clique? -jsb
    %% Parse command line arguments ...
    {ParsedArgs, HarnessArgs, Tests, NonTests} = parse_args(Args),

    %% Configure logging ...
    OutDir = proplists:get_value(outdir, ParsedArgs, "log"),
    ensure_dir(OutDir),

    lager_setup(OutDir),
    
    ok = prepare(ParsedArgs, Tests, NonTests),
    Results = execute(Tests, ParsedArgs, HarnessArgs),
    finalize(Results, ParsedArgs).

prepare(ParsedArgs, Tests, NonTests) ->
    lager:notice("Tests to run: ~p~n", [Tests]),
    case NonTests of
        [] ->
            ok;
        _ ->
            lager:notice("These modules are not runnable tests: ~p~n",
                      [[NTMod || {NTMod, _} <- NonTests]])
    end,
    ok = erlang_setup(ParsedArgs),
    test_setup().

execute(Tests, ParsedArgs, _HarnessArgs) ->
    OutDir = proplists:get_value(outdir, ParsedArgs),
    Report = report(ParsedArgs),
    UpgradeList = upgrade_list(
                    proplists:get_value(upgrade_path, ParsedArgs)),
    Backend = proplists:get_value(backend, ParsedArgs, bitcask),

    {ok, Executor} = riak_test_executor:start_link(Tests,
                                                   Backend,
                                                   OutDir,
                                                   Report,
                                                   UpgradeList,
                                                   self()),
    wait_for_results(Executor, [], length(Tests), 0).


report_results(Results, Verbose) ->
    %% TODO: Good place to also do giddyup reporting and provide a
    %% place for extending to any other reporting sources that might
    %% be useful.
    print_summary(Results, undefined, Verbose),
    ok.

    %% TestResults = run_tests(Tests, Outdir, Report, HarnessArgs),
    %% lists:filter(fun results_filter/1, TestResults).

%% run_test(Test, Outdir, TestMetaData, Report, HarnessArgs, NumTests) ->
%%     rt_cover:maybe_start(Test),
%%     SingleTestResult = riak_test_runner:run(Test, Outdir, TestMetaData, HarnessArgs),

    %% case NumTests of
    %%     1 -> keep_them_up;
    %%     _ -> rt_cluster:teardown()
    %% end,

%% TODO: Do this in the test runner
%%     CoverDir = rt_config:get(cover_output, "coverage"),
%% CoverFile = rt_cover:maybe_export_coverage(Test, CoverDir, erlang:phash2(TestMetaData)),
%% publish_report(SingleTestResult, CoverFile, Report),

%%     [{coverdata, CoverFile} | SingleTestResult].

%% TODO: Use `TestCount' and `Completed' to display progress output
wait_for_results(Executor, TestResults, TestCount, Completed) ->
    receive
        {Executor, {test_result, Result}} ->
            wait_for_results(Executor, [Result | TestResults], TestCount, Completed+1);
        {Executor, done} ->
            rt_cover:stop(),
            TestResults;
        _ ->
            wait_for_results(Executor, TestResults, TestCount, Completed)
    end.

finalize(TestResults, Args) ->
    %% TODO: Fixup coverage reporting
    %% [rt_cover:maybe_import_coverage(proplists:get_value(coverdata, R)) ||
    %%     R <- TestResults],
    %% CoverDir = rt_config:get(cover_output, "coverage"),
    %% Coverage = rt_cover:maybe_write_coverage(all, CoverDir),
    Verbose = proplists:is_defined(verbose, Args),
    report_results(TestResults, Verbose),

    Teardown = not proplists:get_value(keep, Args, false),
    maybe_teardown(Teardown, TestResults),
    ok.
%% Option Name, Short Code, Long Code, Argument Spec, Help Message
cli_options() ->
[
 {help,                   $h, "help",     undefined,  "Print this usage page"},
 {config,                 $c, "conf",     string,     "specifies the project configuration"},
 {tests,                  $t, "tests",    string,     "specifies which tests to run"},
 {suites,                 $s, "suites",   string,     "which suites to run"},
 {groups,                 $g, "groups",   string,     "specifiy a list of test groups to run"},
 {dir,                    $d, "dir",      string,     "run all tests in the specified directory"},
 {skip,                   $x, "skip",     string,     "list of tests to skip in a directory"},
 {verbose,                $v, "verbose",  undefined,  "verbose output"},
 {outdir,                 $o, "outdir",   string,     "output directory"},
 {backend,                $b, "backend",  atom,       "backend to test [memory | bitcask | eleveldb]"},
 {upgrade_path,           $u, "upgrade-path", atom,   "comma-separated list representing an upgrade path (e.g. riak-1.3.4,riak_ee-1.4.12,riak_ee-2.0.0)"},
 {keep,            undefined, "keep",     boolean,    "do not teardown cluster"},
 {continue_on_fail,undefined, "continue", boolean,    "continues executing tests on failure"},
 {report,                 $r, "report",   string,     "you're reporting an official test run, provide platform info (e.g. ubuntu-1404-64)\nUse 'config' if you want to pull from ~/.riak_test.config"},
 {file,                   $F, "file",     string,     "use the specified file instead of ~/.riak_test.config"}
].

print_help() ->
    getopt:usage(cli_options(), escript:script_name()),
    halt(0).

add_deps(Path) ->
    lager:debug("Adding dep path ~p", [Path]),
    case file:list_dir(Path) of
        {ok, Deps} ->
            [code:add_path(lists:append([Path, "/", Dep, "/ebin"])) || Dep <- Deps],
            ok;
        {error, Reason} ->
            lager:error("Failed to add dep path ~p due to ~p.", [Path, Reason]),
            erlang:error(Reason)
    end.

test_setup() ->
    %% Prepare the test harness
    {NodeIds, NodeMap, VersionMap} = rt_harness:setup(),

    %% Start the node manager
    _ = node_manager:start_link(NodeIds, NodeMap, VersionMap),

    %% Ensure existence of scratch_dir
    case ensure_dir(rt_config:get(rt_scratch_dir) ++ "/test.file") of
        ok ->
            great;
        {error, ErrorReason} ->
            lager:error("Could not create scratch dir, ~p",
                        [ErrorReason])
    end,
    ok.

-spec upgrade_list(undefined | string()) -> undefined | [string()].
upgrade_list(undefined) ->
    undefined;
upgrade_list(Path) ->
    string:tokens(Path, ",").

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

%% @doc Print help string if it's specified, otherwise parse the arguments
help_or_parse_args({ok, {[], _}}) ->
    print_help();
help_or_parse_args({ok, {ParsedArgs, HarnessArgs}}) ->
    help_or_parse_tests(ParsedArgs,
                        HarnessArgs,
                        lists:member(help, ParsedArgs));
help_or_parse_args(_) ->
    print_help().

help_or_parse_tests(_, _, true) ->
    print_help();
help_or_parse_tests(ParsedArgs, HarnessArgs, false) ->
    %% Have to load the `riak_test' config prior to assembling the
    %% test metadata
    load_initial_config(ParsedArgs),

    maybe_override_setting(continue_on_fail, true, ParsedArgs),

    TestData = compose_test_data(ParsedArgs),
    {Tests, NonTests} = which_tests_to_run(report(ParsedArgs), TestData),
    Offset = rt_config:get(offset, undefined),
    Workers = rt_config:get(workers, undefined),
    shuffle_tests(ParsedArgs, HarnessArgs, Tests, NonTests, Offset, Workers).

maybe_override_setting(Argument, Value, Arguments) ->
    maybe_override_setting(proplists:is_defined(Argument, Arguments), Argument, 
                           Value, Arguments).

maybe_override_setting(true, Argument, Value, Arguments) ->
    rt_config:set(Argument, proplists:get_value(Argument, Arguments, Value));
maybe_override_setting(false, _Argument, _Value, _Arguments) ->
    ok.

load_initial_config(ParsedArgs) ->
    %% Loads application defaults
    application:load(riak_test),

    %% Loads from ~/.riak_test.config
    rt_config:load(proplists:get_value(config, ParsedArgs),
                   proplists:get_value(file, ParsedArgs)).

shuffle_tests(_, _, [], _, _, _) ->
    lager:error("No tests are scheduled to run~n"),
    halt(1);
shuffle_tests(ParsedArgs, HarnessArgs, Tests, NonTests, undefined, _) ->
    {ParsedArgs, HarnessArgs, Tests, NonTests};
shuffle_tests(ParsedArgs, HarnessArgs, Tests, NonTests, _, undefined) ->
    {ParsedArgs, HarnessArgs, Tests, NonTests};
shuffle_tests(ParsedArgs, HarnessArgs, Tests, NonTests, Offset, Workers) ->
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
    {ParsedArgs, HarnessArgs, TestB ++ TestA, NonTests}.

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

lager_setup(OutputDir) ->
    set_lager_env(OutputDir,
                  rt_config:get(lager_console_level, notice),
                  rt_config:get(lager_file_level, info)),
    lager:start().

set_lager_env(OutputDir, ConsoleLevel, FileLevel) ->
    application:load(lager),
    HandlerConfig = [{lager_console_backend, ConsoleLevel},
                     {lager_file_backend, [{file, filename:join(OutputDir, "test.log")},
                                           {level, FileLevel}]}],
    application:set_env(lager, handlers, HandlerConfig).

maybe_teardown(false, _TestResults) ->
    lager:info("Keeping cluster running as requested");
maybe_teardown(Keep, TestResults) when is_list(TestResults) andalso 
                                       erlang:length(TestResults) == 1 ->
    maybe_teardown(Keep, hd(TestResults));
maybe_teardown(true, {_, {fail, _}, _}) ->
    so_kill_riak_maybe(),
    ok;
maybe_teardown(true, _TestResults) ->
    lager:info("Multiple tests run or no failure"),
    rt_cluster:teardown(),
    ok.

-spec comma_tokenizer(string(), [string()]) -> [string()].
comma_tokenizer(S, Acc) ->
    string:tokens(S, ", ") ++ Acc.

compose_test_data(ParsedArgs) ->
    RawTestList = proplists:get_all_values(tests, ParsedArgs),
    RawGroupList = proplists:get_all_values(groups, ParsedArgs),
    TestList = lists:foldl(fun comma_tokenizer/2, [], RawTestList),
    GroupList = lists:foldl(fun comma_tokenizer/2, [], RawGroupList),

    %% Parse Command Line Tests
    {CodePaths, SpecificTests} =
        lists:foldl(fun extract_test_names/2,
                    {[], []},
                    TestList),

    [code:add_patha(CodePath) || CodePath <- CodePaths,
                                 CodePath /= "."],

    Dirs = get_test_dirs(ParsedArgs, default_test_dir(GroupList)),
    SkipTests = string:tokens(proplists:get_value(skip, ParsedArgs, []), [$,]),
    DirTests = lists:append([load_tests_in_dir(Dir, GroupList, SkipTests) || Dir <- Dirs]),
    lists:usort(DirTests ++ SpecificTests).

-spec default_test_dir([string()]) -> [string()].
%% @doc If any groups have been specified then we want to check in the
%% local test directory by default; otherwise, the default behavior is
%% that no directory is used to pull tests from.
default_test_dir([]) ->
    [];
default_test_dir(_) ->
    ["./ebin"].

-spec get_test_dirs(term(), [string()]) -> [string()].
get_test_dirs(ParsedArgs, DefaultDirs) ->
    case proplists:get_all_values(dir, ParsedArgs) of
        [] ->
            DefaultDirs;
        Dirs ->
            Dirs
    end.

extract_test_names(Test, {CodePaths, TestNames}) ->
    {[filename:dirname(Test) | CodePaths],
     [list_to_atom(filename:rootname(filename:basename(Test))) | TestNames]}.

which_tests_to_run(undefined, CommandLineTests) ->
    lists:partition(fun is_runnable_test/1, CommandLineTests);
which_tests_to_run(Platform, []) ->
    giddyup:get_suite(Platform);
which_tests_to_run(Platform, CommandLineTests) ->
    Suite = filter_zip_suite(Platform, CommandLineTests),
    lists:partition(fun is_runnable_test/1,
                    lists:foldr(fun filter_merge_tests/2, [], Suite)).

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
is_runnable_test(TestModule) ->
    {Mod, Fun} = riak_test_runner:function_name(confirm, TestModule),
    code:ensure_loaded(Mod),
    erlang:function_exported(Mod, Fun, 0) orelse
        erlang:function_exported(Mod, Fun, 1).

get_group_tests(Tests, Groups) ->
    lists:filter(fun(Test) ->
                         Mod = list_to_atom(Test),
                         Attrs = Mod:module_info(attributes),
                         match_group_attributes(Attrs, Groups)
                 end, Tests).

match_group_attributes(Attributes, Groups) ->
    case proplists:get_value(test_type, Attributes) of
        undefined ->
            false;
        TestTypes ->
            lists:member(true,
                         [ TestType == list_to_atom(Group)
                           || Group <- Groups, TestType <- TestTypes ])
    end.

%% run_tests(Tests, Outdir, Report, HarnessArgs) ->
    %% Need properties for tests prior to getting here Need server to
    %% manage the aquisition of nodes and to handle comparison of test
    %% `node_count' property with resources available. Also handle
    %% notification of test completion. Hmm, maybe test execution
    %% should be handled by a `gen_fsm' at this point to distinguish
    %% the case when there are tests left to be tried with available
    %% resources versus all have been tried or resources are
    %% exhausted.

%% [run_test(Test,
%%                             Outdir,
%%                             TestMetaData,
%%                             Report,
%%                             HarnessArgs,
%%                             TestCount) ||
%%                       {Test, TestMetaData} <- Tests],

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

test_summary_format_time(Milliseconds) ->
    Mills = trunc(((Milliseconds / 1000000) - (Milliseconds div 1000000)) * 1000000),
    TotalSecs = (Milliseconds - Mills) div 1000000,
    TotalMins = TotalSecs div 60,
    Hours = TotalSecs div 3600,
    Secs = TotalSecs - (TotalMins * 60),
    Mins = TotalMins - (Hours * 60),
    list_to_binary(io_lib:format("~ph ~pm ~p.~ps", [Hours, Mins, Secs, Mills])).


test_summary_fun({Test, pass, _}, {{Pass, _Fail, _Skipped}, Width}) ->
    TestNameLength = length(atom_to_list(Test)),
    UpdWidth =
        case TestNameLength > Width of
            true ->
                TestNameLength;
            false ->
                Width
        end,
    {{Pass+1, _Fail, _Skipped}, UpdWidth};
test_summary_fun({Test, {fail, _}, _}, {{_Pass, Fail, _Skipped}, Width}) ->
    TestNameLength = length(atom_to_list(Test)),
    UpdWidth =
        case TestNameLength > Width of
            true ->
                TestNameLength;
            false ->
                Width
        end,
    {{_Pass, Fail+1, _Skipped}, UpdWidth};
test_summary_fun({Test, {skipped, _}, _}, {{_Pass, _Fail, Skipped}, Width}) ->
    TestNameLength = length(atom_to_list(Test)),
    UpdWidth =
        case TestNameLength > Width of
            true ->
                TestNameLength;
            false ->
                Width
        end,
    {{_Pass, _Fail, Skipped+1}, UpdWidth}.

format_test_row({Test, Result, Duration}, _Width) ->
    TestString = atom_to_list(Test),
    case Result of
        {Status, Reason} ->
            [TestString, Status, Reason, test_summary_format_time(Duration)];
        pass ->
            [TestString, "pass", "N/A", test_summary_format_time(Duration)]
    end.

print_summary(TestResults, _CoverResult, Verbose) ->
    %% TODO Log vs console output ... -jsb
    io:format("~nTest Results:~n~n"),

    {StatusCounts, Width} = lists:foldl(fun test_summary_fun/2, {{0,0,0}, 0}, TestResults),

    case Verbose of
        true ->
            Rows =
                [format_test_row(Result, Width) || Result <- TestResults],
            Table = clique_table:autosize_create_table(?HEADER, Rows),
            io:format("~ts~n", [Table]);
        false ->
            ok
    end,

    {PassCount, FailCount, SkippedCount} = StatusCounts,
    io:format("---------------------------------------------~n"),
    io:format("~w Tests Failed~n", [FailCount]),
    io:format("~w Tests Skipped~n", [SkippedCount]),
    io:format("~w Tests Passed~n", [PassCount]),
    Percentage = case PassCount == 0 andalso FailCount == 0 of
        true -> 0;
        false -> (PassCount / (PassCount + FailCount + SkippedCount)) * 100
    end,
    io:format("That's ~w% for those keeping score~n", [Percentage]),

    %% case CoverResult of
    %%     cover_disabled ->
    %%         ok;
    %%     {Coverage, AppCov} ->
    %%         io:format("Coverage : ~.1f%~n", [Coverage]),
    %%         [io:format("    ~s : ~.1f%~n", [App, Cov])
    %%          || {App, Cov, _} <- AppCov]
    %% end,
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

load_tests_in_dir(Dir, Groups, SkipTests) ->
    case filelib:is_dir(Dir) of
        true ->
            
            lists:sort(
              lists:foldl(load_tests_folder(Groups, SkipTests),
                          [],
                          filelib:wildcard("*.beam", Dir)));
        _ ->
            io:format("~s is not a dir!~n", [Dir])
    end.

load_tests_folder([], SkipTests) ->
    fun(X, Acc) ->
            %% Drop the .beam suffix
            Test = string:substr(X, 1, length(X) - 5),
            case lists:member(Test, SkipTests) of
                true ->
                    Acc;
                false ->
                    [list_to_atom(Test) | Acc]
            end
    end;
load_tests_folder(Groups, SkipTests) ->
    fun(X, Acc) ->
            %% Drop the .beam suffix
            Test = string:substr(X, 1, length(X) - 5),
            case group_match(Test, Groups)
                andalso not lists:member(Test, SkipTests) of
                true ->
                    [list_to_atom(Test) | Acc];
                false ->
                    Acc
            end
    end.

-spec group_match(string(), [string()]) -> boolean().
group_match(Test, Groups) ->
    Mod = list_to_atom(Test),
    Attrs = Mod:module_info(attributes),
    match_group_attributes(Attrs, Groups).

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
