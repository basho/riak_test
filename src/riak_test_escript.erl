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
%% TODO: Temporary build workaround, remove!!
-compile(export_all).
-export([main/1]).
-export([add_deps/1]).

main(Args) ->
    %% TODO Should we use clique? -jsb
    %% Parse command line arguments ...
    {ParsedArgs, _NonOptionArgs} = parse_args(Args),
    load_initial_config(ParsedArgs),

    %% Configure logging ...
    OutDir = proplists:get_value(outdir, ParsedArgs, "log"),
    ensure_dir(OutDir),
    lager_setup(OutDir),

    %% Do we use GiddyUp for this run?
    Platform = report_platform(ParsedArgs),
    UseGiddyUp = case Platform of
                     undefined -> false;
                     _ -> true
                 end,
    start_giddyup(Platform),
    {Tests, NonTests} = generate_test_lists(UseGiddyUp, ParsedArgs),

    ok = prepare(ParsedArgs, Tests, NonTests),
    Results = execute(Tests, OutDir, ParsedArgs),
    finalize(Results, ParsedArgs),
    stop_giddyup(UseGiddyUp).

% @doc Validate the command-line options
parse_args(Args) ->
    validate_args(getopt:parse(cli_options(), Args)).

validate_args({ok, {[], _}}) ->
    print_help();
validate_args({ok, {ParsedArgs, NonOptionArgs}}) ->
    case proplists:is_defined(help, ParsedArgs) of
        true ->
            print_help();
        _ ->
            {ParsedArgs, NonOptionArgs}
    end;
validate_args(_) ->
    print_help().

%% Option Name, Short Code, Long Code, Argument Spec, Help Message
cli_options() ->
[
 {help,                   $h, "help",     undefined,  "Print this usage page"},
 {config,                 $c, "conf",     string,     "specifies the project configuration"},
 {tests,                  $t, "tests",    string,     "specifies which tests to run"},
 {dir,                    $d, "dir",      string,     "run all tests in the specified directory"},
 {skip,                   $x, "skip",     string,     "list of tests to skip in a directory"},
 {verbose,                $v, "verbose",  undefined,  "verbose output"},
 {outdir,                 $o, "outdir",   string,     "output directory"},
 {backend,                $b, "backend",  atom,       "backend to test [memory | bitcask | eleveldb]"},
 {keep,            undefined, "keep",     boolean,    "do not teardown cluster"},
 {continue_on_fail,       $n, "continue", boolean,    "continues executing tests on failure"},
 {report,                 $r, "report",   string,     "you're reporting an official test run, provide platform info (e.g. ubuntu-1404-64)\nUse 'config' if you want to pull from ~/.riak_test.config"},
 {file,                   $F, "file",     string,     "use the specified file instead of ~/.riak_test.config"}
].

print_help() ->
    getopt:usage(cli_options(), escript:script_name()),
    halt(0).

report_platform(ParsedArgs) ->
    case proplists:get_value(report, ParsedArgs, undefined) of
        undefined ->
            undefined;
        "config" ->
            rt_config:get(giddyup_platform);
        R ->
            R
    end.

%% @doc Print help string if it's specified, otherwise parse the arguments
generate_test_lists(UseGiddyUp, ParsedArgs) ->
    %% Have to load the `riak_test' config prior to assembling the
    %% test metadata

    TestData = compose_test_data(ParsedArgs),
    Backends = [proplists:get_value(backend, ParsedArgs, bitcask)],
    {Tests, NonTests} = wrap_test_in_test_plan(UseGiddyUp, Backends, TestData),
    Offset = rt_config:get(offset, undefined),
    Workers = rt_config:get(workers, undefined),
    shuffle_tests(Tests, NonTests, Offset, Workers).

%% @doc Set values in the configuration with values specified on the command line
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
        proplists:get_value(file, ParsedArgs)),

    %% Override any command-line settings in config
    maybe_override_setting(continue_on_fail, true, ParsedArgs).

%% @doc Shuffle the order in which tests are scheduled
shuffle_tests([], _, _, _) ->
    io:format("ERROR: No tests are scheduled to run~n"),
    lager:error("No tests are scheduled to run"),
    halt(1);
shuffle_tests(Tests, NonTests, undefined, _) ->
    {Tests, NonTests};
shuffle_tests(Tests, NonTests, _, undefined) ->
    {Tests, NonTests};
shuffle_tests(Tests, NonTests, Offset, Workers) ->
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
    {TestB ++ TestA, NonTests}.

prepare(ParsedArgs, Tests, NonTests) ->
    [lager:notice("Test to run: ~p", [rt_test_plan:get_name(Test)]) || Test <- Tests],
    case NonTests of
        [] ->
            ok;
        _ ->
            [lager:notice("Test not to run: ~p", [rt_test_plan:get_name(Test)]) || Test <- NonTests]
    end,
    ok = erlang_setup(ParsedArgs),
    test_setup().

execute(TestPlans, OutDir, ParsedArgs) ->
    UpgradeList = upgrade_list(
                    proplists:get_value(upgrade_path, ParsedArgs)),

    {ok, Executor} = riak_test_executor:start_link(TestPlans,
                                                   OutDir,
                                                   report_platform(ParsedArgs),
                                                   UpgradeList,
                                                   self()),
    wait_for_results(Executor, [], length(TestPlans), 0).


%% TODO: Use `TestCount' and `Completed' to display progress output
wait_for_results(Executor, TestResults, TestCount, Completed) ->
    receive
        {_Executor, {test_result, Result}} ->
            wait_for_results(Executor, [Result | TestResults], TestCount, Completed+1);
        {_Executor, done} ->
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
    %% Verbose = proplists:is_defined(verbose, Args),

    Teardown = not proplists:get_value(keep, Args, false),
    maybe_teardown(Teardown, TestResults),
    ok.

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

erlang_setup(_ParsedArgs) ->
    register(riak_test, self()),
    maybe_add_code_path("./ebin"),

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

%% @doc Determine which tests to run based on command-line argument
%%      If the platform is defined, consult GiddyUp, otherwise just shovel
%%      the whole thing into the Planner
-spec(load_up_test_planner(boolean(), [string()], list()) -> list()).
load_up_test_planner(true, Backends, CommandLineTests) ->
    rt_planner:load_from_giddyup(Backends, CommandLineTests);
load_up_test_planner(_, Backends, CommandLineTests) ->
    [rt_planner:add_test_plan(Name, undefined, Backends, undefined, undefined) || Name <- CommandLineTests].

%% @doc Push all of the test into the Planner for now and wrap them in an `rt_test_plan'
%% TODO: Let the Planner do the work, not the riak_test_executor
-spec(wrap_test_in_test_plan(boolean(), [string()], [atom()]) -> {list(), list()}).
wrap_test_in_test_plan(UseGiddyUp, Backends, CommandLineTests) ->
    {ok, _Pid} = rt_planner:start_link(),
    load_up_test_planner(UseGiddyUp, Backends, CommandLineTests),
    TestPlans = [rt_planner:fetch_test_plan() || _ <- lists:seq(1, rt_planner:number_of_plans())],
    NonRunnableTestPlans = [rt_planner:fetch_test_non_runnable_plan() || _ <- lists:seq(1, rt_planner:number_of_non_runable_plans())],
    rt_planner:stop(),
    {TestPlans, NonRunnableTestPlans}.

%% @doc Pull all jobs from the Planner
%%      Better than using rt_planner:number_of_plans/0
-spec(fetch_all_test_plans(list()) -> list()).
fetch_all_test_plans(Acc) ->
    Plan = rt_planner:fetch_test_plan(),
    case Plan of
        empty ->
            Acc;
        _ ->
            fetch_all_test_plans([Plan|Acc])
    end.

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

%% @doc Start the GiddyUp reporting service if the report is defined
start_giddyup(undefined) ->
    ok;
start_giddyup(Platform) ->
    {ok, _Pid} = giddyup:start_link(Platform,
                                    rt_config:get_default_version_product(),
                                    rt_config:get_default_version_number(),
                                    rt_config:get_default_version(),
                                    rt_config:get(giddyup_host),
                                    rt_config:get(giddyup_user),
                                    rt_config:get(giddyup_password)).

%% @doc Stop the GiddyUp reporting service if the report is defined
stop_giddyup(true) ->
    giddyup:stop();
stop_giddyup(_) ->
    ok.

