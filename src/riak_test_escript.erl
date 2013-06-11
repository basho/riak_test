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
-export([main/1]).

add_deps(Path) ->
    {ok, Deps} = file:list_dir(Path),
    [code:add_path(lists:append([Path, "/", Dep, "/ebin"])) || Dep <- Deps],
    ok.

cli_options() ->
%% Option Name, Short Code, Long Code, Argument Spec, Help Message
[
 {help,               $h, "help",     undefined,  "Print this usage page"},
 {config,             $c, "conf",     string,     "specifies the project configuration"},
 {tests,              $t, "tests",    string,     "specifies which tests to run"},
 {suites,             $s, "suites",   string,     "which suites to run"},
 {dir,                $d, "dir",      string,     "run all tests in the specified directory"},
 {verbose,            $v, "verbose",  undefined,  "verbose output"},
 {outdir,             $o, "outdir",   string,     "output directory"},
 {backend,            $b, "backend",  atom,       "backend to test [memory | bitcask | eleveldb]"},
 {upgrade_version,    $u, "upgrade",  atom,       "which version to upgrade from [ previous | legacy ]"},
 {report,             $r, "report",   string,     "you're reporting an official test run, provide platform info (e.g. ubuntu-1204-64)\nUse 'config' if you want to pull from ~/.riak_test.config"}
].

print_help() ->
    getopt:usage(cli_options(),
                 escript:script_name()),
    halt(0).

run_help([]) -> true;
run_help(ParsedArgs) ->
    lists:member(help, ParsedArgs).

main(Args) ->
    case filelib:is_dir("./ebin") of
        true ->
            code:add_patha("./ebin");
        _ ->
            meh
    end,

    register(riak_test, self()),
    {ParsedArgs, HarnessArgs} = case getopt:parse(cli_options(), Args) of
        {ok, {P, H}} -> {P, H};
        _ -> print_help()
    end,

    case run_help(ParsedArgs) of
        true -> print_help();
        _ -> ok
    end,

    %% ibrowse
    application:load(ibrowse),
    application:start(ibrowse),
    %% Start Lager
    application:load(lager),
    Config = proplists:get_value(config, ParsedArgs),

    %% Loads application defaults
    application:load(riak_test),

    %% Loads from ~/.riak_test.config
    rt_config:load(Config),

    %% Ensure existance of scratch_dir
    case file:make_dir(rt_config:get(rt_scratch_dir)) of
        ok -> great;
        {eexist, _} -> great;
        {ErrorType, ErrorReason} -> lager:error("Could not create scratch dir, {~p, ~p}", [ErrorType, ErrorReason])
    end,

    %% Fileoutput
    Outdir = proplists:get_value(outdir, ParsedArgs),
    ConsoleLagerLevel = case Outdir of
        undefined -> rt_config:get(lager_level, info);
        _ ->
            filelib:ensure_dir(Outdir),
            notice
    end,

    application:set_env(lager, handlers, [{lager_console_backend, ConsoleLagerLevel}]),
    lager:start(),

    %% Report
    Report = case proplists:get_value(report, ParsedArgs, undefined) of
        undefined -> undefined;
        "config" -> rt_config:get(platform, undefined);
        R -> R
    end,

    Verbose = proplists:is_defined(verbose, ParsedArgs),

    Suites = proplists:get_all_values(suites, ParsedArgs),
    case Suites of
        [] -> ok;
        _ -> io:format("Suites are not currently supported.")
    end,

    CommandLineTests = parse_command_line_tests(ParsedArgs),
    Tests = which_tests_to_run(Report, CommandLineTests),

    case Tests of
        [] ->
            lager:warning("No tests are scheduled to run"),
            init:stop(1);
        _ -> keep_on_keepin_on
    end,

    io:format("Tests to run: ~p~n", [Tests]),
    %% Two hard-coded deps...
    add_deps(rt:get_deps()),
    add_deps("deps"),

    [add_deps(Dep) || Dep <- rt_config:get(rt_deps, [])],
    ENode = rt_config:get(rt_nodename, 'riak_test@127.0.0.1'),
    Cookie = rt_config:get(rt_cookie, riak),
    CoverDir = rt_config:get(cover_output, "coverage"),
    [] = os:cmd("epmd -daemon"),
    net_kernel:start([ENode]),
    erlang:set_cookie(node(), Cookie),
    rt_cover:maybe_start(),

    TestResults = lists:filter(fun results_filter/1, [ run_test(Test, Outdir, TestMetaData, Report, HarnessArgs, length(Tests)) || {Test, TestMetaData} <- Tests]),
    Coverage = rt_cover:maybe_write_coverage(all, CoverDir),

    case {length(TestResults), proplists:get_value(status, hd(TestResults))} of
        {1, fail} ->
            print_summary(TestResults, Coverage, Verbose),
            so_kill_riak_maybe();
        _ ->
            lager:info("Multiple tests run or no failure"),
            rt:teardown(),
            print_summary(TestResults, Coverage, Verbose)
    end,
    ok.

parse_command_line_tests(ParsedArgs) ->
    Backends = case proplists:get_all_values(backend, ParsedArgs) of
        [] -> [undefined];
        Other -> Other
    end,
    Upgrades = case proplists:get_all_values(upgrade_version, ParsedArgs) of
                   [] -> [undefined];
                   UpgradeList -> UpgradeList
               end,
    %% Parse Command Line Tests
    {CodePaths, SpecificTests} =
        lists:foldl(fun extract_test_names/2,
                    {[], []},
                    proplists:get_all_values(tests, ParsedArgs)),
    [code:add_patha(CodePath) || CodePath <- CodePaths,
                                 CodePath /= "."],
    Dirs = proplists:get_all_values(dir, ParsedArgs),
    DirTests = lists:append([load_tests_in_dir(Dir) || Dir <- Dirs]),
    lists:foldl(fun(Test, Tests) ->
            [{
              list_to_atom(Test),
              [
                  {id, -1},
                  {platform, <<"local">>},
                  {version, rt:get_version()},
                  {project, list_to_binary(rt_config:get(rt_project, "undefined"))}
              ] ++
              [ {backend, Backend} || Backend =/= undefined ] ++
              [ {upgrade_version, Upgrade} || Upgrade =/= undefined ]}
             || Backend <- Backends,
                Upgrade <- Upgrades ] ++ Tests
        end, [], lists:usort(DirTests ++ SpecificTests)).

extract_test_names(Test, {CodePaths, TestNames}) ->
    {[filename:dirname(Test) | CodePaths],
     [filename:rootname(filename:basename(Test)) | TestNames]}.

which_tests_to_run(undefined, CommandLineTests) ->
    {Tests, NonTests} =
        lists:partition(fun is_runnable_test/1, CommandLineTests),
    lager:info("These modules are not runnable tests: ~p",
               [[NTMod || {NTMod, _} <- NonTests]]),
    Tests;
which_tests_to_run(Platform, []) -> giddyup:get_suite(Platform);
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
    {Mod, Fun} = riak_test_runner:function_name(TestModule),
    code:ensure_loaded(Mod),
    erlang:function_exported(Mod, Fun, 0).

run_test(Test, Outdir, TestMetaData, Report, _HarnessArgs, NumTests) ->
    SingleTestResult = riak_test_runner:confirm(Test, Outdir, TestMetaData),
    case NumTests of
        1 -> keep_them_up;
        _ -> rt:teardown()
    end,
    case Report of
        undefined -> ok;
        _ ->
            %% Old Code for concatinating log files for upload to giddyup
            %% They're too big now, causing problems which will be solved by
            %% GiddyUp's new Artifact feature, comming soon from a Cribbs near you.

            %% The point is, this is here in case we need to turn this back on
            %% before artifacts are ready. And to remind jd that this is the place
            %% to write the artifact client

            %% {log, TestLog} = lists:keyfind(log, 1, SingleTestResult),
            %% NodeLogs = cat_node_logs(),
            %% EncodedNodeLogs = unicode:characters_to_binary(iolist_to_binary(NodeLogs),
            %%                                                latin1, utf8),
            %% NewLogs = iolist_to_binary([TestLog, EncodedNodeLogs]),
            %% ResultWithNodeLogs = lists:keyreplace(log, 1, SingleTestResult,
            %%                                       {log, NewLogs}),
            %% giddyup:post_result(ResultWithNodeLogs)
            case giddyup:post_result(SingleTestResult) of
                error -> woops;
                {ok, Base} ->
                    %% Now push up the artifacts
                    [ giddyup:post_artifact(Base, File) || File <- rt:get_node_logs() ]
            end                    
    end,
    SingleTestResult.

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

load_tests_in_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            code:add_path(Dir),
            lists:sort([ string:substr(Filename, 1, length(Filename) - 5) || Filename <- filelib:wildcard("*.beam", Dir)]);
        _ -> io:format("~s is not a dir!~n", [Dir])
    end.

so_kill_riak_maybe() ->
    io:format("~n~nSo, we find ourselves in a tricky situation here. ~n"),
    io:format("You've run a single test, and it has failed.~n"),
    io:format("Would you like to leave Riak running in order to debug?~n"),
    Input = io:get_chars("[Y/n] ", 1),
    case Input of
        "n" -> rt:teardown();
        "N" -> rt:teardown();
        _ ->
            io:format("Leaving Riak Up... "),
            rt:whats_up()
    end.
