%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(riak_test).
-export([main/1]).

add_deps(Path) ->
    {ok, Deps} = file:list_dir(Path),
    [code:add_path(lists:append([Path, "/", Dep, "/ebin"])) || Dep <- Deps],
    ok.

cli_options() ->
%% Option Name, Short Code, Long Code, Argument Spec, Help Message
[
 {help,               $h, "help",             undefined,        "Print this usage page"},
 {config,             $c, "conf",             string,           "specifies the project configuration"},
 {tests,              $t, "tests",            string,           "specifies which tests to run"},
 {suites,             $s, "suites",           string,           "which suites to run"},
 {dir,                $d, "dir",              string,           "run all tests in the specified directory"},
 {verbose,            $v, "verbose",          undefined,        "verbose output"},
 {outdir,             $o, "outdir",           string,           "output directory"},
 {backend,            $b, "backend",          atom,             "backend to test [memory | bitcask | eleveldb]"},
 {report,             $r, "report",           string,           "you're reporting an official test run, provide platform info (e.g. ubuntu-1204-64)"}
].

print_help() ->
    getopt:usage(cli_options(),
                 escript:script_name()),
    halt(0).

run_help([]) -> true;
run_help(ParsedArgs) ->
    lists:member(help, ParsedArgs).

main(Args) ->
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
    rt:load_config(Config),

    %% Fileoutput
    Outdir = proplists:get_value(outdir, ParsedArgs),
    ConsoleLagerLevel = case Outdir of
        undefined -> rt:config(rt_lager_level, debug);
        _ ->
            filelib:ensure_dir(Outdir),
            notice
    end,

    application:set_env(lager, handlers, [{lager_console_backend, ConsoleLagerLevel}]),
    lager:start(),

    Report = proplists:get_value(report, ParsedArgs, undefined),
    Verbose = proplists:is_defined(verbose, ParsedArgs),

    Suites = proplists:get_all_values(suites, ParsedArgs),
    case Suites of
        [] -> ok;
        _ -> io:format("Suites are not currently supported.")
    end,

    Version = rt:get_version(),

    Backends = case proplists:get_all_values(backend, ParsedArgs) of
        [] -> [bitcask];
        Other -> Other
    end,

    Tests = case Report of
        undefined ->
            SpecificTests = proplists:get_all_values(tests, ParsedArgs),
            Dirs = proplists:get_all_values(dir, ParsedArgs),
            DirTests = lists:append([load_tests_in_dir(Dir) || Dir <- Dirs]),
            lists:foldl(fun(Test, Tests) ->
                    [{
                      list_to_atom(Test),
                      [
                          {id, -1},
                          {backend, Backend},
                          {platform, <<"local">>},
                          {version, Version},
                          {project, list_to_binary(rt:config(rt_project, "undefined"))}
                      ]
                    } || Backend <- Backends ] ++ Tests
                end, [], lists:usort(DirTests ++ SpecificTests));
        Platform ->
            giddyup:get_suite(Platform)
    end,
    io:format("Tests to run: ~p~n", [Tests]),

    [add_deps(Dep) || Dep <- rt:config(rt_deps)],
    ENode = rt:config(rt_nodename, 'riak_test@127.0.0.1'),
    Cookie = rt:config(rt_cookie, riak),
    [] = os:cmd("epmd -daemon"),
    net_kernel:start([ENode]),
    erlang:set_cookie(node(), Cookie),

    TestResults = [ run_test(Test, Outdir, TestMetaData, Report, HarnessArgs) || {Test, TestMetaData} <- Tests],
    print_summary(TestResults, Verbose),
    
    case {length(TestResults), proplists:get_value(status, hd(TestResults))} of
        {1, fail} -> 
            so_kill_riak_maybe();
        _ ->
            lager:info("Multiple tests run or no failure"),
            rt:teardown()
    end,
    ok.

run_test(Test, Outdir, TestMetaData, Report, _HarnessArgs) ->
    SingleTestResult = riak_test_runner:confirm(Test, Outdir, TestMetaData),
    rt:cleanup_harness(),
    case Report of
        undefined -> ok;
        _ -> giddyup:post_result(SingleTestResult)
    end,
    SingleTestResult.

print_summary(TestResults, Verbose) ->
    io:format("~nTest Results:~n"),

    Results = [
                [ atom_to_list(proplists:get_value(test, SingleTestResult)) ++ "-" ++
                  atom_to_list(proplists:get_value(backend, SingleTestResult)),
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
    ok.

test_name_width(Results) ->
    lists:max([ length(X) || [X | _T] <- Results ]).

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