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
 {outdir,             $o, "outdir",           string,           "output directory"}
].


main(Args) ->
    {ok, {ParsedArgs, HarnessArgs}} = getopt:parse(cli_options(), Args),
        
    Verbose = proplists:is_defined(verbose, ParsedArgs),
    Config = proplists:get_value(config, ParsedArgs),
    SpecificTests = proplists:get_all_values(tests, ParsedArgs),
    Suites = proplists:get_all_values(suites, ParsedArgs),
    case Suites of
        [] -> ok;
        _ -> io:format("Suites are not currently supported.")
    end,
    
    Dirs = proplists:get_all_values(dir, ParsedArgs),
    DirTests = lists:append([load_tests_in_dir(Dir) || Dir <- Dirs]),
    %%case Dirs of
    %%    [] -> ok;
    %%    _ -> io:format("Directories are not currently supported.")
    %%end,
    
    Tests = lists:foldr(fun(X, AccIn) -> 
                            case lists:member(X, AccIn) of
                                true -> AccIn;
                                _ -> [X | AccIn]
                            end
                        end, [], lists:sort(DirTests ++ SpecificTests)),
    io:format("Tests to run: ~p~n", [Tests]),
    
    rt:load_config(Config),

    [add_deps(Dep) || Dep <- rt:config(rt_deps)],
    ENode = rt:config(rt_nodename, 'riak_test@127.0.0.1'),
    Cookie = rt:config(rt_cookie, riak),
    [] = os:cmd("epmd -daemon"),
    net_kernel:start([ENode]),
    erlang:set_cookie(node(), Cookie),
    
    %% Start Lager
    application:load(lager),
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
    
    %% rt:set_config(rtdev_path, Path),
    %% rt:set_config(rt_max_wait_time, 180000),
    %% rt:set_config(rt_retry_delay, 500),
    %% rt:set_config(rt_harness, rtbe),
        
    TestResults = [ run_test(Test, Outdir, HarnessArgs) || Test <- Tests],
    
    print_summary(TestResults, Verbose),
    ok.

run_test(Test, Outdir, HarnessArgs) ->
    rt:setup_harness(Test, HarnessArgs),
    TestA = list_to_atom(Test),
    SingleTestResult = riak_test_runner:confirm(TestA, Outdir),
    rt:cleanup_harness(),
    SingleTestResult.
    
print_summary(TestResults, Verbose) ->
    io:format("~nTest Results:~n"),
    
    Results = [ 
                [ atom_to_list(proplists:get_value(test, SingleTestResult)),
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
