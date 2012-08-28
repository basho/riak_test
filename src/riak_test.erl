%% @private
-module(riak_test).
-export([main/1]).

add_deps(Path) ->
    {ok, Deps} = file:list_dir(Path),
    [code:add_path(lists:append([Path, "/", Dep, "/ebin"])) || Dep <- Deps],
    ok.


main(Args) ->
    %% @todo temporarily disabling the HarnessArgs feature until we get better
    %% command line parsing in here. Soon.
    %% [Config, Test | HarnessArgs]=Args,
    [Config | Tests] = Args,
    HarnessArgs = [],
    rt:load_config(Config),

    [add_deps(Dep) || Dep <- rt:config(rt_deps)],
    ENode = rt:config(rt_nodename, 'riak_test@127.0.0.1'),
    Cookie = rt:config(rt_cookie, riak),
    [] = os:cmd("epmd -daemon"),
    net_kernel:start([ENode]),
    erlang:set_cookie(node(), Cookie),
    
    %% Start Lager
    application:load(lager),
    LagerLevel = rt:config(rt_lager_level, debug),
    application:set_env(lager, handlers, [{lager_console_backend, LagerLevel}]),
    lager:start(),
    
    %% rt:set_config(rtdev_path, Path),
    %% rt:set_config(rt_max_wait_time, 180000),
    %% rt:set_config(rt_retry_delay, 500),
    %% rt:set_config(rt_harness, rtbe),
    TestResults = [ run_test(Test, HarnessArgs) || Test <- Tests],
    
    print_summary(TestResults),
    ok.

run_test(Test, HarnessArgs) ->
    rt:setup_harness(Test, HarnessArgs),
    TestA = list_to_atom(Test),
    SingleTestResult = riak_test_runner:confirm(TestA),
    rt:cleanup_harness(),
    SingleTestResult.
    
print_summary(TestResults) ->
    io:format("~nTest Results:~n"),
    
    Results = [ [ atom_to_list(proplists:get_value(test, SingleTestResult)),
        proplists:get_value(status, SingleTestResult)] || SingleTestResult <- TestResults],
    Width = test_name_width(Results),
    [ io:format("~s: ~s~n", [string:left(Name, Width), Result]) || [Name, Result] <- Results],
    
    PassCount = length(lists:filter(fun(X) -> proplists:get_value(status, X) =:= pass end, TestResults)),
    FailCount = length(lists:filter(fun(X) -> proplists:get_value(status, X) =:= fail end, TestResults)),
    io:format("---------------------------------------------~n"),
    io:format("~w Tests Failed~n", [FailCount]),
    io:format("~w Tests Passed~n", [PassCount]),
    io:format("That's ~w% for those keeping score~n", [(PassCount / (PassCount + FailCount)) * 100]),
    ok.
    
test_name_width(Results) ->
    lists:max([ length(X) || [X | _T] <- Results ]).