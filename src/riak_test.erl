%% @private
-module(riak_test).
-export([main/1]).

add_deps(Path) ->
    {ok, Deps} = file:list_dir(Path),
    [code:add_path(lists:append([Path, "/", Dep, "/ebin"])) || Dep <- Deps],
    ok.


main(Args) ->
    [Config, Test | HarnessArgs]=Args,
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
    
    %% add handler for specific test.
    gen_event:add_handler(lager_event, riak_test_lager_backend, [LagerLevel, false]),
    
    %% rt:set_config(rtdev_path, Path),
    %% rt:set_config(rt_max_wait_time, 180000),
    %% rt:set_config(rt_retry_delay, 500),
    %% rt:set_config(rt_harness, rtbe),
    rt:setup_harness(Test, HarnessArgs),
    TestA = list_to_atom(Test),
    %% st:TestFn(),
    TestA:TestA(),
    rt:cleanup_harness(),
    
    %% Custom Logging Voodoo
    {ok, Logs} = gen_event:delete_handler(lager_event, riak_test_lager_backend, []),
    io:format("Handled Log: ~n"),
    [ io:put_chars(user, [Log, "\n"]) || Log <- Logs ],
    
    ok.
