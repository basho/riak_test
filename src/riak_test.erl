%% @private
-module(riak_test).
-export([main/1, all/0, run/1]).
-include_lib("eunit/include/eunit.hrl").

add_deps(Path) ->
    {ok, Deps} = file:list_dir(Path),
    [code:add_path(lists:append([Path, "/", Dep, "/ebin"])) || Dep <- Deps],
    ok.

all() ->
    [run].

main(Args) ->
    [Config, Test | HarnessArgs]=Args,
    rt:load_config(Config),

    [add_deps(Dep) || Dep <- rt:config(rt_deps)],
    ENode = rt:config(rt_nodename, 'riak_test@127.0.0.1'),
    Cookie = rt:config(rt_cookie, riak),
    [] = os:cmd("epmd -daemon"),
    net_kernel:start([ENode]),
    erlang:set_cookie(node(), Cookie),

    application:start(lager),
    LagerLevel = rt:config(rt_lager_level, debug),
    lager:set_loglevel(lager_console_backend, LagerLevel),

    %% rt:set_config(rtdev_path, Path),
    %% rt:set_config(rt_max_wait_time, 180000),
    %% rt:set_config(rt_retry_delay, 500),
    %% rt:set_config(rt_harness, rtbe),
    rt:setup_harness(Test, HarnessArgs),
    TestA = list_to_atom(Test),
    rt:set_config(rt_test, TestA),
    %% run_test().
    %% st:TestFn(),
    %% run(ok),
    case rt:config(rt_cover, false) of
        false ->
            run();
        true ->
            CoverMods = TestA:cover_modules(),
            CoverSpec = io_lib:format("~p.", [{incl_mods, CoverMods}]),
            ?assertEqual(ok, file:write_file("/tmp/rt.coverspec", CoverSpec)),
            ct:run_test([{label, Test},
                         {auto_compile, false},
                         {suite, atom_to_list(?MODULE)},
                         {cover, "/tmp/rt.coverspec"}])
    end.

run(_) ->
    run().
run() ->
    TestA = rt:config(rt_test),
    TestA:TestA(),
    rt:cleanup_harness(),
    ok.
