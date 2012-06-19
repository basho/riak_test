%% @private
-module(riak_test).
-export([main/1]).
-include_lib("eunit/include/eunit.hrl").

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
    rt:if_coverage(fun rt:cover_compile/1, [TestA]),
    TestA = rt:config(rt_test),
    TestA:TestA(),
    rt:if_coverage(fun cover_analyze_file/1, [TestA]),
    rt:cleanup_harness(),
    ok.

cover_analyze_file(_TestMod) ->
    ok = filelib:ensure_dir("cover/index.html"),
    %% Modules = rt:cover_modules(TestMod),
    Modules = cover:modules(),
    %% io:format("~p~n", [coverage_summary(Modules)]),
    lists:foreach(fun generate_coverage/1, Modules),
    generate_coverage_summary(Modules).

generate_coverage(Mod) ->
    {ok, _File} = cover:analyze_to_file(Mod, cover_file("cover", Mod), [html]),
    %% lager:info("Wrote coverage file: ~p", [File]).
    ok.

generate_coverage_summary(Modules) ->
    {ModCovs, {TC, TN, TP}} = coverage_summary(Modules),
    IndexHTML = filename:join(["cover", "index.html"]),
    {ok, F} = file:open(IndexHTML, [write]),
    io:format(F, "<html><body><h1>Coverage Summary</h1>~n", []),
    io:format(F, "<style>"
              "  table, th, td {"
              "    border: black solid 1px;"
              "  }"
              "  td {"
              "    padding: 5px;"
              "    text-align: right;"
              "  }"
              "  td:first-child {"
              "    text-align: left;"
              "  }"
              "  .total {"
              "    font-weight: bold;"
              "  }"
              "</style>", []),
    io:format(F, "<table><tr>"
              "<th>Module</th>"
              "<th>Covered (%)</th>"
              "<th>Covered (Lines)</th>"
              "<th>Not covered (Lines)</th>"
              "</tr>~n", []),
    [io:format(F, "<tr><td>~s</td><td>~b%</td><td>~b</td><td>~b</td></tr>~n",
               [cover_link(Mod), P, C, N]) || {Mod, C, N, P} <- ModCovs],
    io:format(F, "<tr><td>~s</td><td>~b%</td><td>~b</td><td>~b</td></tr>~n",
              ["Total", TP, TC, TN]),
    io:format(F, "</table></body></html>", []),
    ok = file:close(F),
    lager:info("Wrote coverage summary: ~p", [IndexHTML]),
    ok.

coverage_summary(Modules) ->
    {ModCovs, {TC, TN}} =
        lists:mapfoldl(
          fun(Mod, {TC, TN}) ->
                  {ok, {Mod, {C, N}}} = cover:analyze(Mod, coverage, module),
                  ModCov = {Mod, C, N, percentage(C,C+N)},
                  {ModCov, {TC+C, TN+N}}
          end, {0,0}, Modules),
    {ModCovs, {TC, TN, percentage(TC, TC+TN)}}.

percentage(_, 0) ->
    0;
percentage(X, Y) ->
    X * 100 div Y.

cover_link(Module) ->
    lists:flatten(io_lib:format("<a href=\"~s\">~s</a>",
                                [cover_file(Module), Module])).

cover_file(Dir, Module) ->
    filename:join([Dir, cover_file(Module)]).
cover_file(Module) ->
    atom_to_list(Module) ++ ".COVER.html".
