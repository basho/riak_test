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

%% @doc Code to interact with cover, Erlang's code coverage tool.
%% Cover needs to be started in the main node once and on each remote node
%% every time it is started. It should be stopped on remote nodes before
%% stopping them to make sure they have flushed all coverage data.
%% In the end, we want to write coverage analysis to the file system.
%% Here we do it as html files for easy browsing.
-module(rt_cover).
-export([
    maybe_start/1,
    start/1,
    maybe_start_on_node/2,
    maybe_write_coverage/2,
    maybe_export_coverage/3,
    maybe_import_coverage/1,
    stop/0,
    maybe_reset/0,
    maybe_stop_on_node/1,
    maybe_stop_on_nodes/0,
    stop_on_nodes/0,
    stop_on_node/1
    ]).

-define(COVER_SERVER, cover_server).

-record(cover_info, {module :: atom(),
                     coverage :: undefined | {integer(), integer()},
                     output_file :: undefined | string()}).

if_coverage(Fun) ->
    case rt_config:get(cover_enabled, false) of
        false ->
            cover_disabled;
        true ->
            Fun()
    end.

maybe_start(Test) ->
    if_coverage(fun() -> start(Test) end).

mod_src(Mod) ->
    try Mod:module_info(compile) of
        CompileInfo ->
            Src = proplists:get_value(source, CompileInfo, Mod),
            case filelib:is_regular(Src) of
                true -> Src;
                false -> undefined
            end
    catch
        _:_ -> undefined
    end.

has_src(Mod) ->
    case mod_src(Mod) of
        undefined -> false;
        _ -> true
    end.

start(Test) ->
    start2(find_cover_modules(Test)).

start2([]) ->
    lager:info("Skipping cover, no modules included"),
    ok;
start2(CoverMods) ->
    lager:info("Starting cover"),
    stop_on_nodes(),
    ok = cover:stop(),
    Res = case cover:start() of
        {ok, _Pid} ->
            ok;
        Err ->
            Err
    end,
    case Res of
        ok ->
            lager:info("Cover compiling ~p modules", [length(CoverMods)]),
            CMods = [begin
                        Src = mod_src(Mod),
                        case cover:compile_beam(Mod) of
                            {ok, _} ->
                                {ok, {Mod, Src}};
                            {error, CErr} ->
                                lager:warning("Error cover compiling ~p : ~p",
                                              [Mod, CErr]),
                                {error, CErr}
                        end
                    end || Mod <- CoverMods, has_src(Mod)],
            SrcDict = dict:from_list([ModSrc || {ok, ModSrc} <- CMods]),
            rt_config:set(cover_mod_src, SrcDict),
            ok;
        _ ->
            lager:error("Could not start cover server: ~p", [Res]),
            rt_config:set(cover_enabled, false),
            Res
    end.

%% @doc Figure out which modules to include.
%% These are read, per test, from the test module attributes
%% `cover_modules' or `cover_apps'.
find_cover_modules(Test) ->
    {Mod, _Fun} = riak_test_runner:function_name(confirm, Test),
    case proplists:get_value(cover_modules, Mod:module_info(attributes), []) of
        [] ->
            case proplists:get_value(cover_apps, Mod:module_info(attributes), []) of
                [] ->
                    %% fallback to what is in the config file
                    read_cover_modules_from_config();
                Apps ->
                    AppMods = find_app_modules(Apps),
                    AppMods
            end;
        ConfMods ->
            ConfMods
    end.

read_cover_modules_from_config() ->
    case rt_config:get(cover_modules, []) of
        [] ->
            Apps = rt_config:get(cover_apps, []),
            AppMods = find_app_modules(Apps),
            AppMods;
        ConfMods ->
            ConfMods
    end.

to_str(E) when is_atom(E) ->
    atom_to_list(E);
to_str(E) when is_list(E) ->
    E.

to_strs(List) ->
    [to_str(E) || E <- List].

app_name(BeamPath) ->
    filename:basename(filename:dirname(filename:dirname(BeamPath))).

find_app_modules(CoverApps) ->
    Deps = rt:get_deps(),
    AppPattern = case CoverApps of
        all   -> "*";
        []    -> "SILLY_PATTERN_THAT_WILL_NOT_MATCH_ANYTHING";
        [_|_] -> "{" ++ string:join(to_strs(CoverApps),",") ++ "}*"
    end,
    Pattern = filename:join([Deps, AppPattern, "ebin", "*.beam"]),
    lager:debug("Looking for beams to cover in ~s", [Pattern]),
    File2Mod = fun(F) ->
            list_to_atom(filename:rootname(filename:basename(F)))
    end,
    ModApps = [{File2Mod(File), app_name(File)}
                || File <- filelib:wildcard(Pattern)],
    rt_config:set(cover_mod_apps, dict:from_list(ModApps)),
    [Mod || {Mod, _} <- ModApps].

%% @doc Starts cover on the node if enabled and the node is using the current
%% version of Riak. We can not mix versions of modules for coverage analysis,
%% so only current will do.
maybe_start_on_node(Node, Version) ->
    IsCurrent = case Version of
        head      -> true;
        {head, _} -> true;
        _            -> false
    end,
    ShouldStart = IsCurrent andalso
                  cover:modules() /= [] andalso
                  erlang:whereis(?COVER_SERVER) /= undefined,
    case ShouldStart of
        false ->
            ok;
        true ->
            lager:debug("Starting cover on node ~p", [Node]),
            rt:wait_until_pingable(Node),
            {ok, _Node} = cover:start(Node),
            ok
    end.

maybe_stop_on_node(Node) ->
    if_coverage(fun() -> stop_on_node(Node) end).

stop_on_node(Node) ->
    stop_on_nodes([Node]).

maybe_stop_on_nodes() ->
    if_coverage(fun() -> stop_on_nodes() end).

stop_on_nodes() ->
    stop_on_nodes(cover:which_nodes()).

stop_on_nodes(Nodes) ->
    [begin
            lager:info("Stopping cover on node ~p", [Node]),
            cover:stop(Node)
        end
     || Node <- Nodes].

%% @doc If enabled, write coverage analysis as HTML files.
%% The entry point is index.html, which contains total coverage information
%% and links to detailed coverage for each module included in the analysis.
maybe_write_coverage(CoverMods, Dir) ->
    if_coverage(fun() -> write_coverage(CoverMods, Dir) end).

maybe_export_coverage(TestModule, Dir, Phash) ->
    if_coverage(fun() ->
                        prepare_output_dir(Dir),
                        Filename = filename:join(Dir,
                                                 atom_to_list(TestModule)
                                                 ++ "-" ++ integer_to_list(Phash)
                                                 ++ ".coverdata"),
                        ok = cover:export(Filename),
                        Filename
                end).

maybe_import_coverage(cover_disabled) ->
    ok;
maybe_import_coverage(File) ->
    if_coverage(fun() -> cover:import(File) end).

prepare_output_dir(Dir) ->
    %% NOTE: This is not a recursive make dir, only top level will be created.
    case file:make_dir(Dir) of
        ok ->
            Dir;
        {error, eexist} ->
            Pattern = filename:join([Dir, "*.html"]),
            Dels = [{file:delete(File),File}
                    || File <- filelib:wildcard(Pattern)],
            [lager:warning("Could not delete file ~p : ~p", [File, DErr])
             || {{error, DErr}, File} <- Dels],
            Dir;
        _ ->
            lager:warning("Could not create directory ~p, " ++
                          "putting coverage output in current directory"),
            "."
    end.

perc({0, 0}) ->
   0.0;
perc({Cov, NotCov}) ->
    100.0 * Cov / (Cov + NotCov).

perc_str({C, NC}) when C =< 0, NC =< 0 ->
    "N/A";
perc_str({_, _} = CovInfo) ->
    perc_str(perc(CovInfo));
perc_str(CovPerc) ->
    io_lib:format("~.1f%", [CovPerc]).

%% @doc Sort first by coverage, # of lines, module name
cov_order(#cover_info{module=Mod1, coverage={C1, NC1}},
          #cover_info{module=Mod2, coverage={C2, NC2}}) ->
    {perc({C1,NC1}), C1+NC1, Mod1} >
    {perc({C2,NC2}), C2+NC2, Mod2}.

sort_cov(CovList) ->
    lists:sort(fun cov_order/2, CovList).

-spec acc_cov([#cover_info{}]) -> {number(), number()}.
acc_cov(CovList) when is_list(CovList) ->
    AddCov = fun(#cover_info{coverage={Y, N}}, {TY, TN}) -> {TY+Y, TN+N};
        (#cover_info{coverage=undefined}, {TY, TN}) -> {TY, TN}
    end,
    lists:foldl(AddCov, {0, 0}, CovList).

-spec group_by_app(ModCovList:: [#cover_info{}], Mod2App :: dict()) ->
    [{string(), number(), [#cover_info{}]}].
group_by_app(ModCovList, Mod2App) ->
    D1 = lists:foldl(fun(ModCov = #cover_info{module=Mod}, Acc) ->
                    case dict:find(Mod, Mod2App) of
                        {ok, App} ->
                            dict:append(App, ModCov, Acc);
                        error ->
                            dict:append("Unknown", ModCov, Acc)
                    end
            end, dict:new(), ModCovList),
    L1 = dict:to_list(D1),
    [{App, perc(acc_cov(AppModCovList)), sort_cov(AppModCovList)}
     || {App, AppModCovList} <- L1].

%% Predicate to filter out modules with no executable lines
%% (usually all test code ifdef'ed out).
-spec not_empty_file(#cover_info{}) -> boolean().
not_empty_file(#cover_info{coverage = {C, NC}})
        when C > 0; NC > 0 ->
    true;
not_empty_file(_) ->
    false.

-spec process_module(Mod :: atom(), OutDir :: string()) -> #cover_info{}.
process_module(Mod, OutDir) ->
    OutFile = case write_module_coverage(Mod, OutDir) of
        {ok, OutFile0} -> OutFile0;
        _              -> undefined
    end,
    Coverage = case cover:analyse(Mod, coverage, module) of
        {ok, {Mod, Coverage0}} ->
            Coverage0;
        {error, Err} ->
            lager:error("Could not cover analyze module ~p : ~p", [Mod, Err]),
            undefined
    end,
    #cover_info{module=Mod, output_file=OutFile, coverage=Coverage}.

write_coverage(all, Dir) ->
    write_coverage(cover:imported_modules(), Dir);
write_coverage(CoverModules, CoverDir) ->
    lager:info("analyzing modules ~p", [CoverModules]),
    % temporarily reassign the group leader, to suppress annoying io:format output
    {group_leader, GL} = erlang:process_info(whereis(cover_server), group_leader),
    %% tiny recursive fun that pretends to be a group leader$
    F = fun() ->
            YComb = fun(Fun) ->
                    receive
                        {io_request, From, ReplyAs, {put_chars, _Enc, _Msg}} ->
                            From ! {io_reply, ReplyAs, ok},
                            Fun(Fun);
                        {io_request, From, ReplyAs, {put_chars, _Enc, _Mod, _Func, _Args}} ->
                            From ! {io_reply, ReplyAs, ok},
                            Fun(Fun);
                        _Other ->
                            io:format(user, "Other Msg ~p", [_Other]),
                            Fun(Fun)
                    end
            end,
            YComb(YComb)
    end,
    Pid = spawn(F),
    erlang:group_leader(Pid, whereis(cover_server)),
    % First write a file per module
    prepare_output_dir(CoverDir),
    ModCovList0 = rt:pmap(fun(Mod) -> process_module(Mod, CoverDir) end,
                          CoverModules),
    % Create data struct with total, per app and per module coverage
    ModCovList = lists:filter(fun not_empty_file/1, ModCovList0),
    Mod2App = rt_config:get(cover_mod_apps, dict:new()),
    AppCovList = group_by_app(ModCovList, Mod2App),
    TotalPerc = perc(acc_cov(ModCovList)),
    TotalCov = {TotalPerc, AppCovList},

    % Now write main file with links to module files.
    IdxFile = filename:join([CoverDir, "index.html"]),
    write_index_file(TotalCov, IdxFile),
    erlang:group_leader(GL, whereis(cover_server)),
    exit(Pid, kill),
    TotalCov.

write_index_file({TotalPerc, AppCovList}, File) ->
    Contents = [
            "<html>",
            "<head>",
            "<title>Riak Test Coverage</title>",
            "</head>",
            "<body><div id='body'>",
            "<h2>Total coverage : ", perc_str(TotalPerc), "</h2>",
            case AppCovList of
                [] -> "<h2>No per app coverage</h2>";
                _ -> ""
            end,
            [
                [
                    "<h3>",
                    App,
                    " : ", perc_str(AppCov),
                    "</h3><ul>",
                    [begin
                            Name = atom_to_list(Mod),
                            FileLink = case OutFile of
                                undefined -> Name;
                                _ -> ["<a href='", Name, ".COVER.html'>", Name, "</a>"]
                            end,
                            ["<li>",
                             "<span class='perc'> ", perc_str(Cov), " </span>",
                             FileLink,
                             "</li>"]
                        end || #cover_info{module=Mod,
                                    coverage=Cov,
                                    output_file=OutFile} <- ModCov],
                    "</ul>"]
                || {App, AppCov, ModCov} <- AppCovList],
            "</body></html>"
            ],
    file:write_file(File, Contents).

write_module_coverage(CoverMod, CoverDir) ->
    CoverFile = filename:join([CoverDir,
                               atom_to_list(CoverMod)++".COVER.html"]),
    lager:debug("Writing cover information for module ~p in ~s",
                [CoverMod, CoverFile]),
    ModSrc = rt_config:get(cover_mod_src, dict:new()),
    case dict:find(CoverMod, ModSrc) of
        error ->
            lager:warning("Can't write line coverage for module ~p, no source",
                          [CoverMod]),
            {error, no_source};
        {ok, Src} ->
            case filelib:is_regular(Src) andalso filename:extension(Src) == ".erl" of
                true ->
                    case cover:analyse_to_file(CoverMod, CoverFile, [html]) of
                        {ok, _Mod} -> ok;
                        {error, Err} ->
                            lager:warning("Failed to write coverage analysis" ++
                                          " for module ~p (source ~s): ~p",
                                          [CoverMod, Src, Err])
                    end,
                    {ok, CoverFile};
                false ->
                    lager:warning("Source for module ~p is not an erl file : ~s",
                                 [CoverMod, Src]),
                    {error, no_source}
            end
    end.

stop() ->
    lager:info("Stopping cover"),
    cover:stop().

maybe_reset() ->
    if_coverage(fun() -> cover:reset() end).
