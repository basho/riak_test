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
    maybe_start/0,
    start/0,
    maybe_start_on_node/2,
    maybe_write_coverage/2,
    stop/0,
    maybe_stop_on_node/1,
    maybe_stop_on_nodes/0,
    stop_on_nodes/0,
    stop_on_node/1
    ]).

-define(COVER_SERVER, cover_server).

if_coverage(Fun) ->
    case rt_config:get(cover_enabled, true) of
        false ->
            cover_disabled;
        true ->
            Fun()
    end.

maybe_start() ->
    if_coverage(fun start/0).

mod_src(Mod) ->
    proplists:get_value(source, Mod:module_info(compile), Mod).

start() ->
    start(find_cover_modules()).

start([]) ->
    lager:info("Skipping cover, no modules included"),
    rt_config:set(cover_enabled, false),
    ok;
start(CoverMods) ->
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
                    end || Mod <- CoverMods],
            SrcDict = dict:from_list([ModSrc || {ok, ModSrc} <- CMods]),
            rt_config:set(cover_mod_src, SrcDict),
            ok;
        _ ->
            lager:error("Could not start cover server: ~p", [Res]),
            rt_config:set(cover_enabled, false),
            Res
    end.

%% @doc Figure out which modules to include.
%% You can use a list of specific modules in the application
%% variable `cover_modules', or application names in `cover_apps',
%% which may be the special token `all'.
find_cover_modules() ->
    case rt_config:get(cover_modules, []) of
        [] ->
            Apps = rt_config:get(cover_apps, []),
            AppMods = find_app_modules(Apps),
            rt_config:set(cover_modules, AppMods),
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
        [_|_] -> "{" ++ string:join(to_strs(CoverApps),",") ++ "}-*"
    end,
    Pattern = filename:join([Deps, AppPattern, "ebin", "*.beam"]),
    lager:debug("Looking for beams to cover in ~s", [Pattern]),
    File2Mod = fun(F) ->
            list_to_atom(filename:rootname(filename:basename(F)))
    end,
    Excluded = [riak_core_pb],
    ModApps = [{File2Mod(File), app_name(File)}
                || File <- filelib:wildcard(Pattern),
                   not lists:member(File2Mod(File), Excluded)],
    rt_config:set(cover_mod_apps, dict:from_list(ModApps)),
    [Mod || {Mod, _} <- ModApps].

%% @doc Starts cover on the node if enabled and the node is using the current
%% version of Riak. We can not mix versions of modules for coverage analysis,
%% so only current will do.
maybe_start_on_node(Node, Version) ->
    IsCurrent = case Version of
        current      -> true;
        {current, _} -> true;
        _            -> false
    end,
    ShouldStart = IsCurrent andalso
                  rt_config:get(cover_enabled, true) andalso
                  erlang:whereis(?COVER_SERVER) /= undefined,
    case ShouldStart of
        false ->
            ok;
        true ->
            lager:debug("Starting cover on node ~p", [Node]),
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
cov_order({Mod1, {C1, NC1}}, {Mod2, {C2, NC2}}) ->
    {perc({C1,NC1}), C1+NC1, Mod1} >
    {perc({C2,NC2}), C2+NC2, Mod2}.

sort_cov(CovList) ->
    lists:sort(fun cov_order/2, CovList).

acc_cov(CovList) ->
    AddCov = fun({_M, {Y, N}}, {TY, TN}) -> {TY+Y, TN+N} end,
    lists:foldl(AddCov, {0, 0}, CovList).

group_by_app(ModCovList, Mod2App) ->
    D1 = lists:foldl(fun(ModCov = {Mod, _}, Acc) ->
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

write_coverage(all, Dir) ->
    write_coverage(rt_config:get(cover_modules, []), Dir);
write_coverage(CoverModules, CoverDir) ->
    % First write a file per module
    prepare_output_dir(CoverDir),
    ModCovList0 = rt:pmap(fun(Mod) ->
                    write_module_coverage(Mod, CoverDir),
                    {ok, ModCov} = cover:analyse(Mod, coverage, module),
                    ModCov
            end, CoverModules),

    % Create data struct with total, per app and per module coverage
    NotEmpty = fun({_, {C, NC}}) -> C > 0 orelse NC > 0 end,
    ModCovList = lists:filter(NotEmpty, ModCovList0),
    Mod2App = rt_config:get(cover_mod_apps, dict:new()),
    AppCovList = group_by_app(ModCovList, Mod2App),
    TotalPerc = perc(acc_cov(ModCovList)),
    TotalCov = {TotalPerc, AppCovList},

    % Now write main file with links to module files.
    IdxFile = filename:join([CoverDir, "index.html"]),
    write_index_file(TotalCov, IdxFile),

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
                            ["<li>",
                             "<span class='perc'> ", perc_str(Cov), " </span>",
                             "<a href='", Name, ".COVER.html'>", Name, "</a>",
                             "</li>"]
                        end || {Mod, Cov} <- ModCov],
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
            % Cover only finds source if alongside beam or in ../src from beam
            % so had to temporarily copy source next to each beam.
            {file, BeamFile} = cover:is_compiled(CoverMod),
            TmpSrc = filename:rootname(BeamFile) ++ ".erl",
            file:copy(Src, TmpSrc),
            {ok, _Mod} = cover:analyse_to_file(CoverMod, CoverFile, [html]),
            file:delete(TmpSrc),
            ok
    end.

stop() ->
    lager:info("Stopping cover"),
    cover:stop().
