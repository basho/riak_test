%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
-module(rt_load_client).

-export([
         load/1
        ]).

load(Version) ->
    LoadPaths = get_load_paths(Version),
    Modules = get_modules(LoadPaths, []),
    ok = unload_riak_client(Modules),
    ok = load_riak_client(Modules),
    ok.

load_riak_client(Modules) ->
    [{module, Mod} = code:load_abs(File) || {File, Mod} <- Modules],
    ok = application:ensure_all_started(riakc),
    ok.

get_modules([], Acc) ->
    lists:flatten(Acc);
get_modules([H | T], Acc) ->
    Files = filelib:wildcard(filename:join([H, "*.beam"])),
    Files2 = [filename:rootname(File) || File <- Files],
    Mods = extract_mods(Files),
    Zip = lists:zip(Files2, Mods),
    get_modules(T, [Zip | Acc]).

extract_mods(Mods) ->
    [list_to_atom(filename:rootname(filename:basename(Mod))) || Mod <- Mods].

get_load_paths(Version) ->
    Root = rtdev:relpath(Version),
    [
     filename:join([Root, "dev/dev1/lib/riakc*/ebin"]),
     filename:join([Root, "dev/dev1/lib/riak_pb*/ebin"])
    ].

%% we have a problem: the set of files in Version X of the client
%% might not be congruent with that of Version Y
%% so we will hard unload files from the app manifest
%% and then iterate over the beam files in the path and unload any
%% modules with those names - all a bit brute-force-and-ignorance
unload_riak_client(Modules) ->
    %% app first
    ok = application:stop(riak_pb),
    ok = application:stop(riakc),
    [ok = unload(App) || App <- [riakc, riak_pb]],
    [begin code:purge(Mod), code:delete(Mod) end || {_File, Mod} <- Modules],
    ok.

unload(App) ->
    _ = application:load(App),
    case application:get_key(App, modules) of
        {ok, Modules} ->
            [begin code:purge(Mod), code:delete(Mod) end || Mod <- Modules],
            ok;
        _Other ->
            ok
    end.
