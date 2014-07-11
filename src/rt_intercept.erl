%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(rt_intercept).
-compile(export_all).
-define(DEFAULT_INTERCEPT(Target),
        list_to_atom(atom_to_list(Target) ++ "_intercepts")).

files_to_mods(Files) ->
    [list_to_atom(filename:basename(F, ".erl")) || F <- Files].

intercept_files() ->
    filelib:wildcard(filename:join([rt_local:home_dir(), "intercepts", "*.erl"])).

%% @doc Load the intercepts on the nodes under test.
-spec load_intercepts([node()]) -> ok.
load_intercepts(Nodes) ->
    case rt_config:get(load_intercepts, true) of
        false ->
            ok;
        true ->
            Intercepts = rt_config:get(intercepts, []),
            rt:pmap(fun(N) -> load_code(N) end, Nodes),
            rt:pmap(fun(N) -> add(N, Intercepts) end, Nodes),
            ok
    end.

load_code(Node) ->
    rt:wait_until_pingable(Node),
    [ok = remote_compile_and_load(Node, F) || F <- intercept_files()],
    ok.

add_and_save(Node, Intercepts) ->
    CodePaths = rpc:call(Node, code, get_path, []),
    [PatchesDir] = [P || P <- CodePaths, lists:suffix("basho-patches", P)],
    add(Node, Intercepts, PatchesDir).

add(Node, Intercepts) ->
    add(Node, Intercepts, undefined).

add(Node, Intercepts, OutDir) when is_list(Intercepts) ->
    [ok = add(Node, I, OutDir) || I <- Intercepts],
    ok;

add(Node, {Target, Mapping}, OutDir) ->
    add(Node, {Target, ?DEFAULT_INTERCEPT(Target), Mapping}, OutDir);

add(Node, {Target, Intercept, Mapping}, OutDir) ->
    NMapping = [transform_anon_fun(M) || M <- Mapping],
    ok = rpc:call(Node, intercept, add, [Target, Intercept, NMapping, OutDir]).

%% The following function transforms anonymous function mappings passed
%% from an Erlang shell. Anonymous intercept functions from compiled code
%% require the developer to supply free variables themselves, and also
%% requires use of the rt_intercept_pt parse transform.
transform_anon_fun({FunArity, Intercept}=Mapping) when is_function(Intercept) ->
    {env, Env} = erlang:fun_info(Intercept, env),
    case Env of
        [] ->
            error({badarg, Mapping});
        [FreeVars,_,_,Clauses] ->
            NewIntercept = {FreeVars, {'fun',1,{clauses,Clauses}}},
            {FunArity, NewIntercept}
    end;
transform_anon_fun(Mapping) ->
    Mapping.

remote_compile_and_load(Node, F) ->
    lager:debug("Compiling and loading file ~s on node ~s", [F, Node]),
    {ok, _, Bin} = rpc:call(Node, compile, file, [F, [binary]]),
    ModName = list_to_atom(filename:basename(F, ".erl")),
    {module, _} = rpc:call(Node, code, load_binary, [ModName, F, Bin]),
    ok.

wait_until_loaded(Node) ->
    wait_until_loaded(Node, 0).

wait_until_loaded(Node, 5) ->
    {failed_to_load_intercepts, Node};

wait_until_loaded(Node, Tries) ->
    case rt_config:get(load_intercepts, true) of
        false ->
            ok;
        true ->
            case are_intercepts_loaded(Node) of
                true ->
                    ok;
                false ->
                    timer:sleep(500),
                    wait_until_loaded(Node, Tries + 1)
            end
    end.

are_intercepts_loaded(Node) ->
    Results = [rpc:call(Node, code, is_loaded, [Mod])
               || Mod <- files_to_mods(intercept_files())],
    lists:all(fun is_loaded/1, Results).

is_loaded({file,_}) -> true;
is_loaded(_) -> false.
