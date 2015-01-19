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
-module(rt_backend).
-include("rt.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-export([set/2]).

%%%===================================================================
%%% Test harness setup, configuration, and internal utilities
%%%===================================================================

replace_backend(Backend, false) ->
    [{storage_backend, Backend}];
replace_backend(Backend, {riak_kv, KVSection}) ->
    lists:keystore(storage_backend, 1, KVSection, {storage_backend, Backend}).

%% TODO: Probably should abstract this into the rt_config module and
%% make a sensible API to hide the ugliness of dealing with the lists
%% module funs.
%%
%% TODO: Add support for Riak CS backend and arbitrary multi backend
%% configurations
set(eleveldb, Config) ->
    UpdKVSection = replace_backend(riak_kv_eleveldb_backend,
                                   lists:keyfind(riak_kv, 1, Config)),
    lists:keystore(riak_kv, 1, Config, {riak_kv, UpdKVSection});
set(memory, Config) ->
    UpdKVSection = replace_backend(riak_kv_eleveldb_backend,
                                   lists:keyfind(riak_kv, 1, Config)),
    lists:keystore(riak_kv, 1, Config, {riak_kv, UpdKVSection});
set(multi, Config) ->
    UpdKVSection =
        replace_backend(riak_kv_multi_backend,
                        lists:keyfind(riak_kv, 1, Config)) ++
        multi_backend_config(default),
    lists:keystore(riak_kv, 1, Config, {riak_kv, UpdKVSection});
set({multi, indexmix}, Config) ->
    UpdKVSection =
        replace_backend(riak_kv_multi_backend,
                        lists:keyfind(riak_kv, 1, Config)) ++
        multi_backend_config(indexmix),
    lists:keystore(riak_kv, 1, Config, {riak_kv, UpdKVSection});
set(_, Config) ->
    UpdKVSection = replace_backend(riak_kv_bitcask_backend,
                                   lists:keyfind(riak_kv, 1, Config)),
    lists:keystore(riak_kv, 1, Config, {riak_kv, UpdKVSection}).

multi_backend_config(default) ->
    [{multi_backend_default, <<"eleveldb1">>},
     {multi_backend, [{<<"eleveldb1">>, riak_kv_eleveldb_backend, []},
                      {<<"memory1">>, riak_kv_memory_backend, []},
                      {<<"bitcask1">>, riak_kv_bitcask_backend, []}]}];
multi_backend_config(indexmix) ->
    [{multi_backend_default, <<"eleveldb1">>},
     {multi_backend, [{<<"eleveldb1">>, riak_kv_eleveldb_backend, []},
                      {<<"memory1">>, riak_kv_memory_backend, []}]}].

%% @doc Sets the backend of ALL nodes that could be available to riak_test.
%%      this is not limited to the nodes under test, but any node that
%%      riak_test is able to find. It then queries each available node
%%      for it's backend, and returns it if they're all equal. If different
%%      nodes have different backends, it returns a list of backends.
%%      Currently, there is no way to request multiple backends, so the
%%      list return type should be considered an error.
-spec set_backend(atom()) -> atom()|[atom()].
set_backend(Backend) ->
    set_backend(Backend, []).

-spec set_backend(atom(), [{atom(), term()}]) -> atom()|[atom()].
set_backend(bitcask, _) ->
    set_backend(riak_kv_bitcask_backend);
set_backend(eleveldb, _) ->
    set_backend(riak_kv_eleveldb_backend);
set_backend(memory, _) ->
    set_backend(riak_kv_memory_backend);
set_backend(multi, Extras) ->
    set_backend(riak_kv_multi_backend, Extras);
set_backend(Backend, _) when Backend == riak_kv_bitcask_backend; Backend == riak_kv_eleveldb_backend; Backend == riak_kv_memory_backend ->
    lager:info("rt_backend:set_backend(~p)", [Backend]),
    rt_config:update_app_config(all, [{riak_kv, [{storage_backend, Backend}]}]),
    get_backends();
set_backend(Backend, Extras) when Backend == riak_kv_multi_backend ->
    MultiConfig = proplists:get_value(multi_config, Extras, default),
    Config = make_multi_backend_config(MultiConfig),
    rt_config:update_app_config(all, [{riak_kv, Config}]),
    get_backends();
set_backend(Other, _) ->
    lager:warning("rt_backend:set_backend doesn't recognize ~p as a legit backend, using the default.", [Other]),
    get_backends().

make_multi_backend_config(default) ->
    [{storage_backend, riak_kv_multi_backend},
     {multi_backend_default, <<"eleveldb1">>},
     {multi_backend, [{<<"eleveldb1">>, riak_kv_eleveldb_backend, []},
                      {<<"memory1">>, riak_kv_memory_backend, []},
                      {<<"bitcask1">>, riak_kv_bitcask_backend, []}]}];
make_multi_backend_config(indexmix) ->
    [{storage_backend, riak_kv_multi_backend},
     {multi_backend_default, <<"eleveldb1">>},
     {multi_backend, [{<<"eleveldb1">>, riak_kv_eleveldb_backend, []},
                      {<<"memory1">>, riak_kv_memory_backend, []}]}];
make_multi_backend_config(Other) ->
    lager:warning("rt:set_multi_backend doesn't recognize ~p as legit multi-backend config, using default", [Other]),
    make_multi_backend_config(default).

get_backends() ->
    Backends = rt_harness:get_backends(),
    case Backends of
        [riak_kv_bitcask_backend] -> bitcask;
        [riak_kv_eleveldb_backend] -> eleveldb;
        [riak_kv_memory_backend] -> memory;
        [Other] -> Other;
        MoreThanOne -> MoreThanOne
    end.

-spec get_backend([proplists:property()]) -> atom() | error.
get_backend(AppConfigProplist) ->
    case kvc:path('riak_kv.storage_backend', AppConfigProplist) of
        [] -> error;
        Backend -> Backend
    end.
