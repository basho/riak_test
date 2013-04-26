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
-module(rt_config).
-include_lib("eunit/include/eunit.hrl").

-export([
         get/1,
         get/2,
         config_or_os_env/1,
         config_or_os_env/2,
         get_os_env/1,
         get_os_env/2,
         load/1,
         set/2
]).

%% @doc Get the value of an OS Environment variable. The arity 1 version of
%%      this function will fail the test if it is undefined.
get_os_env(Var) ->
    case get_os_env(Var, undefined) of
        undefined ->
            lager:error("ENV['~s'] is not defined", [Var]),
            ?assert(false);
        Value -> Value
    end.

%% @doc Get the value of an OS Evironment variable. The arity 2 version of
%%      this function will return the Default if the OS var is undefined.
get_os_env(Var, Default) ->
    case os:getenv(Var) of
        false -> Default;
        Value -> Value
    end.

%% @private
load(undefined) ->
    load_dot_config("default");
load(ConfigName) ->
    load_dot_config(ConfigName).

%% @private
load_dot_config(ConfigName) ->
    case file:consult(filename:join([os:getenv("HOME"), ".riak_test.config"])) of
        {ok, Terms} ->
            %% First, set up the defaults
            case proplists:get_value(default, Terms) of
                undefined -> meh; %% No defaults set, move on.
                Default -> [set(Key, Value) || {Key, Value} <- Default]
            end,
            %% Now, overlay the specific project
            Config = proplists:get_value(list_to_atom(ConfigName), Terms),
            [set(Key, Value) || {Key, Value} <- Config],
            ok;
        {error, Reason} ->
            erlang:error("Failed to parse config file", ["~/.riak_test.config", Reason])
 end.

%% @private
set(Key, Value) ->
    ok = application:set_env(riak_test, Key, Value).

get(Key) ->
    case kvc:path(Key, application:get_all_env(riak_test)) of
        [] -> erlang:error("Missing configuration key", [Key]);
        Value -> Value
    end.

get(Key, Default) ->
    case kvc:path(Key, application:get_all_env(riak_test)) of
        [] -> Default;
        Value -> Value
    end.

-spec config_or_os_env(atom()) -> term().
config_or_os_env(Config) ->
    OSEnvVar = to_upper(atom_to_list(Config)),
    case {get_os_env(OSEnvVar, undefined), get(Config, undefined)} of
        {undefined, undefined} ->
            MSG = io_lib:format("Neither riak_test.~p nor ENV['~p'] are defined", [Config, OSEnvVar]),
            erlang:error(binary_to_list(iolist_to_binary(MSG)));
        {undefined, V} ->
            lager:info("Found riak_test.~s: ~s", [Config, V]),
            V;
        {V, _} ->
            lager:info("Found ENV[~s]: ~s", [OSEnvVar, V]),
            set(Config, V),
            V
    end.

-spec config_or_os_env(atom(), term()) -> term().
config_or_os_env(Config, Default) ->
    OSEnvVar = to_upper(atom_to_list(Config)),
    case {get_os_env(OSEnvVar, undefined), get(Config, undefined)} of
        {undefined, undefined} -> Default;
        {undefined, V} ->
            lager:info("Found riak_test.~s: ~s", [Config, V]),
            V;
        {V, _} ->
            lager:info("Found ENV[~s]: ~s", [OSEnvVar, V]),
            set(Config, V),
            V
    end.

to_upper(S) -> lists:map(fun char_to_upper/1, S).
char_to_upper(C) when C >= $a, C =< $z -> C bxor $\s;
char_to_upper(C) -> C.