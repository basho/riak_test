%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------

-module(riak_repl_aae_source_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl_aae_source_orig).

%% @doc Introduce 10ms of latency in receiving message off of the
%%      socket.
delayed_get_reply(State) ->
    io:format("delayed~n"),
    ?I_INFO("delayed~n"),
    timer:sleep(10),
    ?M:get_reply_orig(State).

%% @doc Introduce 100ms of latency in receiving message off of the
%%      socket.
really_delayed_get_reply(State) ->
    io:format("really delayed~n"),
    ?I_INFO("really delayed~n"),
    timer:sleep(100),
    ?M:get_reply_orig(State).
