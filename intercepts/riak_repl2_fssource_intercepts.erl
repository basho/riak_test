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

-module(riak_repl2_fssource_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl2_fssource_orig).

slow_handle_info(Msg, State) ->
    io:format("slow_handle_info~n"),
    ?I_INFO("slow_handle_info~n"),
    timer:sleep(10),
    ?M:handle_info_orig(Msg, State).

really_slow_handle_info(Msg, State) ->
    io:format("really_slow_handle_info~n"),
    ?I_INFO("really_slow_handle_info~n"),
    timer:sleep(100),
    ?M:handle_info_orig(Msg, State).
