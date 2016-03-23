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

%% Intercepts functions for the riak_test in ../tests/repl_rt_heartbeat.erl
-module(riak_repl2_rtq_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl2_rtq_orig).

%% @doc Drop the heartbeat messages from the rt source.
slow_trim_q(State) ->
    %% ?I_INFO("slow_trim_q"),

    %% This hideousness is necessary in order to have this intercept sleep only 
    %% on the first iteration. With hope, it causes the message queue of the
    %% RTQ to spike enough to initiate overload handling, then subsequently
    %% allows the queue to drain, overload to flip off, and the writes to complete.
    case get(hosed) of 
    	undefined ->
    	    put(hosed, true);
    	true ->
            timer:sleep(5000),
            put(hosed, false);
        false ->
            put(hosed, false)
    end,

    ?M:trim_q_orig(State).
