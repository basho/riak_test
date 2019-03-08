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
-module(riak_repl2_rtsource_helper_intercepts).
-compile([export_all, nowarn_export_all]).
-include("intercept.hrl").

%% @doc Forward the heartbeat messages from the rt source by
%%      calling the original function.
forward_send_heartbeat(Pid) ->
    %% ?I_INFO("forward_heartbeat"),
    riak_repl2_rtsource_helper_orig:send_heartbeat_orig(Pid).

%% @doc Drop the heartbeat messages from the rt source.
drop_send_heartbeat(_Pid) ->
    %% ?I_INFO("drop_heartbeat"),
    ok.
