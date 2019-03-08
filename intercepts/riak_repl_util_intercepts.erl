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

-module(riak_repl_util_intercepts).
-compile([export_all, nowarn_export_all]).
-include("intercept.hrl").

-define(M, riak_repl_util_orig).


%% intercept calls to riak_repl_util:start_fullsync_timer/3,
%% which is used for v3 repl
%% don't sleep, but see if the specified interval is correct
%% run fullsync after checking interval
interval_check_v3(Pid, FullsyncIvalMins, Cluster) ->
    io:format(user, "Scheduled fullsync from ~p ~p ~p~n",[Pid,
                                                          FullsyncIvalMins,
                                                          Cluster]),
    %% fs to B should always be 1 minute
    %% fs to C should always be 2 minutes
    %% the fs schedule test that doesn't specify
    %% a cluster uses 99
    case Cluster of
      "B" when  FullsyncIvalMins =/= 1
                andalso FullsyncIvalMins =/= 99
                -> throw("Invalid interval for cluster");
      "C" when  FullsyncIvalMins =/= 2
                andalso FullsyncIvalMins =/= 99
                -> throw("Invalid interval for cluster");
        _ -> gen_server:cast(Pid, start_fullsync)
    end.


%% intercept calls to riak_repl_util:schedule_fullsync,
%% which is used for v2 repl
%% don't sleep, but see if the interval in app:env is correct
%% the test that uses this intercept specifies a single
%% interval (99 minutes) for all sink clusters.
%% run fullsync after checking interval
interval_check_v2(Pid) ->
    {ok, Interval} = application:get_env(riak_repl, fullsync_interval),
    io:format(user, "Scheduled v2 fullsync in ~p minutes~n", [Interval]),
    case Interval of
        99 -> riak_repl_keylist_server:start_fullsync(Pid),
              ok;
        _ -> throw("Invalid interval specified for v2 replication")
    end.

