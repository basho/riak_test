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
-module(ts_cluster_riak_shell_regression_log).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-define(DONT_INCREMENT_PROMPT, false).
-define(LOG_FILE, "priv/riak_shell/riak_shell_regression1.log").

%% we cant run the test in this process as it receives various messages
%% and the running test interprets then as being messages to the shell
confirm() ->
    Nodes = ts_setup:start_cluster(3),
    _Conn = ts_setup:conn(Nodes),
    lager:info("Built a cluster of ~p~n", [Nodes]),
    Self = self(),
    trace(Nodes),
    _Pid = spawn_link(fun() -> load_log_file(Self) end),
    Got1 = riak_shell_test_util:loop(),
    Result = ts_data:assert("Regression Log", pass, Got1),
    ts_data:results([
        Result
    ]),
    pass.

load_log_file(Pid) ->
    State = riak_shell_test_util:shell_init(),
    lager:info("~n~nLoad the log -------------------------", []),
    Cmds = [
            {{match, "No Regression Errors."},
              ts_data:flat_format("regression_log \"~s\";", [?LOG_FILE])}
           ],
    Result = riak_shell_test_util:run_commands(Cmds, State,
                                               ?DONT_INCREMENT_PROMPT),
    lager:info("~n~n------------------------------------------------------", []),
    Pid ! Result.

trace(Nodes) ->
    TraceStrings = [
                    "riak_kv_ts_svc:create_table -> return",
                    % "riak_kv_ts_util:create_table -> return",
                    % "riak_kv_wm_timeseries_query:create_table -> return",
                    % "riak_kv_ts_svc:check_table_and_call -> return",
                    % "riak_kv_ts_util:get_table_ddl -> return",
                    "riak_ql_parser:make_insert -> return",
                    "riak_core_bucket:get_bucket -> return",
                    "riak_core_bucket_type:get -> return",
                    "riak_core_metadata:get -> return",
                    "riak_kv_ts_svc:create_table -> stack",
                    % "riak_kv_ts_util:create_table -> stack",
                    % "riak_kv_wm_timeseries_query:create_table -> stack",
                    % "riak_kv_ts_svc:check_table_and_call -> stack",
                    % "riak_kv_ts_util:get_table_ddl -> stack",
                    "riak_ql_parser:make_insert -> stack",
                    "riak_core_bucket:get_bucket -> stack",
                    "riak_core_bucket_type:get -> stack",
                    "riak_core_metadata:get -> stack"
                   ],
    rt_redbug:trace(Nodes, TraceStrings).
