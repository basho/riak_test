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
%% HACK: split the regression log file in two to deal w/ activation slowness,
%% so a failed race on build boxes. The following considerations were made:
%% 1. Splitting the log can/should be done to make the log less monolithic.
%%    This will happen, but is not what the splitting is doing here. The
%%    splitting here is to put CREATE TABLE commands in one log and use of
%%    the created tables in another log to allow sufficient time for the
%%    tables to be activated.
%% 2. Adding a sleep (HACK) or wait_until (preferred)  command in riak_shell
%%    can/should be done, but not on the eve of a release. This is the
%%    prefered option since it allows for regression to impose a maximum
%%    allowable time for table activation.
%% 3. The fixed time to wait for table activation w/i CREATE TABLE can/should
%%    be changed to allow for a longer wait.
%% 4. When a table is created but activation has not completed, the response
%%    can/should be different. But, even w/ this, riak_shell doesn't have
%%    sufficient programming capability to perform a wait. Failing the test
%%    if activation didn't complete is not helpful.
-define(LOG_FILE_CREATE, "priv/riak_shell/riak_shell_regression1.log").
-define(LOG_FILE_USE,    "priv/riak_shell/riak_shell_regression2.log").

%% we cant run the test in this process as it receives various messages
%% and the running test interprets then as being messages to the shell
confirm() ->
    Nodes = ts_setup:start_cluster(3),
    _Conn = ts_setup:conn(Nodes),
    lager:info("Built a cluster of ~p~n", [Nodes]),
    Self = self(),
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
              ts_data:flat_format("regression_log \"~s\";", [?LOG_FILE_CREATE])},
            {{match, "No Regression Errors."},
              ts_data:flat_format("regression_log \"~s\";", [?LOG_FILE_USE])}
           ],
    Result = riak_shell_test_util:run_commands(Cmds, State,
                                               ?DONT_INCREMENT_PROMPT),
    lager:info("~n~n------------------------------------------------------", []),
    Pid ! Result.
