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

-module(ts_cluster_handoff_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TS_VERSION_CURRENT, current).

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    % ct:pal("VERSION 1.4 ~p", [rt:find_version_by_name(["riak_ts-1.4.0", "riak_ts_ee-1.4.0"])]),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(use_ttb_true, Config) ->
    [{use_ttb, true} | Config];
init_per_group(use_ttb_false, Config) ->
    [{use_ttb, false} | Config].

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% tear down the whole cluster before every test
    rtdev:setup_harness('_', '_'),
    ct:pal("TEST CASE ~p", [_TestCase]),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [
     {use_ttb_true, [sequence], rt:grep_test_functions(?MODULE)}
     % ,{use_ttb_false, [sequence], rt:grep_test_functions(?MODULE)}
    ].

all() -> 
    [
     {group, use_ttb_true}
     % ,{group, use_ttb_false}
    ].

%%--------------------------------------------------------------------
%% UTILS
%%--------------------------------------------------------------------

run_query(Pid, Query, Config) when is_pid(Pid) ->
    UseTTB = proplists:get_value(use_ttb, Config),
    riakc_ts:query(Pid, Query, [{use_ttb, UseTTB}]).

%%--------------------------------------------------------------------
%% TESTS
%%--------------------------------------------------------------------

basic_table_hinted_handoff_test(Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    {ok, _} =
        riakc_ts:query(rt:pbc(Node_A),
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "   
            "b TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,quantum(b,1,s)), a,b))"
        ),
    ok = rt:stop(Node_A),
    ok = rt:wait_until_no_pending_changes([Node_B,Node_C]),
    ok = riakc_ts:put(rt:pbc(Node_B), "mytab",
        [{1,B} || B <- lists:seq(1000,5000,1000)]),
    ok = rt:start(Node_A),
    ok = rt:wait_until_no_pending_changes([Node_A,Node_B,Node_C]),
    Query =
        "SELECT * FROM mytab "
        "WHERE a = 1 AND b >= 1000 AND b <= 5000",
    ExpectedResultSet = [{1,B} || B <- lists:seq(1000,5000,1000)],
    ts_data:assert_row_sets(
        {rt_ignore_columns, ExpectedResultSet},
        run_query(rt:pbc(Node_B), Query, Config)
    ),
    ok.

additional_columns_on_local_key_table_hinted_handoff_test(Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    {ok, _} =
        riakc_ts:query(rt:pbc(Node_A),
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "   
            "b TIMESTAMP NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,quantum(b,1,s)), a,b,c))"
        ),
    ok = rt:stop(Node_A),
    ok = rt:wait_until_no_pending_changes([Node_B,Node_C]),
    ok = riakc_ts:put(rt:pbc(Node_B), "mytab",
        [{1,B,C} || B <- lists:seq(1000,5000,1000), C <- lists:seq(1000,5000,1000)]),
    ok = rt:start(Node_A),
    ok = rt:wait_until_no_pending_changes([Node_A,Node_B,Node_C]),
    Query =
        "SELECT * FROM mytab "
        "WHERE a = 1 AND b >= 1000 AND b <= 5000",
    ExpectedResultSet = [{1,B,C} || B <- lists:seq(1000,5000,1000), C <- lists:seq(1000,5000,1000)],
    ts_data:assert_row_sets(
        {rt_ignore_columns, ExpectedResultSet},
        run_query(rt:pbc(Node_B), Query, Config)
    ),
    ok.





