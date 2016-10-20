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

-module(ts_cluster_ddl_upgrades_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

-define(TS_VERSION_CURRENT, current).
    
%% git clone git@github.com:basho/riak.git && git checkout riak_ts-1.4.0 && make locked-deps
-define(TS_VERSION_1_4, riak_ts_1_4).

-define(DDL_REC_CAP, {riak_kv, riak_ql_ddl_rec_version}).

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
     ,{use_ttb_false, [sequence], rt:grep_test_functions(?MODULE)}
    ].

all() -> 
    [
     {group, use_ttb_true}
     ,{group, use_ttb_false}
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

all_nodes_upgrades_to_1_5_test(_Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_1_4, ?TS_VERSION_1_4, ?TS_VERSION_1_4]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ok = rt:upgrade(Node_A, ?TS_VERSION_CURRENT),
        ok = rt:upgrade(Node_B, ?TS_VERSION_CURRENT),
    ok = rt:upgrade(Node_C, ?TS_VERSION_CURRENT),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_capability(Node_A, ?DDL_REC_CAP, v2),
    ok.

upgrade_a_node_to_1_5_test(_Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_1_4, ?TS_VERSION_1_4, ?TS_VERSION_1_4]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ok = rt:upgrade(Node_A, ?TS_VERSION_CURRENT),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_capability(Node_A, ?DDL_REC_CAP, v1),
    ok.

create_table_then_upgrade_a_node_to_1_5_test(Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_1_4, ?TS_VERSION_1_4, ?TS_VERSION_1_4]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(rt:pbc(Node_A),
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c))"
    )),
    ok = riakc_ts:put(rt:pbc(Node_A), "mytab",
        [{1,1,B*C} || B <- lists:seq(1,10), C <- lists:seq(1000,5000,1000)]),
    ExpectedResultSet = [{N} || N <- lists:seq(1000,5000,1000)],
    ok = rt:upgrade(Node_A, ?TS_VERSION_CURRENT),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    Query =
        "SELECT c FROM mytab "
        "WHERE a = 1 AND b = 1 AND c >= 1000 AND c <= 5000 ",
    ts_util:assert_row_sets(
        {rt_ignore_columns, ExpectedResultSet},
        run_query(rt:pbc(Node_A), Query, Config)
    ),
    ok.

create_table_then_upgrade_a_node_to_1_5_then_back_to_1_4_test(Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_1_4, ?TS_VERSION_1_4, ?TS_VERSION_1_4]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(rt:pbc(Node_A),
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c))"
    )),
    ok = rt:upgrade(Node_A, ?TS_VERSION_CURRENT),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    %% put in 1.5, read in 1.4
    ok = riakc_ts:put(rt:pbc(Node_A), "mytab",
        [{1,1,B*C} || B <- lists:seq(1,10), C <- lists:seq(1000,5000,1000)]),
    ExpectedResultSet = [{N} || N <- lists:seq(1000,5000,1000)],
    ok = rt:upgrade(Node_A, ?TS_VERSION_1_4),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    Query =
        "SELECT c FROM mytab "
        "WHERE a = 1 AND b = 1 AND c >= 1000 AND c <= 5000 ",
    ts_util:assert_row_sets(
        {rt_ignore_columns, ExpectedResultSet},
        run_query(rt:pbc(Node_A), Query, Config)
    ),
    ok.

create_table_in_1_5_then_downgrade_to_1_4_test(Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(rt:pbc(Node_A),
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c))"
    )),
    ok = riakc_ts:put(rt:pbc(Node_A), "mytab",
        [{1,1,B*C} || B <- lists:seq(1,10), C <- lists:seq(1000,5000,1000)]),
    ExpectedResultSet = [{N} || N <- lists:seq(1000,5000,1000)],
    ok = rt:upgrade(Node_A, ?TS_VERSION_1_4),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    Query =
        "SELECT c FROM mytab "
        "WHERE a = 1 AND b = 1 AND c >= 1000 AND c <= 5000 ",
    ts_util:assert_row_sets(
        {rt_ignore_columns, ExpectedResultSet},
        run_query(rt:pbc(Node_A), Query, Config)
    ),
    ok.

create_1_5_table_then_downgrade_test(Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(rt:pbc(Node_A),
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c DESC))"
    )),
    ok = rt:upgrade(Node_A, ?TS_VERSION_1_4),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    Query =
        "SELECT c FROM mytab WHERE a = 1 AND b = 1 AND c >= 1000 AND c <= 5000;",
    ?assertMatch(
        {error, _},
        run_query(rt:pbc(Node_B), Query, Config)
    ),
    ok.

all_nodes_must_support_table_features_test(_Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_CURRENT, ?TS_VERSION_1_4, ?TS_VERSION_CURRENT]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ?assertMatch(
        {error, _},
        riakc_ts:query(rt:pbc(Node_A),
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c DESC))"
    )).

create_table_on_current_node_in_mixed_version_cluster_test(Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_CURRENT, ?TS_VERSION_1_4, ?TS_VERSION_CURRENT]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(rt:pbc(Node_A),
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c))"
    )),
    ok = riakc_ts:put(rt:pbc(Node_B), "mytab",
        [{1,1,B*C} || B <- lists:seq(1,10), C <- lists:seq(1000,5000,1000)]),
    ExpectedResultSet = [{N} || N <- lists:seq(1000,5000,1000)],
    Query =
        "SELECT c FROM mytab "
        "WHERE a = 1 AND b = 1 AND c >= 1000 AND c <= 5000 ",
    ts_util:assert_row_sets(
        {rt_ignore_columns, ExpectedResultSet},
        run_query(rt:pbc(Node_B), Query, Config)
    ),
    ok.

create_table_on_previous_node_in_mixed_version_cluster_test(Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_CURRENT, ?TS_VERSION_1_4, ?TS_VERSION_CURRENT]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(rt:pbc(Node_B),
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c))"
    )),
    ok = riakc_ts:put(rt:pbc(Node_A), "mytab",
        [{1,1,B*C} || B <- lists:seq(1,10), C <- lists:seq(1000,5000,1000)]),
    ExpectedResultSet = [{N} || N <- lists:seq(1000,5000,1000)],
    Query =
        "SELECT c FROM mytab "
        "WHERE a = 1 AND b = 1 AND c >= 1000 AND c <= 5000 ",
    ts_util:assert_row_sets(
        {rt_ignore_columns, ExpectedResultSet},
        run_query(rt:pbc(Node_A), Query, Config)
    ),
    ok.

create_table_then_modify_it_before_activation(Config) ->
    [Node_A, Node_B, Node_C] =
        rt:deploy_nodes([?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT, ?TS_VERSION_CURRENT]),
    ok = rt:join_cluster([Node_A,Node_B,Node_C]),
    ok = rt:wait_until_ring_converged([Node_A,Node_B,Node_C]),
    Table_def_1 =
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "d TIMESTAMP NOT NULL, "
            "e TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,c,d,quantum(e,1,s)), a,b,c,d,e))",
    Table_def_2 =
            "CREATE TABLE mytab ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c,1,s)), a,b,c))",
    Fmt = "{\\\"props\\\": {\\\"table_def_1\\\": \\\"~s\\\"}}",
    {ok,_} = rt:admin(Node_A, ["bucket-type", "create", "mytab", lists:flatten(io_lib:format(Fmt, [Table_def_1]))]),
    {ok,_} = rt:admin(Node_A, ["bucket-type", "create", "mytab", lists:flatten(io_lib:format(Fmt, [Table_def_2]))]),
    {ok,_} = rt:admin(Node_A, ["bucket-type", "activate", "mytab"]),
    ok = riakc_ts:put(rt:pbc(Node_A), "mytab",
        [{1,1,B*C} || B <- lists:seq(1,10), C <- lists:seq(1000,5000,1000)]),
    ExpectedResultSet = [{N} || N <- lists:seq(1000,5000,1000)],
    Query =
        "SELECT c FROM mytab "
        "WHERE a = 1 AND b = 1 AND c >= 1000 AND c <= 5000 ",
    ts_util:assert_row_sets(
        {rt_ignore_columns, ExpectedResultSet},
        run_query(rt:pbc(Node_A), Query, Config)
    ),
    ok.
