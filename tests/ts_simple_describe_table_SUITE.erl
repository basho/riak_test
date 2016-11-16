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

-module(ts_simple_describe_table_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    Cluster = ts_util:build_cluster(single),
    [{cluster, Cluster} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [].

all() ->
    rt:grep_test_functions(?MODULE).

%%--------------------------------------------------------------------
%% UTILS
%%--------------------------------------------------------------------

client_pid(Config) ->
    [Node|_] = proplists:get_value(cluster, Config),
    rt:pbc(Node).

run_query(Config, Query) ->
    riakc_ts:query(client_pid(Config), Query).

%% get the cells for one column for all rows as a list of values, this makes it
%% easy to assert data in a column so when another column is added, the tests
%% are not broken. Add new tests for new columns!
assert_column_values(ColName, Expected, {Cols, Rows}) when is_binary(ColName),
                                                           is_list(Expected) ->
    Index = (catch lists:foldl(
        fun(E, Acc) when E == ColName ->
            throw(Acc);
           (_, Acc) ->
            Acc + 1
        end, 1, Cols)),
    % ct:pal("INDEX ~p COLS ~p~nROWS ~p", [Index, Cols, Rows]),
    Actual = [element(Index,R) || R <- Rows],
    ?assertEqual(
        Expected,
        Actual
    ).

%%--------------------------------------------------------------------
%% TESTS
%%--------------------------------------------------------------------

describe_column_name_test(Config) ->
    Table_def =
        "CREATE TABLE GeoCheckin ("
        "myfamily VARCHAR NOT NULL,"
        "myseries VARCHAR NOT NULL,"
        "time TIMESTAMP NOT NULL,"
        "weather VARCHAR NOT NULL,"
        "temperature DOUBLE,"
        "PRIMARY KEY ("
            "(myfamily, myseries, quantum(time, 15, 'm')), myfamily, myseries, time)"
        ")",
    {ok,_} = run_query(Config, Table_def),
    {ok, Result} = run_query(Config, "DESCRIBE GeoCheckin"),
    assert_column_values(
        <<"Column">>,
        [<<"myfamily">>, <<"myseries">>, <<"time">>, <<"weather">>, <<"temperature">>],
        Result
    ).

describe_column_type_test(Config) ->
    {ok, Result} = run_query(Config, "DESCRIBE GeoCheckin"),
    assert_column_values(
        <<"Type">>,
        [<<"varchar">>, <<"varchar">>, <<"timestamp">>, <<"varchar">>, <<"double">>],
        Result
    ).

describe_is_null_test(Config) ->
    {ok, Result} = run_query(Config, "DESCRIBE GeoCheckin"),
    assert_column_values(
        <<"Type">>,
        [<<"varchar">>, <<"varchar">>, <<"timestamp">>, <<"varchar">>, <<"double">>],
        Result
    ).

describe_partition_key_test(Config) ->
    {ok, Result} = run_query(Config, "DESCRIBE GeoCheckin"),
    assert_column_values(
        <<"Partition Key">>,
        [1, 2, 3, [], []],
        Result
    ).

describe_local_key_test(Config) ->
    {ok, Result} = run_query(Config, "DESCRIBE GeoCheckin"),
    assert_column_values(
        <<"Local Key">>,
        [1, 2, 3, [], []],
        Result
    ).

describe_sort_order_test(Config) ->
    Table_def =
        "CREATE TABLE desc_ts ("
        "a VARCHAR NOT NULL,"
        "b VARCHAR NOT NULL,"
        "c TIMESTAMP NOT NULL,"
        "d VARCHAR NOT NULL,"
        "e DOUBLE,"
        "PRIMARY KEY ("
            "(a, b, quantum(c, 15, 'm')), a, b, c DESC)"
        ")",
    {ok,_} = run_query(Config, Table_def),
    {ok, Result} = run_query(Config, "DESCRIBE desc_ts"),
    assert_column_values(
        <<"Sort Order">>,
        [[], [], <<"DESC">>, [], []],
        Result
    ).
