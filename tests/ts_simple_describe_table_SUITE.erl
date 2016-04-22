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
    Cluster = ts_util:build_cluster(multiple),
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

client_pid(Ctx) ->
    [Node|_] = proplists:get_value(cluster, Ctx),
    rt:pbc(Node).

run_query(Ctx, Query) ->
    riakc_ts:query(client_pid(Ctx), Query).

%% The columns for describe should not be different depending
%% on the query 
static_columns() ->
    [<<"Column">>,<<"Type">>,<<"Is Null">>,<<"Primary Key">>, <<"Local Key">>].

%%%
%%% Test basic table description
%%%

basic_table_test(Ctx) ->
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
    {ok,_} = run_query(Ctx, Table_def),
    ?assertEqual(
        {ok, {
            static_columns(),
            [{<<"myfamily">>,   <<"varchar">>,   false,  1,  1},
             {<<"myseries">>,   <<"varchar">>,   false,  2,  2},
             {<<"time">>,       <<"timestamp">>, false,  3,  3},
             {<<"weather">>,    <<"varchar">>,   false, [], []},
             {<<"temperature">>,<<"double">>,    true,  [], []}]}},
        run_query(Ctx, "DESCRIBE GeoCheckin")
    ).
