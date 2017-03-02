%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016-2017 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%s
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% Tests for the different combinations of keys supported by
%% Riak Time Series.
%%
%% -------------------------------------------------------------------
-module(ts_cluster_stream_list_keys_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    %% Allow listing of buckets and keys for testing
    application:set_env(riakc, allow_listing, true),

    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    [Node|_] = Cluster = ts_setup:start_cluster(3),
    Pid = rt:pbc(Node),
    table_def_basic_create_data(Cluster, Pid),
    desc_on_quantum_table_create_data(Pid),
    desc_on_varchar_table_create_data(Pid),
    create_data_in_basic_varchar_table(Cluster, Pid),
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

%%--------------------------------------------------------------------
%% COMMON TESTS
%%--------------------------------------------------------------------

%%%
%%%
%%%

table_def_basic_create_data(_Cluster, Pid) ->
    Table_def =
        "CREATE TABLE basic_table ("
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 1, 's')), a,b,c))",
    {ok, _} = riakc_ts:query(Pid, Table_def),
    ok = riakc_ts:put(Pid, <<"basic_table">>,
            [{1,1,N} || N <- lists:seq(100,5000,100)]).

basic_table_stream_list_keys_test(Ctx) ->
    {ok, ReqId} = riakc_ts:stream_list_keys(client_pid(Ctx), <<"basic_table">>, infinity),
    ts_data:assert_row_sets(
        [{1,1,N} || N <- lists:seq(100,5000,100)],
        stream_list_keys_receive(ReqId)
    ).

%%%
%%%
%%%

create_data_in_basic_varchar_table(_Cluster, Pid) ->
    Table_def =
        "CREATE TABLE basic_varchar_table ("
        "a SINT64 NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 1, 's')), a,b,c))",
    {ok, _} = riakc_ts:query(Pid, Table_def),
    ok = riakc_ts:put(Pid, <<"basic_varchar_table">>,
            [{1,<<N:32>>,N} || N <- lists:seq(100,5000,100)]).

basic_varchar_table_stream_list_keys_test(Ctx) ->
    {ok, ReqId} = riakc_ts:stream_list_keys(client_pid(Ctx), <<"basic_varchar_table">>, infinity),
    ts_data:assert_row_sets(
        [{1,<<N:32>>,N} || N <- lists:seq(100,5000,100)],
        stream_list_keys_receive(ReqId)
    ).

%%%
%%% DESCENDING KEYS, DESC on quantum
%%%

desc_on_quantum_table_create_data(Pid) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(Pid, 
            "CREATE TABLE desc_on_quantum_table ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c, 1, 's')), a,b,c DESC))")
    ),
    ok = riakc_ts:put(Pid, <<"desc_on_quantum_table">>,
            [{1,1,N} || N <- lists:seq(100,5000,100)]).

desc_on_quantum_stream_list_keys_test(Ctx) ->
    {ok, ReqId} = riakc_ts:stream_list_keys(
        client_pid(Ctx), <<"desc_on_quantum_table">>, []),
    ts_data:assert_row_sets(
        [{1,1,N} || N <- lists:seq(100,5000,100)],
        stream_list_keys_receive(ReqId)
    ).

%%%
%%% DESCENDING KEYS, DESC on varchar
%%%

desc_on_varchar_table_create_data(Pid) ->
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(Pid, 
            "CREATE TABLE desc_on_varchar_table ("
            "a SINT64 NOT NULL, "
            "b VARCHAR NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY ((a,b,quantum(c, 1, 's')), a,b DESC,c))")
    ),
    ok = riakc_ts:put(Pid, <<"desc_on_varchar_table">>,
            [{1,<<N:32>>,N} || N <- lists:seq(100,5000,100)]).

desc_on_varchar_stream_list_keys_test(Ctx) ->
    {ok, ReqId} = riakc_ts:stream_list_keys(client_pid(Ctx), <<"desc_on_varchar_table">>, []),
    ts_data:assert_row_sets(
        [{1,<<N:32>>,N} || N <- lists:seq(100,5000,100)],
        stream_list_keys_receive(ReqId)
    ).


%%--------------------------------------------------------------------
%% UTILS
%%--------------------------------------------------------------------

stream_list_keys_receive(ReqId) ->
    lists:sort(stream_list_keys_receive2(ReqId, [])).

stream_list_keys_receive2(ReqId, Acc) ->
    receive
        {ReqId, {keys, Keys}} ->
            stream_list_keys_receive2(ReqId, lists:append(Keys, Acc));
        {ReqId, {error, Reason}} ->
            {error, Reason};
        {ReqId, done} ->
            Acc;
        Else ->
            {error, {unknown_message, Else}}
    after 3000 ->
            {ok, Acc}
    end.
