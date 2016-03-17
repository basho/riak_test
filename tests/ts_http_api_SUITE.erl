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
%% Tests for range queries around the boundaries of quanta.
%%
%% -------------------------------------------------------------------
-module(ts_http_api_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    [_Node|_] = Cluster = ts_util:build_cluster(single),
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
    [ create_table_test,
      create_bad_table_test,
      create_existing_table_test,
      describe_table_test,
      describe_nonexisting_table_test,
      bad_describe_query_test,
      post_single_row_test,
      post_single_row_missing_field_test,
      post_single_row_wrong_field_test,
      post_several_rows_test,
      post_row_to_nonexisting_table_test,
      list_keys_test,
      list_keys_nonexisting_table_test,
      delete_data_existing_row_test,
      delete_data_nonexisting_row_test,
      delete_data_nonexisting_table_test,
      delete_data_wrong_path_test,
      select_all_test,
      select_subset_test,
      invalid_select_test
    ].


%% column_names_def_1() ->
%%     [<<"a">>, <<"b">>, <<"c">>].


table_def_bob() ->
    "create table bob (" ++
        "a varchar not null," ++
        " b varchar not null," ++
        " c timestamp not null," ++
        " d sint64," ++
        " primary key ((a, b, quantum(c, 1, m)), a, b, c))".

bad_table_def() ->
    "create table pap ("++
        "a timestamp not null," ++
        "b timestamp not null," ++
        "c timestamp not null)".

%% client_pid(Ctx) ->
%%     [Node|_] = proplists:get_value(cluster, Ctx),
%%     rt:pbc(Node).

%%%
%%% HTTP API tests
%%%

%%% query
create_table_test(Cfg) ->
    Query = table_def_bob(),
    {ok, "200", _Headers, Body } = execute_query(Query, Cfg),
    Body = success_body().

create_bad_table_test(Cfg) ->
    Query = bad_table_def(),
    {ok, "400", _Headers, "bad query:"++_} = execute_query(Query, Cfg).

create_existing_table_test(Cfg) ->
    Query = table_def_bob(),
    {ok, "500", _Headers,
     "query error: {table_create_fail,<<\"bob\">>,already_active}"} =
        execute_query(Query, Cfg).


describe_table_test(Cfg) ->
    Query = "describe bob",
    {ok, "200", _Headers,  "{\"columns\":"++_ } = execute_query(Query, Cfg).

describe_nonexisting_table_test(Cfg) ->
    Query = "describe john",
    {ok, "404", _Headers, _} = execute_query(Query, Cfg).

bad_describe_query_test(Cfg) ->
    Query = "descripe bob",
    {ok, "400", _Headers, "bad query: \"parse error"++_} = execute_query(Query, Cfg).

%%% put
post_single_row_test(Cfg) ->
    RowStr = row("q1", "w1", 11, 110),
    {ok, "200", _Headers, RespBody} = post_data("bob", RowStr, Cfg),
    RespBody = success_body().

post_single_row_missing_field_test(Cfg) ->
    RowStr = missing_field_row("q1", 12, 200),
    {ok, "400", _Headers,
     "wrong body: {data_problem,{missing_field,<<\"b\">>}}"} =
        post_data("bob", RowStr, Cfg).

post_single_row_wrong_field_test(Cfg) ->
    RowStr = wrong_field_type_row("q1", "w1", 12, "raining"),
    {ok,"400",_Headers,
     "wrong body: {data_problem,{wrong_type,sint64,<<\"raining\">>}}"} =
        post_data("bob", RowStr, Cfg).

post_several_rows_test(Cfg) ->
    RowStrs = string:join([row("q1", "w2", 20, 150), row("q1", "w1", 20, 119)],
                          ", "),
    Body = io_lib:format("[~s]", [RowStrs]),
    {ok, "200", _Headers, RespBody} = post_data("bob", Body, Cfg),
    RespBody = success_body().

post_row_to_nonexisting_table_test(Cfg) ->
    RowStr = row("q1", "w1", 30, 142),
    {ok,"404", _Headers, "table 'riak_ql_table_bill$1' not created"} =
        post_data("bill", RowStr, Cfg).

list_keys_test(Cfg) ->
    {ok, "200", _Headers, _} = list_keys("bob", Cfg).

list_keys_nonexisting_table_test(Cfg) ->
    {ok, "404", _Headers, _} = list_keys("john", Cfg).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
execute_query(Query, Cfg) ->
    Node = get_node(Cfg),
    URL = query_url(Node, Query),
    ibrowse:send_req(URL, [], post).

post_data(Table, Body, Cfg) ->
    Node = get_node(Cfg),
    URL = post_data_url(Node, Table),
    ibrowse:send_req(URL, [{"Content-Type", "application/json"}], post, lists:flatten(Body)).


get_node(Cfg) ->
    [Node|_] = ?config(cluster, Cfg),
    Node.

node_ip_and_port(Node) ->
    {ok, [{IP, Port}]} = rpc:call(Node, application, get_env, [riak_api, http]),
    {IP, Port}.

query_url(Node, Query) ->
     {IP, Port} = node_ip_and_port(Node),
     query_url(IP, Port, Query).

query_url(IP, Port, Query) ->
    EncodedQuery = ibrowse_lib:url_encode(Query),
    lists:flatten(
      io_lib:format("http://~s:~B/ts/v1/query?query=~s",
                    [IP, Port, EncodedQuery])).

post_data_url(Node, Table) ->
    {IP, Port} = node_ip_and_port(Node),
    lists:flatten(
      io_lib:format("http://~s:~B/ts/v1/tables/~s/keys",
                    [IP, Port, Table])).

list_keys(Table, Cfg) ->
    Node = get_node(Cfg),
    URL  = list_keys_url(Node, Table),
    ibrowse:send_req(URL, [], get).

list_keys_url(Node, Table) ->
    {IP, Port} = node_ip_and_port(Node),
    lists:flatten(
      io_lib:format("http://~s:~B/ts/v1/tables/~s/list_keys",
                   [IP, Port, Table])).

row(A, B, C, D) ->
    io_lib:format("{\"a\": \"~s\", \"b\": \"~s\", \"c\": ~B, \"d\":~B}",
                  [A, B, C, D]).

missing_field_row(A, C, D) ->
    io_lib:format("{\"a\": \"~s\", \"c\": ~B, \"d\":~B}",
                  [A, C, D]).

wrong_field_type_row(A, B, C, D) ->
    io_lib:format("{\"a\": \"~s\", \"b\": \"~s\", \"c\": ~B, \"d\":~p}",
                  [A, B, C, D]).


success_body() ->
    "{\"success\":true}".
