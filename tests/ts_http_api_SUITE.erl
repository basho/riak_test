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
      select_test,
      select_subset_test,
      invalid_select_test,
      delete_data_existing_row_test,
      delete_data_nonexisting_row_test,
      delete_data_nonexisting_table_test,
      delete_data_wrong_path_test
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
    {ok, "400", Headers, Body} = execute_query(Query, Cfg),
    "application/json" = content_type(Headers),
    "{\"error\":\"bad query: parse error: {0,riak_ql_parser,<<\\\"Missing primary key\\\">>}\"}" = Body.


create_existing_table_test(Cfg) ->
    Query = table_def_bob(),
    {ok, "409", Headers, Body} =
        execute_query(Query, Cfg),
    "application/json" = content_type(Headers),
    "{\"error\":\"table <<\\\"bob\\\">> already exists\"}" = Body.


describe_table_test(Cfg) ->
    Query = "describe bob",
    {ok, "200", Headers,  Body } = execute_query(Query, Cfg),
    "application/json" = content_type(Headers),
    "{\"columns\":"++_ = Body.

describe_nonexisting_table_test(Cfg) ->
    Query = "describe john",
    {ok, "404", Headers, Body} = execute_query(Query, Cfg),
    "application/json" = content_type(Headers),
    "{\"error\":\"table <<\\\"john\\\">> does not exist\"}" = Body.

bad_describe_query_test(Cfg) ->
    Query = "descripe bob",
    {ok, "400", Headers, Body} = execute_query(Query, Cfg),
    "application/json" = content_type(Headers),
    "{\"error\":\"bad query: parse error: {<<\\\"descripe\\\">>,riak_ql_parser,\\n              [\\\"syntax error before: \\\",\\\"identifier\\\"]}\"}" = Body.

%%% put
post_single_row_test(Cfg) ->
    RowStr = row("q1", "w1", 11, 110),
    {ok, "200", Headers, RespBody} = post_data("bob", RowStr, Cfg),
    "application/json" = content_type(Headers),
    RespBody = success_body().

post_single_row_missing_field_test(Cfg) ->
    RowStr = missing_field_row("q1", 12, 200),
    {ok, "400", Headers, Body} =
        post_data("bob", RowStr, Cfg),
    "application/json" = content_type(Headers),
    "{\"error\":\"wrong body: {data_problem,{missing_field,<<\\\"b\\\">>}}\"}"
        = Body.

post_single_row_wrong_field_test(Cfg) ->
    RowStr = wrong_field_type_row("q1", "w1", 12, "raining"),
    {ok,"400", Headers, Body} = post_data("bob", RowStr, Cfg),
    "application/json" = content_type(Headers),
    "{\"error\":\"wrong body: {data_problem,{wrong_type,sint64,<<\\\"raining\\\">>}}\"}"
        = Body.


post_several_rows_test(Cfg) ->
    RowStrs = string:join([row("q1", "w2", 20, 150), row("q1", "w1", 20, 119)],
                          ", "),
    Body = io_lib:format("[~s]", [RowStrs]),
    {ok, "200", Headers, RespBody} = post_data("bob", Body, Cfg),
    "application/json" = content_type(Headers),
    RespBody = success_body().

post_row_to_nonexisting_table_test(Cfg) ->
    RowStr = row("q1", "w1", 30, 142),
    {ok,"404", Headers, Body} = post_data("bill", RowStr, Cfg),
    "application/json" = content_type(Headers),
    "{\"error\":\"table 'riak_ql_table_bill$1' not created\"}" = Body.

%%% list_keys
list_keys_test(Cfg) ->
    {ok, "200", Headers, Body} = list_keys("bob", Cfg),
    "application/json" = content_type(Headers),
    ok = Body.

list_keys_nonexisting_table_test(Cfg) ->
    {ok, "404", Headers, Body} = list_keys("john", Cfg),
    "application/json" = content_type(Headers),
    ok = Body.

%%% select
select_test(Cfg) ->
    Select = "select * from bob where a='q1' and b='w1' and c>1 and c<99",
    {ok, "200", Headers, Body} = execute_query(Select, Cfg),
    "application/json" = content_type(Headers),
    "{\"columns\":[\"a\",\"b\",\"c\",\"d\"]," ++
        "\"rows\":[[\"q1\",\"w1\",11,110]," ++
        "[\"q1\",\"w1\",20,119]]}" = Body.

select_subset_test(Cfg) ->
    Select = "select * from bob where a='q1' and b='w1' and c>1 and c<15",
    {ok, "200", Headers, Body} = execute_query(Select, Cfg),
    "application/json" = content_type(Headers),
    "{\"columns\":[\"a\",\"b\",\"c\",\"d\"]," ++
        "\"rows\":[[\"q1\",\"w1\",11,110]]}" = Body.

invalid_select_test(Cfg) ->
    Select = "select * from bob where a='q1' and c>1 and c<15",
    {ok, "500", Headers, Body} = execute_query(Select, Cfg),
    "application/json" = content_type(Headers),
    "select query execution error: {missing_key_clause" ++ _ = Body.

%%% delete
delete_data_existing_row_test(Cfg) ->
    {ok, "200", Headers, Body} = delete("bob", "q1", "w1", 11, Cfg),
    "application/json" = content_type(Headers),
    Body = success_body(),
    Select = "select * from bob where a='q1' and b='w1' and c>1 and c<99",
    {ok, "200", _Headers2,
     "{\"columns\":[\"a\",\"b\",\"c\",\"d\"],\"rows\":[[\"q1\",\"w1\",20,119]]}"} =
        execute_query(Select, Cfg).

delete_data_nonexisting_row_test(Cfg) ->
    {ok, "404", Headers, Body } = delete("bob", "q1", "w1", 500, Cfg),
    "application/json" = content_type(Headers),
    ok = Body.

delete_data_nonexisting_table_test(Cfg) ->
    {ok, "404", Headers, Body } = delete("bill", "q1", "w1", 20, Cfg),
    "application/json" = content_type(Headers),
    ok = Body.

delete_data_wrong_path_test(Cfg) ->
    {ok, "404", Headers, Body} = delete_wrong_path("bob", "q1", "w1", 20, Cfg),
    "application/json" = content_type(Headers),
    "lookup on [\"a\",\"q1\",\"b\",\"w1\",\"d\",\"20\"] failed" ++ _ = Body.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
execute_query(Query, Cfg) ->
    Node = get_node(Cfg),
    URL = query_url(Node),
    ibrowse:send_req(URL, [], post, Query).

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

query_url(Node) ->
     {IP, Port} = node_ip_and_port(Node),
     query_url(IP, Port).

query_url(IP, Port) ->
    lists:flatten(
      io_lib:format("http://~s:~B/ts/v1/query",
                    [IP, Port])).

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

delete(Table, A, B, C, Cfg) ->
    Node = get_node(Cfg),
    URL = delete_url(Node, Table, A, B, C),
    ibrowse:send_req(URL, [], delete).

delete_url(Node, Table, A, B, C) ->
    {IP, Port} = node_ip_and_port(Node),
    lists:flatten(
     io_lib:format("http://~s:~B/ts/v1/tables/~s/keys/a/~s/b/~s/c/~B",
                   [IP, Port, Table, A, B, C])).

delete_wrong_path(Table, A, B, C, Cfg) ->
    Node = get_node(Cfg),
    URL = delete_url_wrong_path(Node, Table, A, B, C),
    ibrowse:send_req(URL, [], delete).

delete_url_wrong_path(Node, Table, A, B, C) ->
    {IP, Port} = node_ip_and_port(Node),
    lists:flatten(
     io_lib:format("http://~s:~B/ts/v1/tables/~s/keys/a/~s/b/~s/d/~B",
                   [IP, Port, Table, A, B, C])).


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

content_type(Headers) ->
    proplists:get_value("Content-Type", Headers).
