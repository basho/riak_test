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
%% Tests for range queries around the boundaries of quanta.
%%
%% -------------------------------------------------------------------
-module(ts_simple_http_api_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    application:start(ibrowse),
    [Node|_] = Cluster = ts_setup:start_cluster(1),
    rt:wait_until_pingable(Node),
    rt:wait_for_service(Node, riak_kv),
    Cfg = [{cluster, Cluster} | Config],
    ok = wait_for_web_machine(60, Cfg),
    Cfg.

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
      show_tables_test,
      describe_table_test,
      describe_nonexisting_table_test,
      bad_describe_query_test,
      post_single_row_test,
      post_single_row_with_null_test,
      post_single_row_missing_field_test,
      post_single_row_wrong_field_test,
      post_several_rows_test,
      post_row_to_nonexisting_table_test,
      list_keys_test,
      list_keys_nonexisting_table_test,
      select_test,
      select_with_null_test,
      select_subset_test,
      invalid_select_test,
      invalid_query_test,
      delete_data_existing_row_test,
      delete_data_nonexisting_row_test,
      delete_data_nonexisting_table_test,
      delete_data_wrong_path_test
    ].


%% column_names_def_1() ->
%%     [<<"a">>, <<"b">>, <<"c">>].

wait_for_web_machine(Secs, _) when Secs =< 0 ->
    {error, "Webmachine not ready in time."};
wait_for_web_machine(Secs, Cfg) ->
    %% use SHOW TABLES to detect readiness because it does not modify state
    case execute_query("SHOW TABLES", Cfg) of
        {ok, "200", _, _} ->
            ok;
        {ok, "503", _, _} ->
            timer:sleep(1000),
            wait_for_web_machine(Secs-1, Cfg)
    end.

table_def_bob() ->
    "create table bob ("
    " a varchar not null,"
    " b blob not null,"
    " c timestamp not null,"
    " d sint64,"
    " primary key ((a, b, quantum(c, 1, m)), a, b, c))".

bad_table_def() ->
    "create table pap ("
    " a timestamp not null,"
    " b timestamp not null,"
    " c timestamp not null)".

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
    "text/plain" = content_type(Headers),
    "Query error: Missing primary key" = Body.


create_existing_table_test(Cfg) ->
    Query = table_def_bob(),
    {ok, "409", Headers, Body} =
        execute_query(Query, Cfg),
    "text/plain" = content_type(Headers),
    "Table \"bob\" already exists" = Body.

show_tables_test(Cfg) ->
    Query = "show tables",
    {ok, "200", Headers, Body } = execute_query(Query, Cfg),
    "application/json" = content_type(Headers),
    "{\"columns\":[\"Table\",\"Status\"],"
        "\"rows\":[[\"bob\",\"Active\"]]}" = Body.

describe_table_test(Cfg) ->
    Query = "describe bob",
    {ok, "200", Headers,  Body } = execute_query(Query, Cfg),
    "application/json" = content_type(Headers),
    "{\"columns\":"++_ = Body.

describe_nonexisting_table_test(Cfg) ->
    Query = "describe john",
    {ok, "404", Headers, Body} = execute_query(Query, Cfg),
    "text/plain" = content_type(Headers),
    "Table \"john\" does not exist" = Body.

bad_describe_query_test(Cfg) ->
    Query = "descripe bob",
    {ok, "400", Headers, Body} = execute_query(Query, Cfg),
    "text/plain" = content_type(Headers),
    "Query error: Unexpected token: 'descripe'" = Body.

%%% put
post_single_row_test(Cfg) ->
    RowStr = row("q1", base64:encode_to_string("w1"), 11, 110),
    {ok, "200", Headers, RespBody} = post_data("bob", RowStr, Cfg),
    "application/json" = content_type(Headers),
    RespBody = success_body().

post_single_row_with_null_test(Cfg) ->
    RowStr = row("qN", base64:encode_to_string("wN"), 11, null),
    {ok, "200", Headers, RespBody} = post_data("bob", RowStr, Cfg),
    "application/json" = content_type(Headers),
    RespBody = success_body().

post_single_row_missing_field_test(Cfg) ->
    RowStr = missing_field_row("q1", 12, 200),
    {ok, "400", Headers, Body} =
        post_data("bob", RowStr, Cfg),
    "text/plain" = content_type(Headers),
    "Missing field \"b\" for key in table \"bob\"" = Body.

post_single_row_wrong_field_test(Cfg) ->
    RowStr = wrong_field_type_row("q1", base64:encode_to_string("w1"), 12, "raining"),
    {ok,"400", Headers, Body} = post_data("bob", RowStr, Cfg),
    "text/plain" = content_type(Headers),
    "Bad value for field \"d\" of type sint64 in table \"bob\"" = Body.


post_several_rows_test(Cfg) ->
    RowStrs = string:join([row("q1", base64:encode_to_string("w2"), 20, 150),
                           row("q1", base64:encode_to_string("w1"), 20, 119)],
                          ", "),
    Body = io_lib:format("[~s]", [RowStrs]),
    {ok, "200", Headers, RespBody} = post_data("bob", Body, Cfg),
    "application/json" = content_type(Headers),
    RespBody = success_body().

post_row_to_nonexisting_table_test(Cfg) ->
    RowStr = row("q1", base64:encode_to_string("w1"), 30, 142),
    {ok,"404", Headers, Body} = post_data("bill", RowStr, Cfg),
    "text/plain" = content_type(Headers),
    "Table \"bill\" does not exist" = Body.

%%% list_keys
list_keys_test(Cfg) ->
    {"200", Headers, Body} = list_keys("bob", Cfg),
    "text/plain" = content_type(Headers),
    RecordURLs = string:tokens(Body, "\n"),
    ?assertEqual(length(RecordURLs), 4),
    %% do a get on each key
    lists:foreach(
      fun(URL) ->
              {ok, "200", _Headers, _Body} = ibrowse:send_req(URL, [], get)
      end,
      RecordURLs).


list_keys_nonexisting_table_test(Cfg) ->
    {"404", Headers, Body} = list_keys("john", Cfg),
    "text/plain" = content_type(Headers),
    "Table \"john\" does not exist" = Body.

%%% select
select_test(Cfg) ->
    Select = "select * from bob where a='q1' and b=" ++
        hexlify("w1") ++ " and c>1 and c<99",
    {ok,"200", Headers, Body} = execute_query(Select, Cfg),
    "application/json" = content_type(Headers),
    ?assertEqual(
       "{\"columns\":[\"a\",\"b\",\"c\",\"d\"]," ++
           "\"rows\":[[\"q1\",\"" ++ base64:encode_to_string("w1") ++ "\",11,110]," ++
           "[\"q1\",\"" ++ base64:encode_to_string("w1") ++ "\",20,119]]}", Body).

select_with_null_test(Cfg) ->
    Select = "select * from bob where a='qN' and b=" ++ hexlify("wN") ++ " and c>1 and c<99 and d is null",
    {ok,"200", Headers, Body} = execute_query(Select, Cfg),
    "application/json" = content_type(Headers),
    ?assertEqual(
       "{\"columns\":[\"a\",\"b\",\"c\",\"d\"],"
       "\"rows\":[[\"qN\",\"" ++ base64:encode_to_string("wN") ++ "\",11,[]]]}", Body).

select_subset_test(Cfg) ->
    Select = "select * from bob where a='q1' and b=" ++ hexlify("w1") ++ " and c>1 and c<15",
    {ok, "200", Headers, Body} = execute_query(Select, Cfg),
    "application/json" = content_type(Headers),
    ?assertEqual(
       "{\"columns\":[\"a\",\"b\",\"c\",\"d\"]," ++
           "\"rows\":[[\"q1\",\"" ++ base64:encode_to_string("w1") ++ "\",11,110]]}", Body).

invalid_select_test(Cfg) ->
    Select = "select * from bob where a='q1' and c>1 and c<15",
    %% @todo: this really ought to be a 4XX error, but digging into the errors
    %% from riak_ql might be too much for this API.
    {ok, "500", Headers, Body} = execute_query(Select, Cfg),
    "text/plain" = content_type(Headers),
    "Execution of select query failed on table \"bob\" (The 'b' parameter is part the primary key but not specified in the where clause.)"
        = Body.

invalid_query_test(Cfg) ->
    Select = "OHNOES A DANGLING QUOTE ' ",
    {ok, "400", Headers, Body} = execute_query(Select, Cfg),
    "text/plain" = content_type(Headers),
    "Query error: Unexpected token '''." = Body.

%%% delete
delete_data_existing_row_test(Cfg) ->
    {ok, "200", Headers, Body} = delete("bob", "q1", base64:encode_to_string("w1"), 11, Cfg),
    "application/json" = content_type(Headers),
    Body = success_body(),
    Select = "select * from bob where a='q1' and b=" ++ hexlify("w1") ++ " and c>1 and c<99",
    {ok, "200", _Headers2, QueryBody} = execute_query(Select, Cfg),
    ?assertEqual(
       "{\"columns\":[\"a\",\"b\",\"c\",\"d\"]," ++
           "\"rows\":[[\"q1\",\"" ++ base64:encode_to_string("w1") ++ "\",20,119]]}",
       QueryBody).


delete_data_nonexisting_row_test(Cfg) ->
    {ok, "404", Headers, Body } = delete("bob", "q1", base64:encode_to_string("w1"), 500, Cfg),
    "text/plain" = content_type(Headers),
    "Key not found"
        = Body.

delete_data_nonexisting_table_test(Cfg) ->
    {ok, "404", Headers, Body } = delete("bill", "q1", base64:encode_to_string("w1"), 20, Cfg),
    "text/plain" = content_type(Headers),
    "Table \"bill\" does not exist" = Body.

delete_data_wrong_path_test(Cfg) ->
    {ok, "400", Headers, Body} = delete_wrong_path("bob", "q1", base64:encode_to_string("w1"), 20, Cfg),
    "text/plain" = content_type(Headers),
    "Not all key-constituent fields given on URL" = Body.

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
    {ibrowse_req_id, ReqID} = ibrowse:send_req(URL, [], get, [], [{stream_to, self()}]),
    collect_stream(ReqID).

collect_stream(ReqID) ->
    {Code, Headers} = collect_headers(ReqID),
    Body = collect_body(ReqID),
    {Code, Headers, Body}.

collect_headers(ReqID) ->
    receive
        {ibrowse_async_headers, ReqID, Code, Headers} ->
            {Code, Headers}
    end.

collect_body(ReqID) ->
    receive
        {ibrowse_async_response, ReqID, BodyPart}  ->
            BodyPart ++ collect_body(ReqID);
        {ibrowse_async_response_end, ReqID} ->
            []
    end.

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
    io_lib:format("{\"a\": \"~s\", \"b\": \"~s\", \"c\": ~B, \"d\":~s}",
                  [A, B, C, number_or_null(D)]).

number_or_null(null) ->
    "null";
number_or_null(A) ->
    io_lib:format("~B", [A]).


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

hexlify(Bin) ->
    lists:flatten(io_lib:format("0x~s", [mochihex:to_hex(Bin)])).
