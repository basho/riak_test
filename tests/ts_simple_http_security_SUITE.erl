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
%% %% Tests for security using the HTTP interface.
%%
%% -------------------------------------------------------------------
-module(ts_simple_http_security_SUITE).

-export([
    all/0,
    end_per_group/2,
    end_per_suite/1,
    end_per_testcase/2,
    ensure_https_requires_authentication_test/1,
    groups/0,
    init_per_group/2,
    init_per_suite/1,
    init_per_testcase/2,
    password_user_cannot_connect_with_wrong_password_test/1,
    suite/0,
    with_security_user_cannot_create_table_without_permissions_test/1,
    with_security_user_cannot_put_without_permissions_test/1,
    with_security_user_cannot_query_without_permissions_test/1,
    with_security_when_user_is_given_permissions_user_can_create_table_test/1,
    with_security_when_user_is_given_permissions_user_can_put_data_test/1,
    with_security_when_user_is_given_permissions_user_can_query_data_test/1]
).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(ibrowse),
    io:format("turning on tracing"),
    ibrowse:trace_on(),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/http_certs",

    %% make a bunch of crypto keys
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:endusers(CertDir, "rootCA", ["site3.basho.com", "site4.basho.com"]),

    lager:info("Deploy a node"),
    ClusterConf = [
        {riak_core, [
            {default_bucket_props, [{allow_mult, true}, {dvv_enabled, true}]},
            {ssl, [
                {certfile,   filename:join([CertDir, "site3.basho.com/cert.pem"])},
                {keyfile,    filename:join([CertDir, "site3.basho.com/key.pem"])},
                {cacertfile, filename:join([CertDir, "site3.basho.com/cacerts.pem"])}
            ]}
        ]},
        {riak_search, [ {enabled, false}]}
    ],
    [Node|_] = Nodes = rt:build_cluster(1, ClusterConf),
    {HttpIpPort, HttpsIpPort} = enable_ssl(Node),

    %% build the test context
    Ctx = [
        {cluster, Nodes},
        {http, HttpIpPort},
        {https, HttpsIpPort},
        {cert_dir, CertDir} | Config],

    %% enable riak security
    {ok,_} = riak_admin(Ctx, ["security", "enable"]),

    Ctx.

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
%% UTIL FUNCTIONS
%%--------------------------------------------------------------------

enable_ssl(Node) ->
    [{http, {IP, Port}}|_] = rt:connection_info(Node),
    HttpPort = Port+1000,
    rt:update_app_config(Node, [{riak_api, [{https, [{IP, HttpPort}]}]}]),
    rt:wait_until_pingable(Node),
    rt:wait_for_service(Node, riak_kv),
    {{IP, Port}, {IP, HttpPort}}.

client_conn(Ctx, Username, Password) ->
    CertDir = proplists:get_value(cert_dir, Ctx),
    {IP, Port} = proplists:get_value(https, Ctx),
    rhc:create(IP, Port, "riak", [{is_ssl, true},
        {credentials, Username, Password},
        {ssl_options, [
            {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])},
            {verify, verify_peer},
            {reuse_sessions, false}
        ]}
    ]).
    
riak_admin(Ctx, Args) ->
    [Node|_] = proplists:get_value(cluster, Ctx),
    {ok,_Out} = Result = rt:admin(Node, Args),
    ct:pal("riak-admin ~s~n~s", [string:join(Args, " "), _Out]),
    Result.

create_trusted_user(Ctx, Perms) ->
    User = user_name(),
    Password = "password",
    {ok,_} = riak_admin(Ctx,
        ["security", "add-user", User]),
    {ok,_} = riak_admin(Ctx,
        ["security", "add-source", Perms, "127.0.0.1/32", "trust"]),
    client_conn(Ctx, User, Password).

create_table_sql(Name) ->
    lists:flatten(io_lib:format(
        "create table ~p ("
        " a varchar not null,"
        " b varchar not null,"
        " c timestamp not null,"
        " d sint64,"
        " primary key ((a, b, quantum(c, 1, m)), a, b, c))", [Name])).

%% From riak_core_security.erl
%% Avoid whitespace, control characters, comma, semi-colon,
%% non-standard Windows-only characters, other misc
-define(ILLEGAL, lists:seq(0, 44) ++ lists:seq(58, 63) ++
            lists:seq(127, 191)).

user_name() ->
    User = string:to_lower(base64:encode_to_string(term_to_binary(make_ref()))),
    [C || C <- User, not lists:member(C, ?ILLEGAL)].

execute_query(Ctx, Query) ->
    {IP, Port} = proplists:get_value(https, Ctx),
    URL = lists:flatten(io_lib:format("http://~s:~B/ts/v1/query",
                        [IP, Port])),
    ibrowse:send_req(URL, [], post, Query).

row(A, B, C, D) ->
    io_lib:format("{\"a\": \"~s\", \"b\": \"~s\", \"c\": ~B, \"d\":~B}",
        [A, B, C, D]).

post_data(Ctx, Table, Body) ->
    {IP, Port} = proplists:get_value(https, Ctx),
    URL = lists:flatten(
        io_lib:format("http://~s:~B/ts/v1/tables/~s/keys", [IP, Port, Table])),
    ibrowse:send_req(URL, [{"Content-Type", "application/json"}], post, lists:flatten(Body)).

%%--------------------------------------------------------------------
%% TESTS
%%--------------------------------------------------------------------

ensure_https_requires_authentication_test(Ctx) ->
    {IP, Port} = proplists:get_value(https, Ctx),
    lager:info("Checking SSL demands authentication"),
    Conn = rhc:create(IP, Port, "riak", [{is_ssl, true}]),
    ?assertMatch(
        {error, {ok, "401", _, _}},
        rhc:ping(Conn)
    ).

password_user_cannot_connect_with_wrong_password_test(Ctx) ->
    User = "stranger",
    Password = "drongo",
    {ok,_} = riak_admin(Ctx,
        ["security", "add-user", User, "password=donk"]),
    {ok,_} = riak_admin(Ctx,
        ["security", "add-source", "all", "127.0.0.1/32", "password"]),
    Conn = client_conn(Ctx, User, Password),
    ?assertMatch(
        {error, {ok, "401", _, _}},
        rhc:ping(Conn)
    ).

with_security_user_cannot_create_table_without_permissions_test(Ctx) ->
    create_trusted_user(Ctx, "all"),
    Query = create_table_sql("table1"),
    %% just assert on the error message this once, to make sure it is getting
    %% formatted correctly.
    ?assertMatch(
        {ok, "400", _Headers, _Body},
        execute_query(Ctx, Query)
    ).

with_security_when_user_is_given_permissions_user_can_create_table_test(Ctx) ->
    create_trusted_user(Ctx, "riak_ts.query_create_table"),
    Query = create_table_sql("table2"),
    %% just assert on the error message this once, to make sure it is getting
    %% formatted correctly.
    ?assertMatch(
        {ok, "400", _Headers, _Body},
        execute_query(Ctx, Query)
    ).

with_security_user_cannot_put_without_permissions_test(Ctx) ->
    create_trusted_user(Ctx, "riak_ts.query_create_table"),
    Query = create_table_sql("table3"),
    ?assertMatch(
        {ok, "400", _Headers, _Body},
        execute_query(Ctx, Query)
    ),
    RowStr = row("q1", "w1", 11, 110),
    ?assertMatch(
        {ok, "200", _Headers, "{\"success\":true}"},
        post_data(Ctx, "table3", RowStr)
    ).

with_security_when_user_is_given_permissions_user_can_put_data_test(Ctx) ->
    create_trusted_user(Ctx, "riak_ts.query_create_table,riak_ts.put"),
    Query = create_table_sql("table4"),
    ?assertMatch(
        {ok, "400", _Headers, _Body},
        execute_query(Ctx, Query)
    ),
    RowStr = row("q1", "w1", 11, 110),
    ?assertMatch(
        {ok, "200", _Headers, "{\"success\":true}"},
        post_data(Ctx, "table4", RowStr)
    ).

with_security_user_cannot_query_without_permissions_test(Ctx) ->
    create_trusted_user(Ctx, "riak_ts.query_create_table,riak_ts.put"),
    Query = create_table_sql("table4"),
    ?assertMatch(
        {ok, "400", _Headers, _Body},
        execute_query(Ctx, Query)
    ),
    RowStrs = string:join([row("q1", "w2", 20, 150), row("q1", "w1", 20, 119)],
        ", "),
    ?assertMatch(
        {ok, "200", _Headers, "{\"success\":true}"},
        post_data(Ctx, "table4", RowStrs)
    ),
    Select = "SELECT * FROM table4 WHERE a='q1' AND b='w1' AND c>1 AND c<99",
    ?assertMatch(
        {ok, "400", _Headers, _Body},
        execute_query(Ctx, Select)
    ).

with_security_when_user_is_given_permissions_user_can_query_data_test(Ctx) ->
    create_trusted_user(Ctx, "riak_ts.query_create_table,riak_ts.put,riak_ts.query_select"),
    Query = create_table_sql("table4"),
    ?assertMatch(
        {ok, "400", _Headers, _Body},
        execute_query(Ctx, Query)
    ),
    RowStrs = string:join([row("q1", "w2", 20, 150), row("q1", "w1", 20, 119)],
        ", "),
    ?assertMatch(
        {ok, "200", _Headers, "{\"success\":true}"},
        post_data(Ctx, "table4", RowStrs)
    ),
    Select = "SELECT * FROM table4 WHERE a='q1' AND b='w1' AND c>1 AND c<99",
    Body = "{\"columns\":[\"a\",\"b\",\"c\",\"d\"],"
        "\"rows\":[[\"q1\",\"w1\",11,110],"
        "[\"q1\",\"w1\",20,119]]}",
    ?assertMatch(
        {ok,"200", _Headers, Body},
        execute_query(Ctx, Select)
    ).




