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

-export([suite/0, init_per_suite/1, groups/0, all/0]).
-export([
         put_test/1,
         query_create_table_test/1,
         query_select_test/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(USER, "uesr").
-define(PASSWORD, "passowrd").
-define(TABLE1, "t1").
-define(TABLE2, "t2").

suite() ->
    [{timetrap, {minutes,10}}].

groups() ->
    [].

all() ->
    [
     query_create_table_test,
     put_test,
     query_select_test
    ].

init_per_suite(Config) ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(ibrowse),
    ibrowse:trace_on(),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/http_certs",
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:endusers(CertDir, "rootCA", ["site3.basho.com", "site4.basho.com"]),

    ClusterConf =
        [{riak_core,
          [{ssl,
            [{certfile,   filename:join([CertDir, "site3.basho.com/cert.pem"])},
             {keyfile,    filename:join([CertDir, "site3.basho.com/key.pem"])},
             {cacertfile, filename:join([CertDir, "site3.basho.com/cacerts.pem"])}]}
          ]}],

    [Node] = rt:build_cluster(1, ClusterConf),

    ok = security_command(Node, security_enable, []),

    [{http, {Ip, Port}}|_] = rt:connection_info(Node),
    rt:update_app_config(Node, [{riak_api, [{https, [{Ip, Port+1000}]}]}]),
    rt:wait_until_pingable(Node),
    rt:wait_for_service(Node, riak_kv),
    {ok, [{Ip, SslPort}]} =
        rpc:call(Node, application, get_env, [riak_api, https]),

    ok = security_command(Node, add_user, [?USER, "password=" ++ ?PASSWORD]),
    ok = security_command(Node, add_source, [?USER, "127.0.0.1/32", "trust"]),

    Client =
        rhc_ts:create(Ip, SslPort,
                      [{is_ssl, true},
                       {credentials, ?USER, ?PASSWORD},
                       {ssl_options, [
                                      {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])},
                                      {verify, verify_peer},
                                      {reuse_sessions, false}
                                     ]}
                      ]),

    [{cluster, [Node]},
     {client, Client} | Config].


%% Tests
%%--------------------------------------------------------------------

query_create_table_test(Ctx) ->
    Client = proplists:get_value(client, Ctx),
    Query1 = make_create_table_query(?TABLE1),
    Query2 = make_create_table_query(?TABLE2),

    ct:log("no permissions:\n~s", [Query1]),
    ?assertMatch({error, {401, _}}, rhc_ts:query(Client, Query1)),

    ct:log("with permissions:\n~s", [Query1]),
    ok = security_command(Ctx, grant, ["riak_ts.create_table", "on", ?TABLE1, "to", ?USER]),
    ?assertMatch({ok, _}, rhc_ts:query(Client, Query1)),

    ct:log("permissions revoked:\n~s", [Query1]),
    ok = security_command(Ctx, revoke, ["riak_ts.create_table", "on", ?TABLE1, "from", ?USER]),
    ?assertMatch({error, {401, _}}, rhc_ts:query(Client, Query1)),
    ct:log("another table:\n~s", [Query2]),
    ?assertMatch({error, {401, _}}, rhc_ts:query(Client, Query2)),

    %% with permissions again, still an error because the table already exists
    ok = security_command(Ctx, grant, ["riak_ts.create_table", "on", "any", "to", ?USER]),
    ct:log("with permissions, attempt to create existing table:\n~s", [Query1]),
    ?assertMatch({error, {409, _}}, rhc_ts:query(Client, Query1)).


put_test(Ctx) ->
    %% preconditions:
    %% * user has a riak_ts.create_table permission, nothing else;
    %% * table t1 exists, is empty.
    Client = proplists:get_value(client, Ctx),

    Data = make_data_ann([<<"a">>, <<"b">>, <<"c">>, <<"d">>], make_data()),

    ct:log("no permissions:\n~s", ["(put)"]),
    ?assertMatch({error, {401, _}}, rhc_ts:put(Client, ?TABLE1, Data)),

    ok = security_command(Ctx, grant, ["riak_ts.put", "on", ?TABLE1, "to", ?USER]),
    ct:log("permissions revoked:\n~s", ["(put)"]),
    ?assertMatch(ok, rhc_ts:put(Client, ?TABLE1, Data)),

    ok = security_command(Ctx, revoke, ["riak_ts.put", "on", ?TABLE1, "from", ?USER]),
    ct:log("permissions granted:\n~s", ["(put)"]),
    ?assertMatch({error, {401, _}}, rhc_ts:put(Client, ?TABLE1, Data)).


query_select_test(Ctx) ->
    %% preconditions:
    %% * user only has permissions riak_ts.create_table, riak_ts.put;
    %% * table t1 exists, contains data.
    Client = proplists:get_value(client, Ctx),

    Query = make_select_query(?TABLE1),
    ct:log("no permissions:\n~s", [Query]),
    ?assertMatch({error, {401, _}}, rhc_ts:query(Client, Query)),

    ok = security_command(Ctx, grant, ["riak_ts.query_select", "on", ?TABLE1, "to", ?USER]),
    ct:log("with permissions:\n~s", [Query]),
    Res = rhc_ts:query(Client, Query),
    {ok, {_Columns, Rows}} = Res,
    ct:log("rows:\n~p", [Rows]),
    ok.

%% local functions
%% -------------------------------

security_command(Ctx, Cmd, Args) when is_list(Ctx) ->
    [Node|_] = proplists:get_value(cluster, Ctx),
    security_command(Node, Cmd, Args);
security_command(Node, Cmd, Args) when is_atom(Node) ->
    ok = rpc:call(Node, riak_core_console, Cmd, [Args]).

make_data() ->
    [{list_to_binary(fmt("A~5..0b", [I rem 2])),
      <<"B">>,
      I + 1,
      if I rem 5 == 0 -> null; el/=se -> I end}  %% sprinkle some NULLs
     || I <- lists:seq(1, 300)].
make_data_ann(Columns, Data) ->
    [lists:zip(Columns, tuple_to_list(Row)) || Row <- Data].

make_create_table_query(Table) ->
    fmt("CREATE TABLE ~s ("
        " a VARCHAR NOT NULL,"
        " b VARCHAR NOT NULL,"
        " c TIMESTAMP NOT NULL,"
        " d SINT64,"
        " PRIMARY KEY ((a, b, quantum(c, 1, m)), a, b, c))", [Table]).

make_select_query(Table) ->
    fmt("SELECT * FROM ~s WHERE a = 'A00002' AND b='B' AND c >= 1 AND c < 100", [Table]).


fmt(F, A) ->
    lists:flatten(io_lib:format(F, A)).
