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
-module(ts_simple_security_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(inets),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/pb_security_certs",

    ok = make_certificates(CertDir),
    ok = make_certificates_http_server(CertDir),

    lager:info("Deploy some nodes"),
    ClusterConf = [
            {riak_core, [
                {default_bucket_props, [{allow_mult, true}, {dvv_enabled, true}]},
                {ssl, [
                    {certfile,   filename:join([CertDir, "site3.basho.com/cert.pem"])},
                    {keyfile,    filename:join([CertDir, "site3.basho.com/key.pem"])},
                    {cacertfile, filename:join([CertDir, "site3.basho.com/cacerts.pem"])}
                    ]}
                ]},
            {riak_search, [{enabled, false}]}
           ],
    [Node|_] = Nodes = rt:build_cluster(1, ClusterConf),

    %% build the test context
    [_, {pb, {"127.0.0.1", Port}}] = rt:connection_info(Node),
    Ctx = [{cluster,Nodes},{port,Port},{cert_dir,CertDir}|Config],

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


%% make a bunch of crypto keys
make_certificates(CertDir) ->
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:intermediateCA(CertDir, "intCA", "rootCA"),
    make_certs:intermediateCA(CertDir, "revokedCA", "rootCA"),
    make_certs:endusers(CertDir, "intCA", ["site1.basho.com", "site2.basho.com"]),
    make_certs:endusers(CertDir, "rootCA", ["site3.basho.com", "site4.basho.com", "site5.basho.com"]),
    make_certs:enduser(CertDir, "revokedCA", "site6.basho.com"),
    make_certs:revoke(CertDir, "rootCA", "site5.basho.com"),
    make_certs:revoke(CertDir, "rootCA", "revokedCA"),

    %% use a leaf certificate as a CA certificate and make a totally bogus new leaf certificate
    make_certs:create_ca_dir(CertDir, "site1.basho.com", make_certs:ca_cnf("site1.basho.com")),
    file:copy(filename:join(CertDir, "site1.basho.com/key.pem"), filename:join(CertDir, "site1.basho.com/private/key.pem")),
    make_certs:enduser(CertDir, "site1.basho.com", "site7.basho.com"),
    file:copy(filename:join([CertDir, "site1.basho.com", "cacerts.pem"]), filename:join(CertDir, "site7.basho.com/cacerts.pem")),
    {ok, Bin} = file:read_file(filename:join(CertDir, "site1.basho.com/cert.pem")),
    {ok, FD} = file:open(filename:join(CertDir, "site7.basho.com/cacerts.pem"), [append]),
    file:write(FD, ["\n", Bin]),
    file:close(FD),
    make_certs:gencrl(CertDir, "site1.basho.com"),
    ok.

%% start a HTTP server to serve the CRLs
%%
%% NB: we use the 'stand_alone' option to link the server to the
%% test process, so it exits when the test process exits.
make_certificates_http_server(CertDir) ->
    {ok, _HTTPPid} = inets:start(httpd, [{port, 8000}, {server_name, "localhost"},
                        {server_root, "/tmp"},
                        {document_root, CertDir},
                        {modules, [mod_get]}], stand_alone),
    ok.

client_pid(Ctx, User, Password) ->
    CertDir = proplists:get_value(cert_dir, Ctx),
    Port = proplists:get_value(port, Ctx),
    riakc_pb_socket:start(
        "127.0.0.1", Port,
        [{credentials, iolist_to_binary(User), Password},
         {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])}]).
    
riak_admin(Ctx, Args) ->
    [Node|_] = proplists:get_value(cluster, Ctx),
    {ok,_Out} = Result = rt:admin(Node, Args),
    ct:pal("riak-admin ~s~n~s", [string:join(Args, " "), _Out]),
    Result.

%% From riak_core_security.erl
%% Avoid whitespace, control characters, comma, semi-colon,
%% non-standard Windows-only characters, other misc
-define(ILLEGAL, lists:seq(0, 44) ++ lists:seq(58, 63) ++
            lists:seq(127, 191)).

user_name() ->
    User = string:to_lower(base64:encode_to_string(term_to_binary(make_ref()))),
    [C || C <- User, not lists:member(C, ?ILLEGAL)].

%%--------------------------------------------------------------------
%% TESTS
%%--------------------------------------------------------------------

trusted_user_does_not_need_a_password_to_connect_test(Ctx) ->
    User = user_name(),
    NonsensePassword = "nonsense",
    {ok,_} = riak_admin(Ctx,
        ["security", "add-user", User]),
    {ok,_} = riak_admin(Ctx, 
        ["security", "add-source", "all", "127.0.0.1/32", "trust"]),
    {ok, _} = client_pid(Ctx, User, NonsensePassword).

password_user_cannot_connect_with_wrong_password_test(Ctx) ->
    User = "stranger",
    Password = "drongo",
    {ok,_} = riak_admin(Ctx,
        ["security", "add-user", User, "password=donk"]),
    {ok,_} = riak_admin(Ctx, 
        ["security", "add-source", "all", "127.0.0.1/32", "password"]),
    ?assertEqual(
        {error,{tcp,<<"Authentication failed">>}},
        client_pid(Ctx, User, Password)
    ).

with_security_user_cannot_create_table_without_permissions_test(Ctx) ->
    User = user_name(),
    UserBin = list_to_binary(User),
    Password = "password",
    {ok,_} = riak_admin(Ctx,
        ["security", "add-user", User]),
    {ok,_} = riak_admin(Ctx, 
        ["security", "add-source", "all", "127.0.0.1/32", "trust"]),
    {ok, Pid} = client_pid(Ctx, User, Password),
    %% just assert on the error message this once, to make sure it is getting
    %% formatted correctly.
    ?assertEqual(
        {error,
         <<"Permission denied: User '", UserBin/binary, "' does not have ",
         "'riak_ts.query_create_table' on table1">>},
        riakc_ts:query(Pid,
            "CREATE TABLE table1 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c))")
    ).

with_security_when_user_is_given_permissions_user_can_create_table_test(Ctx) ->
    User = user_name(),
    Password = "password",
    {ok,_} = riak_admin(Ctx,
        ["security", "add-user", User]),
    {ok,_} = riak_admin(Ctx, 
        ["security", "add-source", "all", "127.0.0.1/32", "trust"]),
    {ok,_} = riak_admin(Ctx, 
        ["security", "grant", "riak_ts.query_create_table", "on", "any", "to", User]),
    {ok, Pid} = client_pid(Ctx, User, Password),
    ?assertEqual(
        {ok, {[],[]}},
        riakc_ts:query(Pid,
            "CREATE TABLE table2 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c))")
    ).


with_security_user_cannot_put_without_permissions_test(Ctx) ->
    User = user_name(),
    Password = "password",
    {ok,_} = riak_admin(Ctx,
        ["security", "add-user", User]),
    {ok,_} = riak_admin(Ctx, 
        ["security", "add-source", "all", "127.0.0.1/32", "trust"]),
    {ok,_} = riak_admin(Ctx, 
        ["security", "grant", "riak_ts.query_create_table", "on", "any", "to", User]),
    {ok, Pid} = client_pid(Ctx, User, Password),
    ?assertEqual(
        {ok,{[],[]}},
        riakc_ts:query(Pid,
            "CREATE TABLE table3 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c))")
    ),
    ?assertMatch(
        {error, <<"Permission denied", _/binary>>},
        riakc_ts:put(Pid, <<"table3">>, [{1,1,1}])
    ).

with_security_when_user_is_given_permissions_user_can_put_data_test(Ctx) ->
    User = user_name(),
    Password = "password",
    {ok,_} = riak_admin(Ctx,
        ["security", "add-user", User]),
    {ok,_} = riak_admin(Ctx, 
        ["security", "add-source", "all", "127.0.0.1/32", "trust"]),
    {ok,_} = riak_admin(Ctx, 
        ["security", "grant", "riak_ts.query_create_table,riak_ts.put", "on", "any", "to", User]),
    {ok, Pid} = client_pid(Ctx, User, Password),
    ?assertEqual(
        {ok,{[],[]}},
        riakc_ts:query(Pid,
            "CREATE TABLE table4 ("
            "a SINT64 NOT NULL, "
            "b SINT64 NOT NULL, "
            "c TIMESTAMP NOT NULL, "
            "PRIMARY KEY  ((a,b,quantum(c, 1, 's')), a,b,c))")
    ),
    ?assertEqual(
        ok,
        riakc_ts:put(Pid, <<"table4">>, [{1,1,1}])
    ).


