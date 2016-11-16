-module(s3_users).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

confirm() ->
    Cluster = [Node| _ ] = build_cluster(2),
    enable_security(Node),
    verify_create_user(Cluster),
    verify_grant_user(Cluster),
    pass.

verify_create_user([Node|_]) ->
    Username = "username",
    DisplayName = "displayname",
    User = rpc:call(Node,
                           riak_s3_user,
                           create,
                           [Username, DisplayName]),
    {Username, Options} = rpc:call(Node, riak_core_security, get_user, [Username]),

    io:format("User: ~p", [User]),
    io:format("Options: ~p", [Options]),
    ?assertEqual(DisplayName, proplists:get_value("s3.DisplayName", Options)).

verify_grant_user([Node|_]) ->
    Username = "jorge",
    DisplayName = "displayname",
    ok = rpc:call(Node, riak_core_security, add_user, [Username, []]),
    {ok, {AccessKey, SecretKey}} = rpc:call(Node,
                                            riak_s3_user_riak_security_backend,
                                            grant_s3_to_user, [Username, DisplayName]),
    {Username, Options} = rpc:call(Node, riak_core_security, get_user, [Username]),

    ?assertEqual(DisplayName, proplists:get_value("s3.DisplayName", Options)),
    ?assertEqual(AccessKey, proplists:get_value("s3.AccessKeyId", Options)),
    ?assertEqual(SecretKey, proplists:get_value("s3.SecretAccessKey", Options)).

build_cluster(N) ->
    rt:build_cluster(lists:duplicate(N, {current, []})).

enable_security(Node) ->
    ok = rpc:call(Node, riak_core_console, security_enable, [[]]).
