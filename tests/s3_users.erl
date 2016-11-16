-module(s3_users).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

confirm() ->
    Cluster = [Node| _ ] = build_cluster(2),
    enable_security(Node),
    %% verify_create_user(Cluster),
    verify_upgrade_user(Cluster),
    pass.

verify_upgrade_user([Node|_]) ->
    Username = "username",
    DisplayName = "displayname",
    ok = rpc:call(Node, riak_core_security, add_user, [Username, []]),
    {ok, {AccessKey, SecretKey}} = rpc:call(Node,
                                            riak_s3_user_riak_security_backend,
                                            enable_s3_user, [Username, DisplayName]),
    UsernameBinary = list_to_binary(Username),
    {UsernameBinary, Options} = rpc:call(Node, riak_core_security, get_user, [Username]),

    ?assertEqual(proplists:get_value("s3.DisplayName", Options), DisplayName),
    ?assertEqual(proplists:get_value("s3.AccessKeyId", Options), AccessKey),
    ?assertEqual(proplists:get_value("s3.SecretAccessKey", Options), SecretKey).

build_cluster(N) ->
    rt:build_cluster(lists:duplicate(N, {current, []})).

enable_security(Node) ->
    ok = rpc:call(Node, riak_core_console, security_enable, [[]]).
