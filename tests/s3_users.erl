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

verify_create_user([Node1, _Node2|_]) ->
    Username = "username",
    DisplayName = "displayname",
    User = rpc:call(Node1,
                    riak_s3_user,
                    create,
                    [Username, DisplayName]),
    AccessKey = rpc:call(Node1, riak_s3_user, get_access_key_id, [User]),
    io:format("User: ~p AccessKey: ~p~n", [User, AccessKey]),
    rt:wait_until(
      fun() ->
              User2 = rpc:call(Node1, riak_s3_user, find_by_access_key, [AccessKey]),
              % TODO: name vs username, pick one
              User2 =:= User
      end).

verify_grant_user([Node1, Node2|_]) ->
    Username = "jorge",
    DisplayName = "displayname",
    ok = rpc:call(Node1, riak_core_security, add_user, [Username, []]),
    {ok, {AccessKey, SecretKey}} = rpc:call(Node1,
                                            riak_s3_user_riak_security_backend,
                                            grant_s3_to_user, [Username, DisplayName]),
    rt:wait_until(
      fun() ->
              {Username, Options} = rpc:call(Node2, riak_core_security, get_user, [Username]),

              DisplayName =:= proplists:get_value("s3.DisplayName", Options) andalso
              AccessKey =:= proplists:get_value("s3.AccessKeyId", Options) andalso
              SecretKey =:= proplists:get_value("s3.SecretAccessKey", Options)
      end).

build_cluster(N) ->
    rt:build_cluster(lists:duplicate(N, {current, []})).

enable_security(Node) ->
    ok = rpc:call(Node, riak_core_console, security_enable, [[]]).
