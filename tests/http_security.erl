-module(http_security).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(ibrowse),
    io:format("turning on tracing"),
    ibrowse:trace_on(),

    lager:info("Deploy some nodes"),
    PrivDir = rt:priv_dir(),
    Conf = [
            {riak_core, [
                         {default_bucket_props, [{allow_mult, true}]},
                    {ssl, [
                            {certfile, filename:join([PrivDir,
                                                      "certs/selfsigned/site3-cert.pem"])},
                            {keyfile, filename:join([PrivDir,
                                                     "certs/selfsigned/site3-key.pem"])}
                            ]},
                    {security, true}
                    ]}
    ],
    Nodes = rt:build_cluster(4, Conf),
    Node = hd(Nodes),
    enable_ssl(Node),
    %[enable_ssl(N) || N <- Nodes],
    {ok, [{"127.0.0.1", Port0}]} = rpc:call(Node, application, get_env,
                                 [riak_core, http]),
    {ok, [{"127.0.0.1", Port}]} = rpc:call(Node, application, get_env,
                                 [riak_core, https]),

    MD = riak_test_runner:metadata(),
    _HaveIndexes = case proplists:get_value(backend, MD) of
                      undefined -> false; %% default is da 'cask
                      bitcask -> false;
                      _ -> true
                  end,

    %% connections over regular HTTP get told to go elsewhere
    C0 = rhc:create("127.0.0.1", Port0, "riak", []),
    ?assertMatch({error, {ok, "426", _, _}}, rhc:ping(C0)),

    C1 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C1)),

    C2 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "pass"}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C2)),

    %% grant the user credentials
    ok = rpc:call(Node, riak_core_console, add_user, [["user", "password=password"]]),

    %% trust anyone on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user",
                                                         "127.0.0.1/32",
                                                         "trust"]]),

    %% invalid credentials should be ignored in trust mode
    C3 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "pass"}]),
    ?assertEqual(ok, rhc:ping(C3)),

    %% require password on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user",
                                                         "127.0.0.1/32",
                                                         "password"]]),

    %% invalid credentials should be rejected in password mode
    C4 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "pass"}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C4)),

    %% valid credentials should be accepted in password mode
    C5 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "password"}]),

    ?assertEqual(ok, rhc:ping(C5)),

    %% verifying the peer certificate reject mismatch with server cert
    C6 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true},
                                                {credentials, "user", "password"},
                                                {ssl_options, [
                        {cacertfile, filename:join([PrivDir,
                                                    "certs/cacert.org/ca/root.crt"])},
                        {verify, verify_peer},
                        {reuse_sessions, false}
                        ]}
                                               ]),

    ?assertEqual({error,{conn_failed,{error,"unknown ca"}}}, rhc:ping(C6)),

    %% verifying the peer certificate should work if the cert is valid
    C7 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true},
                                                {credentials, "user", "password"},
                                                {ssl_options, [
                        {cacertfile, filename:join([PrivDir,
                                                    "certs/selfsigned/ca/rootcert.pem"])},
                        {verify, verify_peer},
                        {reuse_sessions, false}
                        ]}
                                               ]),

    ?assertEqual(ok, rhc:ping(C7)),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    Object = riakc_obj:new(<<"hello">>, <<"world">>, <<"howareyou">>,
                           <<"text/plain">>),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:put(C7, Object)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "ON",
                                                    "default", "hello", "TO", "user"]]),

    %% key is not present
    ?assertMatch({error, notfound}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:put(C7, Object)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "ON",
                                                    "default", "hello", "TO", "user"]]),

    %% NOW we can put
    ?assertEqual(ok, rhc:put(C7, Object)),

    {ok, O} = rhc:get(C7, <<"hello">>, <<"world">>),
    ?assertEqual(<<"hello">>, riakc_obj:bucket(O)),
    ?assertEqual(<<"world">>, riakc_obj:key(O)),
    ?assertEqual(<<"howareyou">>, riakc_obj:get_value(O)),

    %% delete
    ?assertMatch({error, {ok, "403", _, _}}, rhc:delete(C7, <<"hello">>,
                                                        <<"world">>)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.delete", "ON",
                                                    "default", "hello", "TO", "user"]]),
    ?assertEqual(ok, rhc:delete(C7, <<"hello">>,
                                <<"world">>)),

    %% key is deleted
    ?assertMatch({error, notfound}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    %% slam the door in the user's face
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.put,riak_kv.get,riak_kv.delete", "ON",
                    "default", "hello", "FROM", "user"]]),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:put(C7, Object)),

    %% list buckets
    lager:info("listing buckets"),
    ?assertMatch({error, {"403", _}}, rhc:list_buckets(C7)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_buckets", "ON",
                                                    "default", "TO", "user"]]),
    ?assertMatch({ok, [<<"hello">>]}, rhc:list_buckets(C7)),

    %% list keys
    ?assertMatch({error, {"403", _}}, rhc:list_keys(C7, <<"hello">>)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "ON",
                                                    "default", "TO", "user"]]),

    ?assertMatch({ok, [<<"world">>]}, rhc:list_keys(C7, <<"hello">>)),

    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.list_keys", "ON",
                                                    "default", "FROM", "user"]]),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:get_bucket(C7, <<"hello">>)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.get_bucket", "ON",
                                                    "default", "hello", "TO", "user"]]),

    ?assertEqual(3, proplists:get_value(n_val, element(2, rhc:get_bucket(C7,
                                                                         <<"hello">>)))),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:set_bucket(C7, <<"hello">>,
                                                            [{n_val, 5}])),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.set_bucket", "ON",
                                                    "default", "hello", "TO", "user"]]),

    ?assertEqual(ok, rhc:set_bucket(C7, <<"hello">>,
                                    [{n_val, 5}])),

    ?assertEqual(5, proplists:get_value(n_val, element(2, rhc:get_bucket(C7,
                                                                         <<"hello">>)))),

    %% counters

    %% grant get/put again
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get,riak_kv.put", "ON",
                                                    "default", "hello", "TO", "user"]]),


    ?assertMatch({error, {ok, "404", _, _}}, rhc:counter_val(C7, <<"hello">>,
                                                    <<"numberofpies">>)),

    ok = rhc:counter_incr(C7, <<"hello">>,
                          <<"numberofpies">>, 5),

    ?assertEqual({ok, 5}, rhc:counter_val(C7, <<"hello">>,
                                          <<"numberofpies">>)),

    %% revoke get
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.get", "ON", "default", "hello", "FROM", "user"]]),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:counter_val(C7, <<"hello">>,
                                          <<"numberofpies">>)),
    ok = rhc:counter_incr(C7, <<"hello">>,
                          <<"numberofpies">>, 5),

    %% revoke put
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.put", "ON", "default", "hello", "FROM", "user"]]),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:counter_incr(C7, <<"hello">>,
                          <<"numberofpies">>, 5)),

    %% mapred tests

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "ON",
                                                    "default", "MR", "TO", "user"]]),


    ok = rhc:put(C7, riakc_obj:new(<<"MR">>, <<"lobster_roll">>, <<"16">>,
                           <<"text/plain">>)),

    ok = rhc:put(C7, riakc_obj:new(<<"MR">>, <<"pickle_plate">>, <<"9">>,
                           <<"text/plain">>)),

    ok = rhc:put(C7, riakc_obj:new(<<"MR">>, <<"pimms_cup">>, <<"8">>,
                           <<"text/plain">>)),

    ?assertMatch({error, {"403", _}},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {jsfun,
                                                     <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun,
                                                        <<"Riak.reduceSum">>}, undefined,
                                               true}])),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "ON",
                                                    "default", "MR", "TO", "user"]]),

    ?assertMatch({error, {"403", _}},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {jsfun,
                                                     <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun,
                                                        <<"Riak.reduceSum">>}, undefined,
                                               true}])),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.mapreduce", "ON",
                                                    "default", "MR", "TO", "user"]]),

    ?assertEqual({ok, [{1, [33]}]},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {jsfun,
                                                     <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun,
                                                        <<"Riak.reduceSum">>}, undefined,
                                               true}])),

    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.list_keys", "ON",
                                                    "default", "MR", "FROM", "user"]]),

    ?assertMatch({error, {"403", _}},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {jsfun,
                                                     <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun,
                                                        <<"Riak.reduceSum">>}, undefined,
                                               true}])),
    ok.

enable_ssl(Node) ->
    {ok, [{"127.0.0.1", Port}]} = rpc:call(Node, application, get_env,
                                 [riak_core, http]),
    rt:update_app_config(Node, [{riak_core, [{https, [{"127.0.0.1",
                                                     Port+1000}]}]}]),
    rt:wait_until_pingable(Node),
    rt:wait_for_service(Node, riak_kv).



