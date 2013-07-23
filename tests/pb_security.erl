-module(pb_security).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    ok = application:start(crypto),
    ok = application:start(asn1),
    ok = application:start(public_key),
    ok = application:start(ssl),
    lager:info("Deploy some nodes"),
    PrivDir = rt:priv_dir(),
    Conf = [
            {riak_api, [
                    {certfile, filename:join([PrivDir,
                                              "certs/selfsigned/site3-cert.pem"])},
                    {keyfile, filename:join([PrivDir,
                                             "certs/selfsigned/site3-key.pem"])},
                    {cacertfile, filename:join([PrivDir,
                                               "certs/selfsigned/ca/rootcert.pem"])}
                    ]},
            {riak_core, [
                    {security, true}
                    ]}
    ],

    Nodes = rt:build_cluster(4, Conf),
    Node = hd(Nodes),

    {ok, [{"127.0.0.1", Port}]} = rpc:call(Node, application, get_env,
                                 [riak_api, pb]),

    %% can connect without credentials, but not do anything
    {ok, PB0} =  riakc_pb_socket:start("127.0.0.1", Port,
                                       []),
    ?assertEqual({error, <<"Security is enabled, please STARTTLS first">>},
                 riakc_pb_socket:ping(PB0)),

    riakc_pb_socket:stop(PB0),

    %% can't connect without specifying cacert to validate the server
    ?assertMatch({error, _}, riakc_pb_socket:start("127.0.0.1", Port,
                                                   [{credentials, "user",
                                                     "pass"}])),

    %% invalid credentials should be invalid
    ?assertEqual({error, {tcp, <<"Authentication failed">>}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "user",
                                        "pass"}, {cacertfile,
                                                  filename:join([PrivDir,
                                                                 "certs/selfsigned/ca/rootcert.pem"])}])),

    %% grant the user credentials
    ok = rpc:call(Node, riak_core_console, add_user, [["user", "password=password"]]),

    %% trust anyone on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user", "127.0.0.1/32",
                                                    "trust"]]),

    %% invalid credentials should be ignored in trust mode
    {ok, PB1} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "user",
                                        "pass"}, {cacertfile,
                                                  filename:join([PrivDir,
                                                                 "certs/selfsigned/ca/rootcert.pem"])}]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB1)),
    riakc_pb_socket:stop(PB1),
 
    %% require password on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user", "127.0.0.1/32",
                                                    "password"]]),

    %% invalid credentials should be invalid
    ?assertEqual({error, {tcp, <<"Authentication failed">>}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "user",
                                        "pass"}, {cacertfile,
                                                  filename:join([PrivDir,
                                                                 "certs/selfsigned/ca/rootcert.pem"])}])),

    %% valid credentials should be valid
    {ok, PB2} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "user",
                                        "password"}, {cacertfile,
                                                  filename:join([PrivDir,
                                                                 "certs/selfsigned/ca/rootcert.pem"])}]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB2)),
    riakc_pb_socket:stop(PB2),

    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site4.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site4.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    %% valid credentials should be valid
    {ok, PB3} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "site4.basho.com",
                                        "password"},
                                       {cacertfile, filename:join([PrivDir,
                                                                   "certs/selfsigned/ca/rootcert.pem"])},
                                       {certfile, filename:join([PrivDir,
                                                                   "certs/selfsigned/site4-cert.pem"])},
                                       {keyfile, filename:join([PrivDir,
                                                                   "certs/selfsigned/site4-key.pem"])}
                                      ]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB3)),
    riakc_pb_socket:stop(PB3),

    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site5.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site5.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    %% authing with mismatched user should fail
    ?assertEqual({error, {tcp, <<"Authentication failed">>}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "site5.basho.com",
                                        "password"},
                                       {cacertfile, filename:join([PrivDir,
                                                                   "certs/selfsigned/ca/rootcert.pem"])},
                                       {certfile, filename:join([PrivDir,
                                                                   "certs/selfsigned/site4-cert.pem"])},
                                       {keyfile, filename:join([PrivDir,
                                                                   "certs/selfsigned/site4-key.pem"])}
                                      ])),

    %% authing with non-peer certificate should fail
    ?assertEqual({error, {tcp, closed}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "site5.basho.com",
                                        "password"},
                                       {cacertfile, filename:join([PrivDir,
                                                                   "certs/selfsigned/ca/rootcert.pem"])},
                                       {certfile, filename:join([PrivDir,
                                                                   "certs/cacert.org/ca-cert.pem"])},
                                       {keyfile, filename:join([PrivDir,
                                                                   "certs/cacert.org/ca-key.pem"])}
                                      ])),


    %% time to actually do some stuff
    {ok, PB} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "user",
                                        "password"}, {cacertfile,
                                                  filename:join([PrivDir,
                                                                 "certs/selfsigned/ca/rootcert.pem"])}]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB)),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "ON",
                                                    "hello", "TO", "user"]]),

    ?assertMatch({error, notfound}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:put(PB, 
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"howareyou">>))),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "ON",
                                                    "hello", "TO", "user"]]),

    ?assertEqual(ok,
                 riakc_pb_socket:put(PB, 
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"howareyou">>))),

    ?assertMatch({ok, _Obj}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.get,riak_kv.put", "ON",
                                                    "hello", "FROM", "user"]]),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:put(PB, 
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"howareyou">>))),

    riakc_pb_socket:stop(PB),
    ok.

