-module(pb_security).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

confirm() ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
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

    MD = riak_test_runner:metadata(),
    HaveIndexes = case proplists:get_value(backend, MD) of
                      undefined -> false; %% default is da 'cask
                      bitcask -> false;
                      _ -> true
                  end,

    Nodes = rt:build_cluster(4, Conf),
    Node = hd(Nodes),

    [_, {pb, {"127.0.0.1", Port}}] = rt:connection_info(Node),

    lager:info("Checking non-SSL results in error"),
    %% can connect without credentials, but not do anything
    {ok, PB0} =  riakc_pb_socket:start("127.0.0.1", Port,
                                       []),
    ?assertEqual({error, <<"Security is enabled, please STARTTLS first">>},
                 riakc_pb_socket:ping(PB0)),

    riakc_pb_socket:stop(PB0),

    lager:info("Checking SSL requires peer cert validation"),
    %% can't connect without specifying cacert to validate the server
    ?assertMatch({error, _}, riakc_pb_socket:start("127.0.0.1", Port,
                                                   [{credentials, "user",
                                                     "pass"}])),

    lager:info("Checking that authentication is required"),
    %% invalid credentials should be invalid
    ?assertEqual({error, {tcp, <<"Authentication failed">>}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "user",
                                        "pass"}, {cacertfile,
                                                  filename:join([PrivDir,
                                                                 "certs/selfsigned/ca/rootcert.pem"])}])),

    lager:info("Creating user"),
    %% grant the user credentials
    ok = rpc:call(Node, riak_core_console, add_user, [["user", "password=password"]]),

    lager:info("Setting trust mode on user"),
    %% trust 'user' on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user", "127.0.0.1/32",
                                                    "trust"]]),

    lager:info("Checking that credentials are ignored in trust mode"),
    %% invalid credentials should be ignored in trust mode
    {ok, PB1} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "user",
                                        "pass"}, {cacertfile,
                                                  filename:join([PrivDir,
                                                                 "certs/selfsigned/ca/rootcert.pem"])}]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB1)),
    riakc_pb_socket:stop(PB1),

    lager:info("Setting password mode on user"),
    %% require password on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user", "127.0.0.1/32",
                                                    "password"]]),

    lager:info("Checking that incorrect password fails auth"),
    %% invalid credentials should be invalid
    ?assertEqual({error, {tcp, <<"Authentication failed">>}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "user",
                                        "pass"}, {cacertfile,
                                                  filename:join([PrivDir,
                                                                 "certs/selfsigned/ca/rootcert.pem"])}])),

    lager:info("Checking that correct password is successful"),
    %% valid credentials should be valid
    {ok, PB2} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "user",
                                        "password"}, {cacertfile,
                                                  filename:join([PrivDir,
                                                                 "certs/selfsigned/ca/rootcert.pem"])}]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB2)),
    riakc_pb_socket:stop(PB2),

    lager:info("Creating a certificate-authenticated user"),
    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site4.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site4.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    lager:info("Checking certificate authentication"),
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

    lager:info("Creating another cert-auth user"),
    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site5.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site5.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    lager:info("Checking auth with mismatched user/cert fails"),
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

    lager:info("Checking auth with non-peer certificate fails"),
    %% authing with non-peer certificate should fail
    ?assertEqual({error, {tcp, {tls_alert, "unknown ca"}}}, riakc_pb_socket:start("127.0.0.1", Port,
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

    lager:info("verifying that user cannot get/put without grants"),
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    lager:info("Granting riak_kv.get, checking get works but put doesn't"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "ON",
                                                   "default",  "hello", "TO", "user"]]),

    ?assertMatch({error, notfound}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"howareyou">>))),

    lager:info("Granting riak_kv.put, checking put works and roundtrips with get"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "ON",
                                                    "default", "hello", "TO", "user"]]),

    ?assertEqual(ok,
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"1">>, "application/json"))),

    ?assertMatch({ok, _Obj}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

    lager:info("Revoking get/put, checking that get/put are disallowed"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.get,riak_kv.put", "ON",
                                                    "default", "hello", "FROM", "user"]]),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"howareyou">>))),

    %% try the 'any' grant
    lager:info("Granting get on ANY, checking user can fetch any bucket/key"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "ON",
                                                    "ANY", "TO", "user"]]),

    ?assertMatch({ok, _Obj}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

    lager:info("Revoking ANY permission, checking fetch fails"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.get", "ON",
                                                    "ANY", "FROM", "user"]]),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    %% list keys
    lager:info("Checking that list keys is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:list_keys(PB, <<"hello">>)),

    lager:info("Granting riak_kv.list_keys, checking that list_keys succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "ON",
                                                    "default", "hello", "TO", "user"]]),

    ?assertMatch({ok, [<<"world">>]}, riakc_pb_socket:list_keys(PB, <<"hello">>)),

    lager:info("Checking that list buckets is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:list_buckets(PB)),

    lager:info("Granting riak_kv.list_buckets, checking that list_buckets succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_buckets", "ON",
                                                    "default", "TO", "user"]]),

    ?assertMatch({ok, [<<"hello">>]}, riakc_pb_socket:list_buckets(PB)),

    %% still need mapreduce permission
    lager:info("Checking that full-bucket mapred is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:mapred_bucket(PB, <<"hello">>,
                                       [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                        {reduce, {jsfun,
                                                  <<"Riak.reduceSum">>},
                                         undefined, true}])),

    lager:info("Granting mapreduce, checking that job succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.mapreduce", "ON",
                                                    "default", "TO", "user"]]),

    ?assertEqual({ok, [{1, [1]}]},
                 riakc_pb_socket:mapred_bucket(PB, <<"hello">>,
                                       [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                        {reduce, {jsfun,
                                                  <<"Riak.reduceSum">>},
                                         undefined, true}])),

    %% revoke only the list_keys permission
    lager:info("Revoking list-keys, checking that full-bucket mapred fails"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.list_keys", "ON",
                                                    "default", "hello", "FROM", "user"]]),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:mapred_bucket(PB, <<"hello">>,
                                       [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                        {reduce, {jsfun,
                                                  <<"Riak.reduceSum">>},
                                         undefined, true}])),

    case HaveIndexes of
        false -> ok;
        true ->
            %% 2i permission test
            lager:info("Checking 2i is disallowed"),
            ?assertMatch({error, <<"Permission", _/binary>>},
                         riakc_pb_socket:get_index(PB, <<"hello">>,
                                                   {binary_index,
                                                    "name"},
                                                   <<"John">>)),

            lager:info("Granting 2i permissions, checking that results come back"),
            ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.index", "ON",
                                                            "default", "TO", "user"]]),

            %% don't actually have any indexes
            ?assertMatch({ok, ?INDEX_RESULTS{keys=[]}},
                         riakc_pb_socket:get_index(PB, <<"hello">>,
                                                   {binary_index,
                                                    "name"},
                                                   <<"John">>)),
            ok
    end,

    %% get/set bprops
     lager:info("Checking that get_bucket is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:get_bucket(PB, <<"mybucket">>)),

    lager:info("Granting riak_core.get_bucket, checking that get_bucket succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.get_bucket", "ON",
                                                    "default", "mybucket", "TO", "user"]]),

    ?assertEqual(3, proplists:get_value(n_val, element(2,
                                                       riakc_pb_socket:get_bucket(PB,
                                                                                  <<"mybucket">>)))),

    lager:info("Checking that set_bucket is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:set_bucket(PB, <<"mybucket">>, [{n_val, 5}])),

    lager:info("Granting set_bucket, checking that set_bucket succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.set_bucket", "ON",
                                                    "default", "mybucket", "TO", "user"]]),
    ?assertEqual(ok,
                 riakc_pb_socket:set_bucket(PB, <<"mybucket">>, [{n_val, 5}])),

    ?assertEqual(5, proplists:get_value(n_val, element(2,
                                                       riakc_pb_socket:get_bucket(PB,
                                                                                  <<"mybucket">>)))),

    %%%%%%%%%%%%
    %%% bucket type tests
    %%%%%%%%%%%%

    %% create a new type
    ok = rpc:call(Node, riak_core_bucket_type, create, [<<"mytype">>, [{n_val,
                                                                        3}]]),
    %% allow cluster metadata some time to propogate
    timer:sleep(1000),

    lager:info("Checking that get on a new bucket type is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          {<<"mytype">>,
                                                                           <<"hello">>},
                                                                          <<"world">>)),

    lager:info("Granting get on the new bucket type, checking that it succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "ON",
                                                    "mytype", "hello", "TO", "user"]]),

    ?assertMatch({error, notfound}, riakc_pb_socket:get(PB, {<<"mytype">>,
                                                             <<"hello">>},
                                                         <<"world">>)),

    lager:info("Checking that permisisons are unchanged on the default bucket type"),
    %% can't read from the default bucket, though
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    lager:info("Checking that put on the new bucket type is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new({<<"mytype">>, <<"hello">>}, <<"world">>,
                                                   <<"howareyou">>))),

    lager:info("Granting put on a bucket in the new bucket type, checking that it succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "ON",
                                                    "mytype", "hello", "TO", "user"]]),

    ?assertEqual(ok,
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new({<<"mytype">>, <<"hello">>}, <<"world">>,
                                                   <<"howareyou">>))),

    ?assertEqual(ok,
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new({<<"mytype">>,
                                                    <<"hello">>}, <<"drnick">>,
                                                   <<"Hi, everybody">>))),

    ?assertMatch({ok, _Obj}, riakc_pb_socket:get(PB, {<<"mytype">>,
                                                      <<"hello">>},
                                                         <<"world">>)),

    lager:info("Revoking get/put on the new bucket type, checking that they fail"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.get,riak_kv.put", "ON",
                                                    "mytype", "hello", "FROM", "user"]]),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          {<<"mytype">>,
                                                                           <<"hello">>},
                                                                          <<"world">>)),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new({<<"mytype">>, <<"hello">>}, <<"world">>,
                                                   <<"howareyou">>))),

    lager:info("Checking that list keys is disallowed on the new bucket type"),
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:list_keys(PB, {<<"mytype">>, <<"hello">>})),

    lager:info("Granting list keys on a bucket in the new type, checking that it works"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "ON",
                                                    "mytype", "hello", "TO", "user"]]),

    ?assertEqual([<<"drnick">>, <<"world">>], lists:sort(element(2, riakc_pb_socket:list_keys(PB,
                                                                {<<"mytype">>,
                                                                 <<"hello">>})))),

    lager:info("Creating another bucket type"),
    %% create a new type
    ok = rpc:call(Node, riak_core_bucket_type, create, [<<"mytype2">>,
                                                        [{allow_mult, true}]]),
    %% allow cluster metadata some time to propogate
    timer:sleep(1000),

    lager:info("Checking that get on the new type is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          {<<"mytype2">>,
                                                                           <<"hello">>},
                                                                          <<"world">>)),

    lager:info("Granting get/put on all buckets in the new type, checking that get/put works"),
    %% do a wildcard grant
    ok = rpc:call(Node, riak_core_console, grant,
                  [["riak_kv.get,riak_kv.put", "ON",
                                                    "mytype2", "TO", "user"]]),

    ?assertMatch({error, notfound}, riakc_pb_socket:get(PB, {<<"mytype2">>,
                                                             <<"hello">>},
                                                         <<"world">>)),

    ?assertEqual(ok,
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new({<<"mytype2">>,
                                                    <<"embiggen">>},
                                                   <<"the noblest heart">>,
                                                   <<"true">>))),

    ?assertEqual(ok,
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new({<<"mytype2">>,
                                                    <<"cromulent">>},
                                                   <<"perfectly">>,
                                                   <<"true">>))),

    lager:info("Checking that list buckets is disallowed on the new type"),
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:list_buckets(PB, <<"mytype2">>)),

    lager:info("Granting list buckets on the new type, checking that it succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_buckets", "ON",
                                                    "mytype2", "TO", "user"]]),

    ?assertMatch([<<"cromulent">>, <<"embiggen">>], lists:sort(element(2,
                                                                       riakc_pb_socket:list_buckets(PB,
                                                                                                   <<"mytype2">>)))),

    %% counters

    lager:info("Checking that counters work on resources that have get/put permitted"),
    ?assertEqual({error, notfound}, riakc_pb_socket:counter_val(PB, {<<"mytype2">>, <<"hello">>},
                                                                <<"numberofpies">>)),
    ok = riakc_pb_socket:counter_incr(PB, {<<"mytype2">>, <<"hello">>},
                                 <<"numberofpies">>, 5),
    ?assertEqual({ok, 5}, riakc_pb_socket:counter_val(PB, {<<"mytype2">>, <<"hello">>},
                                                      <<"numberofpies">>)),

    lager:info("Revoking get, checking that counter_val fails"),
    %% revoke get
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.get", "ON", "mytype2", "FROM", "user"]]),

    ?assertMatch({error, <<"Permission",  _/binary>>}, riakc_pb_socket:counter_val(PB, {<<"mytype2">>, <<"hello">>},
                                                                <<"numberofpies">>)),
    ok = riakc_pb_socket:counter_incr(PB, {<<"mytype2">>, <<"hello">>},
                                      <<"numberofpies">>, 5),

    lager:info("Revoking put, checking that counter_incr fails"),
    %% revoke put
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.put", "ON", "mytype2", "FROM", "user"]]),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:counter_incr(PB, {<<"mytype2">>, <<"hello">>},
                                                                                   <<"numberofpies">>, 5)),

    %% get/set bucket type props

    lager:info("Checking that get/set bucket-type properties are disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:set_bucket_type(PB, <<"mytype2">>,
                                                                                      [{n_val, 5}])),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:get_bucket_type(PB, <<"mytype">>)),

    lager:info("Granting get on bucket type props, checking it succeeds and put still fails"),
    ok = rpc:call(Node, riak_core_console, grant,
                  [["riak_core.get_bucket_type", "ON", "mytype2", "TO", "user"]]),

    ?assertEqual(3, proplists:get_value(n_val,
                                        element(2, riakc_pb_socket:get_bucket_type(PB,
                                                                        <<"mytype2">>)))),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:set_bucket_type(PB, <<"mytype2">>,
                                                                                      [{n_val, 5}])),

    lager:info("Granting set on bucket type props, checking it succeeds"),
    ok = rpc:call(Node, riak_core_console, grant,
                  [["riak_core.set_bucket_type", "ON", "mytype2", "TO", "user"]]),

    riakc_pb_socket:set_bucket_type(PB, <<"mytype2">>, [{n_val, 5}]),

    ?assertEqual(5, proplists:get_value(n_val,
                                        element(2, riakc_pb_socket:get_bucket_type(PB,
                                                                        <<"mytype2">>)))),

    riakc_pb_socket:stop(PB),

    group_test(Node, Port, PrivDir).

group_test(Node, Port, PrivDir) ->
    %%%%%%%%%%%%%%%%
    %% test groups
    %%%%%%%%%%%%%%%%

    lager:info("Creating a new group"),
    %% create a new group
    ok = rpc:call(Node, riak_core_console, add_user, [["group"]]),

    lager:info("Creating a user in the group"),
    %% create a new user in that group
    ok = rpc:call(Node, riak_core_console, add_user, [["myuser", "roles=group"]]),


    lager:info("Granting get/put/delete on a bucket type to the group, checking those requests work"),
    %% do a wildcard grant
    ok = rpc:call(Node, riak_core_console, grant,
                  [["riak_kv.get,riak_kv.put,riak_kv.delete", "ON",
                                                    "mytype2", "TO", "group"]]),

    %% trust 'myuser' on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["myuser", "127.0.0.1/32",
                                                    "trust"]]),

    {ok, PB} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "myuser",
                                        "password"}, {cacertfile,
                                                  filename:join([PrivDir,
                                                                 "certs/selfsigned/ca/rootcert.pem"])}]),

    ?assertMatch({error, notfound}, riakc_pb_socket:get(PB, {<<"mytype2">>,
                                                             <<"hello">>},
                                                         <<"world">>)),
    ?assertEqual(ok,
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new({<<"mytype2">>, <<"hello">>}, <<"world">>,
                                                   <<"howareyou">>))),

    {ok, Obj} = riakc_pb_socket:get(PB, {<<"mytype2">>,
                                                      <<"hello">>},
                                                         <<"world">>),
    riakc_pb_socket:delete_obj(PB, Obj),

    ?assertMatch({error, notfound}, riakc_pb_socket:get(PB, {<<"mytype2">>,
                                                             <<"hello">>},
                                                         <<"world">>)),

    riakc_pb_socket:stop(PB),
    ok.

