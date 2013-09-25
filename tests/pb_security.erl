-module(pb_security).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

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

    %% trust 'user' on localhost
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
                                                   "default",  "hello", "TO", "user"]]),

    ?assertMatch({error, notfound}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:put(PB, 
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"howareyou">>))),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "ON",
                                                    "default", "hello", "TO", "user"]]),

    ?assertEqual(ok,
                 riakc_pb_socket:put(PB, 
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"1">>, "application/json"))),

    ?assertMatch({ok, _Obj}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

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
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "ON",
                                                    "ANY", "TO", "user"]]),

    ?assertMatch({ok, _Obj}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.get", "ON",
                                                    "ANY", "FROM", "user"]]),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    %% list keys
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:list_keys(PB, <<"hello">>)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "ON",
                                                    "default", "hello", "TO", "user"]]),

    ?assertMatch({ok, [<<"world">>]}, riakc_pb_socket:list_keys(PB, <<"hello">>)),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:list_buckets(PB)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_buckets", "ON",
                                                    "default", "TO", "user"]]),

    ?assertMatch({ok, [<<"hello">>]}, riakc_pb_socket:list_buckets(PB)),

    %% still need mapreduce permission
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:mapred_bucket(PB, <<"hello">>,
                                       [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                        {reduce, {jsfun,
                                                  <<"Riak.reduceSum">>},
                                         undefined, true}])),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.mapreduce", "ON",
                                                    "default", "TO", "user"]]),

    ?assertEqual({ok, [{1, [1]}]},
                 riakc_pb_socket:mapred_bucket(PB, <<"hello">>,
                                       [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                        {reduce, {jsfun,
                                                  <<"Riak.reduceSum">>},
                                         undefined, true}])),

    %% revoke only the list_keys permission
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
            ?assertMatch({error, <<"Permission", _/binary>>},
                         riakc_pb_socket:get_index(PB, <<"hello">>,
                                                   {binary_index,
                                                    "name"},
                                                   <<"John">>)),

            ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.index", "ON",
                                                            "default", "TO", "user"]]),

            %% don't actually have any indexes
            ?assertMatch({ok, {index_results_v1, [], _, _}},
                         riakc_pb_socket:get_index(PB, <<"hello">>,
                                                   {binary_index,
                                                    "name"},
                                                   <<"John">>)),
            ok
    end,

    %% get/set bprops
    
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:get_bucket(PB, <<"mybucket">>)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.get_bucket", "ON",
                                                    "default", "mybucket", "TO", "user"]]),

    ?assertEqual(3, proplists:get_value(n_val, element(2,
                                                       riakc_pb_socket:get_bucket(PB,
                                                                                  <<"mybucket">>)))),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:set_bucket(PB, <<"mybucket">>, [{n_val, 5}])),

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

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          {<<"mytype">>,
                                                                           <<"hello">>},
                                                                          <<"world">>)),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "ON",
                                                    "mytype", "hello", "TO", "user"]]),

    ?assertMatch({error, notfound}, riakc_pb_socket:get(PB, {<<"mytype">>,
                                                             <<"hello">>},
                                                         <<"world">>)),

    %% can't read from the default bucket, though
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:put(PB, 
                                     riakc_obj:new({<<"mytype">>, <<"hello">>}, <<"world">>,
                                                   <<"howareyou">>))),

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

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:list_keys(PB, {<<"mytype">>, <<"hello">>})),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "ON",
                                                    "mytype", "hello", "TO", "user"]]),

    ?assertEqual([<<"drnick">>, <<"world">>], lists:sort(element(2, riakc_pb_socket:list_keys(PB,
                                                                {<<"mytype">>,
                                                                 <<"hello">>})))),

    %% create a new type
    ok = rpc:call(Node, riak_core_bucket_type, create, [<<"mytype2">>,
                                                        [{allow_mult, true}]]),
    %% allow cluster metadata some time to propogate
    timer:sleep(1000),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          {<<"mytype2">>,
                                                                           <<"hello">>},
                                                                          <<"world">>)),
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

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:list_buckets(PB, <<"mytype2">>)),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_buckets", "ON",
                                                    "mytype2", "TO", "user"]]),

    ?assertMatch([<<"cromulent">>, <<"embiggen">>], lists:sort(element(2,
                                                                       riakc_pb_socket:list_buckets(PB,
                                                                                                   <<"mytype2">>)))),

    %% counters

    ?assertEqual({error, notfound}, riakc_pb_socket:counter_val(PB, {<<"mytype2">>, <<"hello">>},
                                                                <<"numberofpies">>)),
    ok = riakc_pb_socket:counter_incr(PB, {<<"mytype2">>, <<"hello">>},
                                 <<"numberofpies">>, 5),
    ?assertEqual({ok, 5}, riakc_pb_socket:counter_val(PB, {<<"mytype2">>, <<"hello">>},
                                                                <<"numberofpies">>)),
    %% revoke get
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.get", "ON", "mytype2", "FROM", "user"]]),

    ?assertMatch({error, <<"Permission",  _/binary>>}, riakc_pb_socket:counter_val(PB, {<<"mytype2">>, <<"hello">>},
                                                                <<"numberofpies">>)),
    ok = riakc_pb_socket:counter_incr(PB, {<<"mytype2">>, <<"hello">>},
                                      <<"numberofpies">>, 5),
    %% revoke get
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.put", "ON", "mytype2", "FROM", "user"]]),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:counter_incr(PB, {<<"mytype2">>, <<"hello">>},
                                                                                   <<"numberofpies">>, 5)),

    %% get/set bucket type props

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:set_bucket_type(PB, <<"mytype2">>,
                                                                                      [{n_val, 5}])),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:get_bucket_type(PB, <<"mytype">>)),

    ok = rpc:call(Node, riak_core_console, grant,
                  [["riak_core.get_bucket_type", "ON", "mytype2", "TO", "user"]]),

    ?assertEqual(3, proplists:get_value(n_val,
                                        element(2, riakc_pb_socket:get_bucket_type(PB,
                                                                        <<"mytype2">>)))),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:set_bucket_type(PB, <<"mytype2">>,
                                                                                      [{n_val, 5}])),

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

    %% create a new group
    ok = rpc:call(Node, riak_core_console, add_user, [["group"]]),

    %% create a new user in that group
    ok = rpc:call(Node, riak_core_console, add_user, [["myuser", "roles=group"]]),


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

