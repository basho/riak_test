-module(pb_security).

-behavior(riak_test).
-export([confirm/0]).

-export([map_object_value/3, reduce_set_union/2, mapred_modfun_input/3]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(assertDenied(Op), ?assertMatch({error, <<"Permission",_/binary>>}, Op)).

confirm() ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(inets),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/pb_security_certs",

    %% make a bunch of crypto keys
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

    %% start a HTTP server to serve the CRLs
    %%
    %% NB: we use the 'stand_alone' option to link the server to the
    %% test process, so it exits when the test process exits.
    {ok, _HTTPPid} = inets:start(httpd, [{port, 8000}, {server_name, "localhost"},
                        {server_root, "/tmp"},
                        {document_root, CertDir},
                        {modules, [mod_get]}], stand_alone),

    lager:info("Deploy some nodes"),
    PrivDir = rt:priv_dir(),
    Conf = [
            {riak_core, [
                {default_bucket_props, [{allow_mult, true}]},
                {ssl, [
                    {certfile, filename:join([CertDir,"site3.basho.com/cert.pem"])},
                    {keyfile, filename:join([CertDir, "site3.basho.com/key.pem"])},
                    {cacertfile, filename:join([CertDir, "site3.basho.com/cacerts.pem"])}
                    ]}
                ]},
            {riak_search, [
                           {enabled, true}
                          ]}
           ],

    MD = riak_test_runner:metadata(),
    HaveIndexes = case proplists:get_value(backend, MD) of
                      undefined -> false; %% default is da 'cask
                      bitcask -> false;
                      _ -> true
                  end,

    Nodes = rt_cluster:build_cluster(4, Conf),
    Node = hd(Nodes),
    %% enable security on the cluster
    ok = rpc:call(Node, riak_core_console, security_enable, [[]]),

    [_, {pb, {"127.0.0.1", Port}}] = rt:connection_info(Node),

    lager:info("Checking non-SSL results in error"),
    %% can connect without credentials, but not do anything
    {ok, PB0} =  riakc_pb_socket:start("127.0.0.1", Port,
                                       []),
    ?assertEqual({error, <<"Security is enabled, please STARTTLS first">>},
                 riakc_pb_socket:ping(PB0)),

    riakc_pb_socket:stop(PB0),

    %% Hindi in Devanagari : हिन्दी
    Username = [2361,2367,2344,2381,2342,2368],
    UsernameBin = unicode:characters_to_binary(Username, utf8, utf8),

    lager:info("Checking SSL requires peer cert validation"),
    %% can't connect without specifying cacert to validate the server
    ?assertMatch({error, _}, riakc_pb_socket:start("127.0.0.1", Port,
                                                   [{credentials, UsernameBin,
                                                     "pass"}])),

    lager:info("Checking that authentication is required"),
    %% invalid credentials should be invalid
    ?assertEqual({error, {tcp, <<"Authentication failed">>}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, UsernameBin,
                                        "pass"}, {cacertfile,
                                                  filename:join([CertDir, "rootCA/cert.pem"])}])),

    lager:info("Creating user"),
    %% grant the user credentials
    ok = rpc:call(Node, riak_core_console, add_user, [[Username, "password=password"]]),

    lager:info("Setting trust mode on user"),
    %% trust 'user' on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [[Username, "127.0.0.1/32",
                                                    "trust"]]),

    lager:info("Checking that credentials are ignored in trust mode"),
    %% invalid credentials should be ignored in trust mode
    {ok, PB1} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, UsernameBin,
                                        "pass"}, {cacertfile,
                                                  filename:join([CertDir, "rootCA/cert.pem"])}]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB1)),
    riakc_pb_socket:stop(PB1),

    lager:info("Setting password mode on user"),
    %% require password on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [[Username, "127.0.0.1/32",
                                                    "password"]]),

    lager:info("Checking that incorrect password fails auth"),
    %% invalid credentials should be invalid
    ?assertEqual({error, {tcp, <<"Authentication failed">>}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, UsernameBin,
                                        "pass"}, {cacertfile,
                                                  filename:join([CertDir, "rootCA/cert.pem"])}])),

    lager:info("Checking that correct password is successful"),
    %% valid credentials should be valid
    {ok, PB2} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, UsernameBin,
                                        "password"}, {cacertfile,
                                                  filename:join([CertDir, "rootCA/cert.pem"])}]),
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
                                       {cacertfile, filename:join([CertDir, "site4.basho.com/cacerts.pem"])},
                                       {certfile, filename:join([CertDir, "site4.basho.com/cert.pem"])},
                                       {keyfile, filename:join([CertDir, "site4.basho.com/key.pem"])}
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
                                       {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])},
                                       {certfile, filename:join([CertDir, "site4.basho.com/cert.pem"])},
                                       {keyfile, filename:join([CertDir, "site4.basho.com/key.pem"])}
                                      ])),

    lager:info("Checking revoked certificates are denied"),
    ?assertMatch({error, {tcp, _Reason}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "site5.basho.com",
                                        "password"},
                                       {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])},
                                       {certfile, filename:join([CertDir, "site5.basho.com/cert.pem"])},
                                       {keyfile, filename:join([CertDir, "site5.basho.com/key.pem"])}
                                      ])),

    lager:info("Checking auth with non-peer certificate fails"),
    %% authing with non-peer certificate should fail
    ?assertMatch({error, {tcp, _Reason}}, riakc_pb_socket:start("127.0.0.1", Port,
                                                  [{credentials, "site5.basho.com",
                                                    "password"},
                                                   {cacertfile, filename:join([PrivDir,
                                                                               "certs/CA/rootCA/cert.pem"])},
                                                   {certfile, filename:join([PrivDir,
                                                                             "certs/cacert.org/ca-cert.pem"])},
                                                   {keyfile, filename:join([PrivDir,
                                                                            "certs/cacert.org/ca-key.pem"])}
                                                  ])),

    lager:info("cert from intermediate CA should work"),
    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site1.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site1.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    {ok, PB4} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "site1.basho.com", "password"},
                                       {cacertfile, filename:join([CertDir, "site1.basho.com/cacerts.pem"])},
                                       {certfile, filename:join([CertDir, "site1.basho.com/cert.pem"])},
                                       {keyfile, filename:join([CertDir, "site1.basho.com/key.pem"])}
                                      ]),

    ?assertEqual(pong, riakc_pb_socket:ping(PB4)),
    riakc_pb_socket:stop(PB4),

    lager:info("checking certificates from a revoked CA are denied"),
    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site6.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site6.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    ?assertMatch({error, {tcp, _Reason}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "site6.basho.com", "password"},
                                       {cacertfile, filename:join([CertDir, "site6.basho.com/cacerts.pem"])},
                                       {certfile, filename:join([CertDir, "site6.basho.com/cert.pem"])},
                                       {keyfile, filename:join([CertDir, "site6.basho.com/key.pem"])}
                                      ])),

    lager:info("checking a certificate signed by a leaf CA is not honored"),
    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site7.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site7.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    ?assertMatch({error, {tcp, _Reason}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "site7.basho.com", "password"},
                                       {cacertfile, filename:join([CertDir, "site7.basho.com/cacerts.pem"])},
                                       {certfile, filename:join([CertDir, "site7.basho.com/cert.pem"])},
                                       {keyfile, filename:join([CertDir, "site7.basho.com/key.pem"])}
                                      ])),

    %% time to actually do some stuff
    {ok, PB} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, UsernameBin, "password"},
                                       {cacertfile,
                                                  filename:join([CertDir, "rootCA/cert.pem"])}]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB)),

    lager:info("verifying that user cannot get/put without grants"),
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    lager:info("Granting riak_kv.get, checking get works but put doesn't"),
    grant(Node, ["riak_kv.get", "on", "default",  "hello", "to", Username]),

    ?assertMatch({error, notfound}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"howareyou">>))),

    lager:info("Granting riak_kv.put, checking put works and roundtrips with get"),
    grant(Node, ["riak_kv.put", "on", "default", "hello", "to", Username]),

    ?assertEqual(ok,
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"1">>, "application/json"))),

    ?assertMatch({ok, _Obj}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

    %% 1.4 counters
    %%
    grant(Node, ["riak_kv.put,riak_kv.get", "on", "default", "counters", "to", Username]),
    %% ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put,riak_kv.get", "on",
    %%                                                 "default", "counters", "to", Username]]),


    lager:info("Checking that counters work on resources that have get/put permitted"),
    ?assertEqual({error, notfound}, riakc_pb_socket:counter_val(PB,
                                                                <<"counters">>,
                                                                <<"numberofpies">>)),
    ok = riakc_pb_socket:counter_incr(PB, <<"counters">>,
                                 <<"numberofpies">>, 5),
    ?assertEqual({ok, 5}, riakc_pb_socket:counter_val(PB, <<"counters">>,
                                                      <<"numberofpies">>)),

    lager:info("Revoking get, checking that counter_val fails"),
    %% revoke get
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.get", "on", "default", "counters", "from", Username]]),

    ?assertMatch({error, <<"Permission",  _/binary>>},
                 riakc_pb_socket:counter_val(PB, <<"counters">>,
                                             <<"numberofpies">>)),
    ok = riakc_pb_socket:counter_incr(PB, <<"counters">>,
                                      <<"numberofpies">>, 5),

    lager:info("Revoking put, checking that counter_incr fails"),
    %% revoke put
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.put", "on", "default", "counters", "from", Username]]),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:counter_incr(PB, <<"counters">>,
                                              <<"numberofpies">>, 5)),


    lager:info("Revoking get/put, checking that get/put are disallowed"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.get,riak_kv.put", "on",
                                                    "default", "hello", "from", Username]]),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:put(PB,
                                     riakc_obj:new(<<"hello">>, <<"world">>,
                                                   <<"howareyou">>))),

    %% try the 'any' grant
    lager:info("Granting get on ANY, checking user can fetch any bucket/key"),
    grant(Node, ["riak_kv.get", "on", "any", "to", Username]),

    ?assertMatch({ok, _Obj}, riakc_pb_socket:get(PB, <<"hello">>,
                                                         <<"world">>)),

    lager:info("Revoking ANY permission, checking fetch fails"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.get", "on",
                                                    "any", "from", Username]]),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          <<"hello">>,
                                                                          <<"world">>)),

    %% list keys
    lager:info("Checking that list keys is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:list_keys(PB, <<"hello">>)),

    lager:info("Granting riak_kv.list_keys, checking that list_keys succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "on",
                                                    "default", "hello", "to", Username]]),

    ?assertMatch({ok, [<<"world">>]}, riakc_pb_socket:list_keys(PB, <<"hello">>)),

    lager:info("Checking that list buckets is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:list_buckets(PB)),

    lager:info("Granting riak_kv.list_buckets, checking that list_buckets succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_buckets", "on",
                                                    "default", "to", Username]]),

    {ok, BList} = riakc_pb_socket:list_buckets(PB),
    ?assertEqual([<<"counters">>, <<"hello">>], lists:sort(BList)),

    %% still need mapreduce permission
    lager:info("Checking that full-bucket mapred is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:mapred_bucket(PB, <<"hello">>,
                                       [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                        {reduce, {jsfun,
                                                  <<"Riak.reduceSum">>},
                                         undefined, true}])),

    lager:info("Granting mapreduce, checking that job succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.mapreduce", "on",
                                                    "default", "to", Username]]),

    ?assertEqual({ok, [{1, [1]}]},
                 riakc_pb_socket:mapred_bucket(PB, <<"hello">>,
                                       [{map, {jsfun, <<"Riak.mapValuesJson">>}, undefined, false},
                                        {reduce, {jsfun,
                                                  <<"Riak.reduceSum">>},
                                         undefined, true}])),

    lager:info("checking mapreduce with a whitelisted modfun works"),
    ?assertEqual({ok, [{1, [<<"1">>]}]},
                 riakc_pb_socket:mapred_bucket(PB, <<"hello">>,
                                       [{map, {modfun, riak_kv_mapreduce,
                                               map_object_value}, undefined, false},
                                        {reduce, {modfun,
                                                  riak_kv_mapreduce,
                                                  reduce_set_union},
                                         undefined, true}])),

    %% load this module on all the nodes
    ok = rt:load_modules_on_nodes([?MODULE], Nodes),

    lager:info("checking mapreduce with a insecure modfun input fails"),
    ?assertMatch({error, <<"{inputs,{insecure_module_path",_/binary>>},
                 riakc_pb_socket:mapred_bucket(PB, {modfun, ?MODULE,
                                                    mapred_modfun_input, []},
                                       [{map, {modfun, riak_kv_mapreduce,
                                               map_object_value}, undefined, false},
                                        {reduce, {modfun,
                                                  riak_kv_mapreduce,
                                                  reduce_set_union},
                                         undefined, true}])),

    lager:info("checking mapreduce with a insecure modfun phase fails"),
    ?assertMatch({error, <<"{query,{insecure_module_path",_/binary>>},
                 riakc_pb_socket:mapred_bucket(PB, <<"hello">>,
                                       [{map, {modfun, ?MODULE,
                                               map_object_value}, undefined, false},
                                        {reduce, {modfun,
                                                  ?MODULE,
                                                  reduce_set_union},
                                         undefined, true}])),

    lager:info("whitelisting module path"),
    ok = rpc:call(Node, application, set_env, [riak_kv, add_paths,
                                    [filename:dirname(code:which(?MODULE))]]),

    lager:info("checking mapreduce with a insecure modfun input fails when"
               " whitelisted but lacking permissions"),
    ?assertMatch({error, <<"Permission",_/binary>>},
                 riakc_pb_socket:mapred_bucket(PB, {modfun, ?MODULE,
                                                    mapred_modfun_input, []},
                                       [{map, {modfun, riak_kv_mapreduce,
                                               map_object_value}, undefined, false},
                                        {reduce, {modfun,
                                                  riak_kv_mapreduce,
                                                  reduce_set_union},
                                         undefined, true}])),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.mapreduce", "on",
                                                    "any", "to", Username]]),
    ?assertEqual({ok, [{1, [<<"1">>]}]},
                 riakc_pb_socket:mapred_bucket(PB, {modfun, ?MODULE,
                                                    mapred_modfun_input, []},
                                       [{map, {modfun, riak_kv_mapreduce,
                                               map_object_value}, undefined, false},
                                        {reduce, {modfun,
                                                  riak_kv_mapreduce,
                                                  reduce_set_union},
                                         undefined, true}])),

    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.mapreduce", "on",
                                                    "any", "from", Username]]),

    lager:info("checking mapreduce with a insecure modfun phase works when"
               " whitelisted"),
    ?assertEqual({ok, [{1, [<<"1">>]}]},
                 riakc_pb_socket:mapred_bucket(PB, <<"hello">>,
                                       [{map, {modfun, ?MODULE,
                                               map_object_value}, undefined, false},
                                        {reduce, {modfun,
                                                  ?MODULE,
                                                  reduce_set_union},
                                         undefined, true}])),



    lager:info("link walking should fail with a deprecation error"),
    ?assertMatch({error, _}, riakc_pb_socket:mapred(PB, [{<<"lists">>, <<"mine">>}],
                               [{link, <<"items">>, '_', true}])),

    %% revoke only the list_keys permission
    lager:info("Revoking list-keys, checking that full-bucket mapred fails"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.list_keys", "on",
                                                    "default", "hello", "from", Username]]),

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
            ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.index", "on",
                                                            "default", "to", Username]]),

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
    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.get_bucket", "on",
                                                    "default", "mybucket", "to", Username]]),

    ?assertEqual(3, proplists:get_value(n_val, element(2,
                                                       riakc_pb_socket:get_bucket(PB,
                                                                                  <<"mybucket">>)))),

    lager:info("Checking that set_bucket is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:set_bucket(PB, <<"mybucket">>, [{n_val, 5}])),

    lager:info("Granting set_bucket, checking that set_bucket succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.set_bucket", "on",
                                                    "default", "mybucket", "to", Username]]),
    ?assertEqual(ok,
                 riakc_pb_socket:set_bucket(PB, <<"mybucket">>, [{n_val, 5}])),

    ?assertEqual(5, proplists:get_value(n_val, element(2,
                                                       riakc_pb_socket:get_bucket(PB,
                                                                                  <<"mybucket">>)))),

    %%%%%%%%%%%%
    %%% bucket type tests
    %%%%%%%%%%%%

    %% create a new type
    rt:create_and_activate_bucket_type(Node, <<"mytype">>, [{n_val, 3}]),
    rt:wait_until_bucket_type_status(<<"mytype">>, active, Nodes),
    rt:wait_until_bucket_type_visible(Nodes, <<"mytype">>),

    lager:info("Checking that get on a new bucket type is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          {<<"mytype">>,
                                                                           <<"hello">>},
                                                                          <<"world">>)),

    lager:info("Granting get on the new bucket type, checking that it succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "on",
                                                    "mytype", "hello", "to", Username]]),

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
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "on",
                                                    "mytype", "hello", "to", Username]]),

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
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.get,riak_kv.put", "on",
                                                    "mytype", "hello", "from", Username]]),

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
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "on",
                                                    "mytype", "hello", "to", Username]]),

    ?assertEqual([<<"drnick">>, <<"world">>], lists:sort(element(2, riakc_pb_socket:list_keys(PB,
                                                                {<<"mytype">>,
                                                                 <<"hello">>})))),

    lager:info("Creating another bucket type"),
    %% create a new type
    rt:create_and_activate_bucket_type(Node, <<"mytype2">>, [{allow_mult, true}]),
    rt:wait_until_bucket_type_status(<<"mytype2">>, active, Nodes),
    rt:wait_until_bucket_type_visible(Nodes, <<"mytype2">>),

    lager:info("Checking that get on the new type is disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:get(PB,
                                                                          {<<"mytype2">>,
                                                                           <<"hello">>},
                                                                          <<"world">>)),

    lager:info("Granting get/put on all buckets in the new type, checking that get/put works"),
    %% do a wildcard grant
    ok = rpc:call(Node, riak_core_console, grant,
                  [["riak_kv.get,riak_kv.put", "on",
                                                    "mytype2", "to", Username]]),

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
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_buckets", "on",
                                                    "mytype2", "to", Username]]),

    ?assertMatch([<<"cromulent">>, <<"embiggen">>], lists:sort(element(2,
                                                                       riakc_pb_socket:list_buckets(PB,
                                                                                                   <<"mytype2">>)))),


    %% get/set bucket type props

    lager:info("Checking that get/set bucket-type properties are disallowed"),
    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:set_bucket_type(PB, <<"mytype2">>,
                                                                                      [{n_val, 5}])),

    ?assertMatch({error, <<"Permission", _/binary>>},
                 riakc_pb_socket:get_bucket_type(PB, <<"mytype2">>)),

    lager:info("Granting get on bucket type props, checking it succeeds and put still fails"),
    ok = rpc:call(Node, riak_core_console, grant,
                  [["riak_core.get_bucket_type", "on", "mytype2", "to", Username]]),

    ?assertEqual(3, proplists:get_value(n_val,
                                        element(2, riakc_pb_socket:get_bucket_type(PB,
                                                                        <<"mytype2">>)))),

    ?assertMatch({error, <<"Permission", _/binary>>}, riakc_pb_socket:set_bucket_type(PB, <<"mytype2">>,
                                                                                      [{n_val, 5}])),

    lager:info("Granting set on bucket type props, checking it succeeds"),
    ok = rpc:call(Node, riak_core_console, grant,
                  [["riak_core.set_bucket_type", "on", "mytype2", "to", Username]]),

    riakc_pb_socket:set_bucket_type(PB, <<"mytype2">>, [{n_val, 5}]),

    ?assertEqual(5, proplists:get_value(n_val,
                                        element(2, riakc_pb_socket:get_bucket_type(PB,
                                                                        <<"mytype2">>)))),

    riakc_pb_socket:set_bucket_type(PB, <<"mytype2">>, [{n_val, 3}]),

    crdt_tests(Nodes, PB),

    riakc_pb_socket:stop(PB),

    group_test(Node, Port, CertDir).

group_test(Node, Port, CertDir) ->
    %%%%%%%%%%%%%%%%
    %% test groups
    %%%%%%%%%%%%%%%%

    lager:info("Creating a new group"),
    %% create a new group
    ok = rpc:call(Node, riak_core_console, add_group, [["group"]]),

    lager:info("Creating a user in the group"),
    %% create a new user in that group
    ok = rpc:call(Node, riak_core_console, add_user, [["myuser", "groups=group"]]),


    lager:info("Granting get/put/delete on a bucket type to the group, checking those requests work"),

    %% do a wildcard grant
    grant(Node,["riak_kv.get,riak_kv.put,riak_kv.delete", "on", "mytype2",
                "to", "group"]),

    %% trust 'myuser' on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["myuser", "127.0.0.1/32",
                                                    "trust"]]),

    {ok, PB} = riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "myuser", "password"},
                                       {cacertfile,
                                        filename:join([CertDir, "rootCA/cert.pem"])}]),

    ?assertMatch({error, notfound}, (riakc_pb_socket:get(PB, {<<"mytype2">>,
                                                              <<"hello">>},
                                                          <<"world">>))),
    ?assertEqual(ok,
                 (riakc_pb_socket:put(PB,
                                      riakc_obj:new({<<"mytype2">>, <<"hello">>}, <<"world">>,
                                                    <<"howareyou">>)))),

    {ok, Obj} = riakc_pb_socket:get(PB, {<<"mytype2">>,
                                                      <<"hello">>},
                                                         <<"world">>),
    riakc_pb_socket:delete_obj(PB, Obj),

    ?assertMatch({error, notfound}, (riakc_pb_socket:get(PB, {<<"mytype2">>,
                                                              <<"hello">>},
                                                          <<"world">>))),

    lager:info("riak search should not be running with security enabled"),
    ?assertMatch({error, <<"Riak Search 1.0 is deprecated", _/binary>>},
                           riakc_pb_socket:search(PB, <<"index">>, <<"foo:bar">>)),

    riakc_pb_socket:stop(PB),
    pass.

grant(Node, Args) ->
    ok = rpc:call(Node, riak_core_console, grant, [Args]).

crdt_tests([Node|_]=Nodes, PB) ->
    Username = [2361,2367,2344,2381,2342,2368],

    %% rt:create_and_activate
    lager:info("Creating bucket types for CRDTs"),

    Types = [{<<"counters">>, counter, riakc_counter:to_op(riakc_counter:increment(5, riakc_counter:new()))},
             {<<"sets">>, set, riakc_set:to_op(riakc_set:add_element(<<"foo">>, riakc_set:new()))},
             {<<"maps">>, map, riakc_map:to_op(riakc_map:update({<<"bar">>, counter}, fun(In) -> riakc_counter:increment(In) end, riakc_map:new()))}],
    [ begin
          rt:create_and_activate_bucket_type(Node, BType, [{allow_mult, true}, {datatype, DType}]),
          rt:wait_until_bucket_type_status(BType, active, Nodes),
          rt:wait_until_bucket_type_visible(Nodes, BType)
      end || {BType, DType, _Op} <- Types ],

    lager:info("Checking that CRDT fetch is denied"),

    [ ?assertDenied(riakc_pb_socket:fetch_type(PB, {BType, <<"bucket">>}, <<"key">>))
      ||  {BType, _, _} <- Types ],

    lager:info("Granting CRDT riak_kv.get, checking that fetches succeed"),

    [ grant(Node, ["riak_kv.get", "on", binary_to_list(Type), "to", Username]) || {Type, _, _} <- Types ],

    [ ?assertEqual({error, {notfound, DType}},
                   riakc_pb_socket:fetch_type(PB, {BType, <<"bucket">>}, <<"key">>)) ||
        {BType, DType, _} <- Types ],

    lager:info("Checking that CRDT update is denied"),

    [ ?assertDenied(riakc_pb_socket:update_type(PB, {BType, <<"bucket">>}, <<"key">>, Op))
      ||  {BType, _, Op} <- Types ],


    lager:info("Granting CRDT riak_kv.put, checking that updates succeed"),

    [ grant(Node, ["riak_kv.put", "on", binary_to_list(Type), "to", Username]) || {Type, _, _} <- Types ],

    [ ?assertEqual(ok, riakc_pb_socket:update_type(PB, {BType, <<"bucket">>}, <<"key">>, Op))
      ||  {BType, _, Op} <- Types ],

    ok.

map_object_value(RiakObject, A, B) ->
    riak_kv_mapreduce:map_object_value(RiakObject, A, B).

reduce_set_union(List, A) ->
    riak_kv_mapreduce:reduce_set_union(List, A).

mapred_modfun_input(Pipe, _Args, _Timeout) ->
    riak_pipe:queue_work(Pipe, {{<<"hello">>, <<"world">>}, {struct, []}}),
    riak_pipe:eoi(Pipe).
