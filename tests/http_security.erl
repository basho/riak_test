-module(http_security).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(assertDenied(Op), ?assertMatch({error, {forbidden, _}}, Op)).

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
                                 [riak_api, http]),
    {ok, [{"127.0.0.1", Port}]} = rpc:call(Node, application, get_env,
                                 [riak_api, https]),

    MD = riak_test_runner:metadata(),
    _HaveIndexes = case proplists:get_value(backend, MD) of
                      undefined -> false; %% default is da 'cask
                      bitcask -> false;
                      _ -> true
                  end,

    lager:info("Checking non-SSL results in error"),
    %% connections over regular HTTP get told to go elsewhere
    C0 = rhc:create("127.0.0.1", Port0, "riak", []),
    ?assertMatch({error, {ok, "426", _, _}}, rhc:ping(C0)),

    lager:info("Checking SSL demands authentication"),
    C1 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C1)),

    lager:info("Checking that unknown user demands reauth"),
    C2 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "pass"}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C2)),

    lager:info("Creating user"),
    %% grant the user credentials
    ok = rpc:call(Node, riak_core_console, add_user, [["user", "password=password"]]),

    lager:info("Setting trust mode on user"),
    %% trust anyone on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user",
                                                         "127.0.0.1/32",
                                                         "trust"]]),

    lager:info("Checking that credentials are ignored in trust mode"),
    %% invalid credentials should be ignored in trust mode
    C3 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "pass"}]),
    ?assertEqual(ok, rhc:ping(C3)),

    lager:info("Setting password mode on user"),
    %% require password on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user",
                                                         "127.0.0.1/32",
                                                         "password"]]),

    lager:info("Checking that incorrect password demands reauth"),
    %% invalid credentials should be rejected in password mode
    C4 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "pass"}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C4)),

    lager:info("Checking that correct password is successful"),
    %% valid credentials should be accepted in password mode
    C5 = rhc:create("127.0.0.1", Port, "riak", [{is_ssl, true}, {credentials,
                                                                "user",
                                                                 "password"}]),

    ?assertEqual(ok, rhc:ping(C5)),

    lager:info("verifying the peer certificate rejects mismatch with server cert"),
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

    ?assertMatch({error,{conn_failed,{error,_}}}, rhc:ping(C6)),

    lager:info("verifying the peer certificate should work if the cert is valid"),
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

    lager:info("verifying that user cannot get/put without grants"),
    ?assertMatch({error, {ok, "403", _, _}}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    Object = riakc_obj:new(<<"hello">>, <<"world">>, <<"howareyou">>,
                           <<"text/plain">>),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:put(C7, Object)),

    lager:info("Granting riak_kv.get, checking get works but put doesn't"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "ON",
                                                    "default", "hello", "TO", "user"]]),

    %% key is not present
    ?assertMatch({error, notfound}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:put(C7, Object)),

    lager:info("Granting riak_kv.put, checking put works and roundtrips with get"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "ON",
                                                    "default", "hello", "TO", "user"]]),

    %% NOW we can put
    ?assertEqual(ok, rhc:put(C7, Object)),

    {ok, O} = rhc:get(C7, <<"hello">>, <<"world">>),
    ?assertEqual(<<"hello">>, riakc_obj:bucket(O)),
    ?assertEqual(<<"world">>, riakc_obj:key(O)),
    ?assertEqual(<<"howareyou">>, riakc_obj:get_value(O)),

    lager:info("Checking that delete is disallowed"),
    %% delete
    ?assertMatch({error, {ok, "403", _, _}}, rhc:delete(C7, <<"hello">>,
                                                        <<"world">>)),

    lager:info("Granting riak_kv.delete, checking that delete succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.delete", "ON",
                                                    "default", "hello", "TO", "user"]]),
    ?assertEqual(ok, rhc:delete(C7, <<"hello">>,
                                <<"world">>)),

    %% key is deleted
    ?assertMatch({error, notfound}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    %% write it back for list_buckets later
    ?assertEqual(ok, rhc:put(C7, Object)),

    %% slam the door in the user's face
    lager:info("Revoking get/put/delete, checking that get/put/delete are disallowed"),
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.put,riak_kv.get,riak_kv.delete", "ON",
                    "default", "hello", "FROM", "user"]]),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:put(C7, Object)),

    %% list buckets
    lager:info("Checking that list buckets is disallowed"),
    ?assertMatch({error, {"403", _}}, rhc:list_buckets(C7)),

    lager:info("Granting riak_kv.list_buckets, checking that list_buckets succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_buckets", "ON",
                                                    "default", "TO", "user"]]),
    ?assertMatch({ok, [<<"hello">>]}, rhc:list_buckets(C7)),

    %% list keys
    lager:info("Checking that list keys is disallowed"),
    ?assertMatch({error, {"403", _}}, rhc:list_keys(C7, <<"hello">>)),

    lager:info("Granting riak_kv.list_keys, checking that list_keys succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "ON",
                                                    "default", "TO", "user"]]),

    ?assertMatch({ok, [<<"world">>]}, rhc:list_keys(C7, <<"hello">>)),

    lager:info("Revoking list_keys"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.list_keys", "ON",
                                                    "default", "FROM", "user"]]),

    lager:info("Checking that get_bucket is disallowed"),
    ?assertMatch({error, {ok, "403", _, _}}, rhc:get_bucket(C7, <<"hello">>)),

    lager:info("Granting riak_core.get_bucket, checking that get_bucket succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.get_bucket", "ON",
                                                    "default", "hello", "TO", "user"]]),

    ?assertEqual(3, proplists:get_value(n_val, element(2, rhc:get_bucket(C7,
                                                                         <<"hello">>)))),

    lager:info("Checking that set_bucket is disallowed"),
    ?assertMatch({error, {ok, "403", _, _}}, rhc:set_bucket(C7, <<"hello">>,
                                                            [{n_val, 5}])),

    lager:info("Granting set_bucket, checking that set_bucket succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.set_bucket", "ON",
                                                    "default", "hello", "TO", "user"]]),

    ?assertEqual(ok, rhc:set_bucket(C7, <<"hello">>,
                                    [{n_val, 5}])),

    ?assertEqual(5, proplists:get_value(n_val, element(2, rhc:get_bucket(C7,
                                                                         <<"hello">>)))),

    %% counters

    %% grant get/put again
    lager:info("Granting get/put for counters, checking value and increment"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get,riak_kv.put", "ON",
                                                    "default", "hello", "TO", "user"]]),


    ?assertMatch({error, {ok, "404", _, _}}, rhc:counter_val(C7, <<"hello">>,
                                                    <<"numberofpies">>)),

    ok = rhc:counter_incr(C7, <<"hello">>,
                          <<"numberofpies">>, 5),

    ?assertEqual({ok, 5}, rhc:counter_val(C7, <<"hello">>,
                                          <<"numberofpies">>)),

    %% revoke get
    lager:info("Revoking get, checking that value fails but increment succeeds"),
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.get", "ON", "default", "hello", "FROM", "user"]]),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:counter_val(C7, <<"hello">>,
                                          <<"numberofpies">>)),
    ok = rhc:counter_incr(C7, <<"hello">>,
                          <<"numberofpies">>, 5),

    %% revoke put
    lager:info("Revoking put, checking that increment fails"),
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.put", "ON", "default", "hello", "FROM", "user"]]),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:counter_incr(C7, <<"hello">>,
                          <<"numberofpies">>, 5)),

    %% mapred tests
    lager:info("Checking that full-bucket mapred is disallowed"),
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

    lager:info("Granting list-keys, asserting full-bucket mapred is still disallowed"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "ON",
                                                    "default", "MR", "TO", "user"]]),

    ?assertMatch({error, {"403", _}},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {jsfun,
                                                     <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun,
                                                        <<"Riak.reduceSum">>}, undefined,
                                               true}])),

    lager:info("Granting mapreduce, checking that job succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.mapreduce", "ON",
                                                    "default", "MR", "TO", "user"]]),

    ?assertEqual({ok, [{1, [33]}]},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {jsfun,
                                                     <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun,
                                                        <<"Riak.reduceSum">>}, undefined,
                                               true}])),

    lager:info("Revoking list-keys, checking that full-bucket mapred fails"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.list_keys", "ON",
                                                    "default", "MR", "FROM", "user"]]),

    ?assertMatch({error, {"403", _}},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {jsfun,
                                                     <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun,
                                                        <<"Riak.reduceSum">>}, undefined,
                                               true}])),

    crdt_tests(Nodes, C7),
    ok.

enable_ssl(Node) ->
    [{http, {_IP, Port}}|_] = rt:connection_info(Node),
    rt:update_app_config(Node, [{riak_api, [{https, [{"127.0.0.1",
                                                     Port+1000}]}]}]),
    rt:wait_until_pingable(Node),
    rt:wait_for_service(Node, riak_kv).



crdt_tests([Node|_]=Nodes, RHC) ->
    lager:info("Creating bucket types for CRDTs"),
    Types = [{<<"counters">>, counter, riakc_counter:to_op(riakc_counter:increment(5, riakc_counter:new()))},
             {<<"sets">>, set, riakc_set:to_op(riakc_set:add_element(<<"foo">>, riakc_set:new()))},
             {<<"maps">>, map, riakc_map:to_op(riakc_map:add({<<"bar">>, counter}, riakc_map:new()))}],
    [ begin
          rt:create_and_activate_bucket_type(Node, BType, [{allow_mult, true}, {datatype, DType}]),
          rt:wait_until_bucket_type_status(BType, active, Nodes)
      end || {BType, DType, _Op} <- Types ],

    lager:info("Checking that CRDT fetch is denied"),

    [ ?assertDenied(rhc:fetch_type(RHC, {BType, <<"bucket">>}, <<"key">>))
     ||  {BType, _, _} <- Types],

    lager:info("Granting CRDT riak_kv.get, checking that fetches succeed"),

    [ grant(Node, ["riak_kv.get", "ON", binary_to_list(Type), "TO", "user"]) || {Type, _, _} <- Types ],

    [ ?assertEqual({error, {notfound, DType}},
                  (rhc:fetch_type(RHC, {BType, <<"bucket">>}, <<"key">>))) ||
          {BType, DType, _, _} <- Types],

    lager:info("Checking that CRDT update is denied"),

    [ ?assertDenied(rhc:update_type(RHC, {BType, <<"bucket">>}, <<"key">>, Op))
     ||  {BType, _, Op} <- Types],


    lager:info("Granting CRDT riak_kv.put, checking that updates succeed"),

    [ grant(Node, ["riak_kv.put", "ON", binary_to_list(Type), "TO", "user"]) || {Type, _, _} <- Types ],

    [?assertEqual(ok, (rhc:update_type(RHC, {BType, <<"bucket">>}, <<"key">>, Op)))
     ||  {BType, _, Op} <- Types],

    ok.

grant(Node, Args) ->
    ok = rpc:call(Node, riak_core_console, grant, [Args]).
