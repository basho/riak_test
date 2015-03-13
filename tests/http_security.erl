-module(http_security).

-behavior(riak_test).
-export([confirm/0]).

-export([map_object_value/3, reduce_set_union/2, mapred_modfun_input/3]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(assertDenied(Op), ?assertMatch({error, {forbidden, _}}, Op)).

confirm() ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(ibrowse),
    io:format("turning on tracing"),
    ibrowse:trace_on(),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/http_certs",

    %% make a bunch of crypto keys
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:endusers(CertDir, "rootCA", ["site3.basho.com", "site4.basho.com"]),


    lager:info("Deploy some nodes"),
    PrivDir = rt:priv_dir(),
    Conf = [
            {riak_core, [
                    {default_bucket_props, [{allow_mult, true}]},
                    {ssl, [
                            {certfile, filename:join([CertDir,
                                                      "site3.basho.com/cert.pem"])},
                            {keyfile, filename:join([CertDir,
                                                     "site3.basho.com/key.pem"])},
                            {cacertfile, filename:join([CertDir, "site3.basho.com/cacerts.pem"])}
                            ]}
                    ]},
             {riak_search, [
                     {enabled, true}
                    ]}
    ],
    Nodes = rt:build_cluster(4, Conf),
    Node = hd(Nodes),
    %% enable security on the cluster
    ok = rpc:call(Node, riak_core_console, security_enable, [[]]),
    enable_ssl(Node),
    %%[enable_ssl(N) || N <- Nodes],
    {ok, [{IP0, Port0}]} = rpc:call(Node, application, get_env,
                                    [riak_api, http]),
    {ok, [{IP, Port}]} = rpc:call(Node, application, get_env,
                                  [riak_api, https]),

    MD = riak_test_runner:metadata(),
    HaveIndexes = case proplists:get_value(backend, MD) of
                      undefined -> false; %% default is da 'cask
                      bitcask -> false;
                      _ -> true
                  end,

    lager:info("Checking non-SSL results in error"),
    %% connections over regular HTTP get told to go elsewhere
    C0 = rhc:create(IP0, Port0, "riak", []),
    ?assertMatch({error, {ok, "426", _, _}}, rhc:ping(C0)),

    lager:info("Checking SSL demands authentication"),
    C1 = rhc:create(IP, Port, "riak", [{is_ssl, true}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C1)),

    lager:info("Checking that unknown user demands reauth"),
    C2 = rhc:create(IP, Port, "riak", [{is_ssl, true}, {credentials,
                                                        "user",
                                                        "pass"}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C2)),

    %% Store this in a variable so once Riak supports utf-8 usernames
    %% via HTTP(s) we can test it with just one change
    Username = "user",

    lager:info("Creating user"),
    %% grant the user credentials
    ok = rpc:call(Node, riak_core_console, add_user, [[Username, "password=password"]]),

    lager:info("Setting trust mode on user"),
    %% trust anyone from this host
    MyIP = case IP0 of
               "127.0.0.1" -> IP0;
               _ ->
                   {ok,Hostname} = inet:gethostname(),
                   {ok,A0} = inet:getaddr(Hostname, inet),
                   inet:ntoa(A0)
           end,
    ok = rpc:call(Node, riak_core_console, add_source, [[Username,
                                                         MyIP++"/32",
                                                         "trust"]]),

    lager:info("Checking that credentials are ignored in trust mode"),
    %% invalid credentials should be ignored in trust mode
    C3 = rhc:create(IP, Port, "riak", [{is_ssl, true}, {credentials,
                                                        Username,
                                                        "pass"}]),
    ?assertEqual(ok, rhc:ping(C3)),

    lager:info("Setting password mode on user"),
    %% require password from our IP
    ok = rpc:call(Node, riak_core_console, add_source, [[Username,
                                                         MyIP++"/32",
                                                         "password"]]),

    lager:info("Checking that incorrect password demands reauth"),
    %% invalid credentials should be rejected in password mode
    C4 = rhc:create(IP, Port, "riak", [{is_ssl, true}, {credentials,
                                                        Username,
                                                        "pass"}]),
    ?assertMatch({error, {ok, "401", _, _}}, rhc:ping(C4)),

    lager:info("Checking that correct password is successful"),
    %% valid credentials should be accepted in password mode
    C5 = rhc:create(IP, Port, "riak", [{is_ssl, true}, {credentials,
                                                        Username,
                                                        "password"}]),

    ?assertEqual(ok, rhc:ping(C5)),

    lager:info("verifying the peer certificate rejects mismatch with server cert"),
    %% verifying the peer certificate reject mismatch with server cert
    C6 = rhc:create(IP, Port, "riak", [{is_ssl, true},
                                       {credentials, Username, "password"},
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
    C7 = rhc:create(IP, Port, "riak", [{is_ssl, true},
                                       {credentials, Username, "password"},
                                       {ssl_options, [
                        {cacertfile, filename:join([CertDir,
                                                    "rootCA/cert.pem"])},
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
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get", "on",
                                                    "default", "hello", "to", Username]]),

    %% key is not present
    ?assertMatch({error, notfound}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:put(C7, Object)),

    lager:info("Granting riak_kv.put, checking put works and roundtrips with get"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "on",
                                                    "default", "hello", "to", Username]]),

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

    lager:info("Checking that delete for non-existing key is disallowed"),
    ?assertMatch({error, {ok, "403", _, _}}, rhc:delete(C7, <<"hello">>,
                                                        <<"_xxboguskey">>)),

    lager:info("Granting riak_kv.delete, checking that delete succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.delete", "on",
                                                    "default", "hello", "to", Username]]),
    ?assertEqual(ok, rhc:delete(C7, <<"hello">>,
                                <<"world">>)),

    %% key is deleted
    ?assertMatch({error, notfound}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    %% write it back for list_buckets later
    ?assertEqual(ok, rhc:put(C7, Object)),

    lager:info("Checking that delete for non-existing key is allowed"),
    ?assertMatch({error, {ok, "404", _, _}}, rhc:delete(C7, <<"hello">>,
                                                        <<"_xxboguskey">>)),


    %% slam the door in the user's face
    lager:info("Revoking get/put/delete, checking that get/put/delete are disallowed"),
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.put,riak_kv.get,riak_kv.delete", "on",
                    "default", "hello", "from", Username]]),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:get(C7, <<"hello">>,
                                                     <<"world">>)),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:put(C7, Object)),

    %% list buckets
    lager:info("Checking that list buckets is disallowed"),
    ?assertMatch({error, {"403", _}}, rhc:list_buckets(C7)),

    lager:info("Granting riak_kv.list_buckets, checking that list_buckets succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_buckets", "on",
                                                    "default", "to", Username]]),
    ?assertMatch({ok, [<<"hello">>]}, rhc:list_buckets(C7)),

    %% list keys
    lager:info("Checking that list keys is disallowed"),
    ?assertMatch({error, {"403", _}}, rhc:list_keys(C7, <<"hello">>)),

    lager:info("Granting riak_kv.list_keys, checking that list_keys succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "on",
                                                    "default", "to", Username]]),

    ?assertMatch({ok, [<<"world">>]}, rhc:list_keys(C7, <<"hello">>)),

    lager:info("Revoking list_keys"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.list_keys", "on",
                                                    "default", "from", Username]]),

    %% list keys with bucket type
    rt:create_and_activate_bucket_type(Node, <<"list-keys-test">>, []),

    lager:info("Checking that list keys on a bucket-type is disallowed"),
    ?assertMatch({error, {"403", _}}, rhc:list_keys(C7, {<<"list-keys-test">>, <<"hello">>})),

    lager:info("Granting riak_kv.list_keys on the bucket type, checking that list_keys succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "on",
                                                    "list-keys-test", "to", Username]]),
    ?assertMatch({ok, []}, rhc:list_keys(C7, {<<"list-keys-test">>, <<"hello">>})),

    lager:info("Checking that get_bucket is disallowed"),
    ?assertMatch({error, {ok, "403", _, _}}, rhc:get_bucket(C7, <<"hello">>)),

    lager:info("Granting riak_core.get_bucket, checking that get_bucket succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.get_bucket", "on",
                                                    "default", "hello", "to", Username]]),

    ?assertEqual(3, proplists:get_value(n_val, element(2, rhc:get_bucket(C7,
                                                                         <<"hello">>)))),

    lager:info("Checking that reset_bucket is disallowed"),
    ?assertMatch({error, {ok, "403", _, _}}, rhc:reset_bucket(C7, <<"hello">>)),

    lager:info("Checking that set_bucket is disallowed"),
    ?assertMatch({error, {ok, "403", _, _}}, rhc:set_bucket(C7, <<"hello">>,
                                                            [{n_val, 5}])),

    lager:info("Granting set_bucket, checking that set_bucket succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_core.set_bucket", "on",
                                                    "default", "hello", "to", Username]]),

    ?assertEqual(ok, rhc:set_bucket(C7, <<"hello">>,
                                    [{n_val, 5}])),

    ?assertEqual(5, proplists:get_value(n_val, element(2, rhc:get_bucket(C7,
                                                                         <<"hello">>)))),

    %% 2i
    case HaveIndexes of
        false -> ok;
        true ->
            %% 2i permission test
            lager:info("Checking 2i is disallowed"),
            ?assertMatch({error, {"403", _}},
                         rhc:get_index(C7, <<"hello">>,
                                                   {binary_index,
                                                    "name"},
                                                   <<"John">>)),

            lager:info("Granting 2i permissions, checking that results come back"),
            ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.index", "on",
                                                            "default", "to", Username]]),

            %% don't actually have any indexes
            ?assertMatch({ok, ?INDEX_RESULTS{}},
                         rhc:get_index(C7, <<"hello">>,
                                                   {binary_index,
                                                    "name"},
                                                   <<"John">>)),

            lager:info("Checking that 2i on a bucket-type is disallowed"),
            ?assertMatch({error, {"403", _}},
                         rhc:get_index(C7, {<<"list-keys-test">>,
                                            <<"hello">>}, {binary_index, "name"}, <<"John">>)),

            lager:info("Granting riak_kv.index on the bucket type, checking that get_index succeeds"),
            ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.index", "on",
                                                    "list-keys-test", "to", Username]]),
            ?assertMatch({ok, ?INDEX_RESULTS{}},
                         rhc:get_index(C7, {<<"list-keys-test">>,
                                            <<"hello">>}, {binary_index, "name"}, <<"John">>)),

            ok
    end,

    %% counters

    %% grant get/put again
    lager:info("Granting get/put for counters, checking value and increment"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.get,riak_kv.put", "on",
                                                    "default", "hello", "to", Username]]),


    ?assertMatch({error, {ok, "404", _, _}}, rhc:counter_val(C7, <<"hello">>,
                                                    <<"numberofpies">>)),

    ok = rhc:counter_incr(C7, <<"hello">>,
                          <<"numberofpies">>, 5),

    ?assertEqual({ok, 5}, rhc:counter_val(C7, <<"hello">>,
                                          <<"numberofpies">>)),

    %% revoke get
    lager:info("Revoking get, checking that value fails but increment succeeds"),
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.get", "on", "default", "hello", "from", Username]]),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:counter_val(C7, <<"hello">>,
                                          <<"numberofpies">>)),
    ok = rhc:counter_incr(C7, <<"hello">>,
                          <<"numberofpies">>, 5),

    %% revoke put
    lager:info("Revoking put, checking that increment fails"),
    ok = rpc:call(Node, riak_core_console, revoke,
                  [["riak_kv.put", "on", "default", "hello", "from", Username]]),

    ?assertMatch({error, {ok, "403", _, _}}, rhc:counter_incr(C7, <<"hello">>,
                          <<"numberofpies">>, 5)),

    %% mapred tests
    lager:info("Checking that full-bucket mapred is disallowed"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.put", "on",
                                                    "default", "MR", "to", Username]]),


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
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.list_keys", "on",
                                                    "default", "MR", "to", Username]]),

    ?assertMatch({error, {"403", _}},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {jsfun,
                                                     <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun,
                                                        <<"Riak.reduceSum">>}, undefined,
                                               true}])),

    lager:info("Granting mapreduce, checking that job succeeds"),
    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.mapreduce", "on",
                                                    "default", "MR", "to", Username]]),

    ?assertEqual({ok, [{1, [33]}]},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {jsfun,
                                                     <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun,
                                                        <<"Riak.reduceSum">>}, undefined,
                                               true}])),

    %% load this module on all the nodes
    ok = rt:load_modules_on_nodes([?MODULE], Nodes),

    lager:info("checking erlang mapreduce works"),
    ?assertMatch({ok, [{1, _}]},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {modfun,
                                                     riak_kv_mapreduce,
                                                        map_object_value}, undefined, false},
                                              {reduce, {modfun,
                                                        riak_kv_mapreduce,
                                                       reduce_set_union}, undefined,
                                               true}])),

    lager:info("checking that insecure input modfun fails"),
    ?assertMatch({error, _},
                 rhc:mapred_bucket(C7, {modfun, ?MODULE, mapred_modfun_input,
                                        []}, [{map, {modfun,
                                                     riak_kv_mapreduce,
                                                        map_object_value}, undefined, false},
                                              {reduce, {modfun,
                                                        riak_kv_mapreduce,
                                                       reduce_set_union}, undefined,
                                               true}])),

    lager:info("checking that insecure query modfuns fail"),
    ?assertMatch({error, _},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {modfun,
                                                     ?MODULE,
                                                        map_object_value}, undefined, false},
                                              {reduce, {modfun,
                                                        ?MODULE,
                                                       reduce_set_union}, undefined,
                                               true}])),

    lager:info("whitelisting module path"),
    {?MODULE, _ModBin, ModFile} = code:get_object_code(?MODULE),
    ok = rpc:call(Node, application, set_env, [riak_kv, add_paths, [filename:dirname(ModFile)]]),

    lager:info("checking that insecure input modfun fails when whitelisted but"
               " lacking permissions"),
    ?assertMatch({error, {"403", _}},
                 rhc:mapred_bucket(C7, {modfun, ?MODULE, mapred_modfun_input,
                                        []}, [{map, {modfun,
                                                     riak_kv_mapreduce,
                                                        map_object_value}, undefined, false},
                                              {reduce, {modfun,
                                                        riak_kv_mapreduce,
                                                       reduce_set_union}, undefined,
                                               true}])),

    ok = rpc:call(Node, riak_core_console, grant, [["riak_kv.mapreduce", "on",
                                                    "any", "to", Username]]),

    lager:info("checking that insecure input modfun works when whitelisted and"
               " has permissions"),
    ?assertMatch({ok, _},
                 rhc:mapred_bucket(C7, {modfun, ?MODULE, mapred_modfun_input,
                                        []}, [{map, {modfun,
                                                     riak_kv_mapreduce,
                                                        map_object_value}, undefined, false},
                                              {reduce, {modfun,
                                                        riak_kv_mapreduce,
                                                       reduce_set_union}, undefined,
                                               true}])),

    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.mapreduce", "on",
                                                    "any", "from", Username]]),

    lager:info("checking that insecure query modfuns works when whitelisted"),
    ?assertMatch({ok, _},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {modfun,
                                                     ?MODULE,
                                                        map_object_value}, undefined, false},
                                              {reduce, {modfun,
                                                        ?MODULE,
                                                       reduce_set_union}, undefined,
                                               true}])),


    lager:info("Revoking list-keys, checking that full-bucket mapred fails"),
    ok = rpc:call(Node, riak_core_console, revoke, [["riak_kv.list_keys", "on",
                                                    "default", "MR", "from", Username]]),

    ?assertMatch({error, {"403", _}},
                 rhc:mapred_bucket(C7, <<"MR">>, [{map, {jsfun,
                                                     <<"Riak.mapValuesJson">>}, undefined, false},
                                              {reduce, {jsfun,
                                                        <<"Riak.reduceSum">>}, undefined,
                                               true}])),

    crdt_tests(Nodes, C7),

    URL = lists:flatten(io_lib:format("https://~s:~b", [IP, Port])),

    lager:info("checking link walking fails because it is deprecated"),

    ?assertMatch({ok, "403", _, <<"Link walking is deprecated", _/binary>>},
                       ibrowse:send_req(URL ++ "/riak/hb/first/_,_,_", [], get,
                     [], [{response_format, binary}, {is_ssl, true},
                          {ssl_options, [
                                         {cacertfile, filename:join([CertDir,
                                                                     "rootCA/cert.pem"])},
                                         {verify, verify_peer},
                                         {reuse_sessions, false}]}])),

    lager:info("checking search 1.0 403s because search won't allow"
               "connections with security enabled"),

    ?assertMatch({ok, "403", _, <<"Riak Search 1.0 is deprecated", _/binary>>},
                       ibrowse:send_req(URL ++ "/solr/index/select?q=foo:bar&wt=json", [], get,
                     [], [{response_format, binary}, {is_ssl, true},
                          {ssl_options, [
                                         {cacertfile, filename:join([CertDir,
                                                                     "rootCA/cert.pem"])},
                                         {verify, verify_peer},
                                         {reuse_sessions, false}]}])),
    pass.

enable_ssl(Node) ->
    [{http, {IP, Port}}|_] = rt:connection_info(Node),
    rt:update_app_config(Node, [{riak_api, [{https, [{IP,
                                                      Port+1000}]}]}]),
    rt:wait_until_pingable(Node),
    rt:wait_for_service(Node, riak_kv).

map_object_value(RiakObject, A, B) ->
    riak_kv_mapreduce:map_object_value(RiakObject, A, B).

reduce_set_union(List, A) ->
    riak_kv_mapreduce:reduce_set_union(List, A).

mapred_modfun_input(Pipe, _Args, _Timeout) ->
    riak_pipe:queue_work(Pipe, {{<<"MR">>, <<"lobster_roll">>}, {struct, []}}),
    riak_pipe:eoi(Pipe).

crdt_tests([Node|_]=Nodes, RHC) ->
    Username = "user",

    lager:info("Creating bucket types for CRDTs"),
    Types = [{<<"counters">>, counter, riakc_counter:to_op(riakc_counter:increment(5, riakc_counter:new()))},
             {<<"sets">>, set, riakc_set:to_op(riakc_set:add_element(<<"foo">>, riakc_set:new()))}],
    [ begin
          rt:create_and_activate_bucket_type(Node, BType, [{allow_mult, true}, {datatype, DType}]),
          rt:wait_until_bucket_type_status(BType, active, Nodes),
          rt:wait_until_bucket_type_visible(Nodes, BType)
      end || {BType, DType, _Op} <- Types ],

    lager:info("Checking that CRDT fetch is denied"),

    [ ?assertDenied(rhc:fetch_type(RHC, {BType, <<"bucket">>}, <<"key">>))
     ||  {BType, _, _} <- Types],

    lager:info("Granting CRDT riak_kv.get, checking that fetches succeed"),

    [ grant(Node, ["riak_kv.get", "on", binary_to_list(Type), "to", Username]) || {Type, _, _} <- Types ],

    [ ?assertEqual({error, {notfound, DType}},
                  (rhc:fetch_type(RHC, {BType, <<"bucket">>}, <<"key">>))) ||
          {BType, DType, _, _} <- Types],

    lager:info("Checking that CRDT update is denied"),

    [ ?assertDenied(rhc:update_type(RHC, {BType, <<"bucket">>}, <<"key">>, Op))
     ||  {BType, _, Op} <- Types],


    lager:info("Granting CRDT riak_kv.put, checking that updates succeed"),

    [ grant(Node, ["riak_kv.put", "on", binary_to_list(Type), "to", Username]) || {Type, _, _} <- Types ],

    [?assertEqual(ok, (rhc:update_type(RHC, {BType, <<"bucket">>}, <<"key">>, Op)))
     ||  {BType, _, Op} <- Types],

    ok.

grant(Node, Args) ->
    ok = rpc:call(Node, riak_core_console, grant, [Args]).
