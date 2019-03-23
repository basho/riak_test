-module(pb_cipher_suites).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(assertDenied(Op), ?assertMatch({error, <<"Permission",_/binary>>}, Op)).

confirm() ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(inets),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/pb_cipher_suites_certs",

    %% make a bunch of crypto keys
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:intermediateCA(CertDir, "intCA", "rootCA"),
    make_certs:intermediateCA(CertDir, "revokedCA", "rootCA"),
    make_certs:endusers(CertDir, "intCA", ["site1.basho.com", "site2.basho.com"]),
    make_certs:endusers(CertDir, "rootCA", ["site3.basho.com", "site4.basho.com", "site5.basho.com"]),
    make_certs:enduser(CertDir, "revokedCA", "site6.basho.com"),
    make_certs:revoke(CertDir, "rootCA", "site5.basho.com"),
    make_certs:revoke(CertDir, "rootCA", "revokedCA"),

    %% start a HTTP server to serve the CRLs
    %%
    %% NB: we use the 'stand_alone' option to link the server to the
    %% test process, so it exits when the test process exits.
    {ok, _HTTPPid} = inets:start(httpd, [{port, 8000}, {server_name, "localhost"},
                        {server_root, "/tmp"},
                        {document_root, CertDir},
                        {modules, [mod_get]}], stand_alone),

    lager:info("Deploy some nodes"),
    Conf = [{riak_core, [
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

    Nodes = rt:build_cluster(4, Conf),
    Node = hd(Nodes),
    %% enable security on the cluster
    ok = rpc:call(Node, riak_core_console, security_enable, [[]]),


    [_, {pb, {"127.0.0.1", Port}}] = rt:connection_info(Node),

    lager:info("Creating user"),
    %% grant the user credentials
    ok = rpc:call(Node, riak_core_console, add_user, [["user", "password=password"]]),

    lager:info("Setting password mode on user"),
    %% require password on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["user", "127.0.0.1/32",
                                                    "password"]]),

    CipherList = "AES256-SHA256:RC4-SHA",
    %% set a simple default cipher list, one good one a and one shitty one
    rpc:call(Node, riak_core_security, set_ciphers,
             [CipherList]),

    [AES, RC4] = ParsedCiphers = [begin
                %% this includes the pseudo random function, which apparently
                %% we don't want
                {A, B, C, _D} = ssl_cipher:suite_definition(E),
                {A, B, C}
            end ||
            E <- element(1,
                         riak_core_ssl_util:parse_ciphers(CipherList))],

    lager:info("Check that the server's preference for ECDHE-RSA-AES128-SHA256"
               "is honored"),
    ?assertEqual({ok, {'tlsv1.2', AES}},
                 pb_connection_info(Port,
                                    [{credentials, "user",
                                      "password"}, {cacertfile,
                                                    filename:join([CertDir,
                                                                   "rootCA/cert.pem"])},
                                     {ssl_opts, [{ciphers,
                                                  lists:reverse(ParsedCiphers)}]}
                                    ])),

    lager:info("disabling honor_cipher_info"),
    rpc:call(Node, application, set_env, [riak_api, honor_cipher_order,
                                          false]),

    lager:info("Check that the client's preference for RC4-SHA"
               "is honored"),
    ?assertEqual({ok, {'tlsv1.2', RC4}},
                 pb_connection_info(Port,
                                    [{credentials, "user",
                                      "password"}, {cacertfile,
                                                    filename:join([CertDir,
                                                                   "rootCA/cert.pem"])},
                                     {ssl_opts, [{ciphers,
                                                  lists:reverse(ParsedCiphers)}]}
                                    ])),

    lager:info("check that connections trying to use tls 1.1 fail"),
    ?assertError({badmatch, _},
                 pb_connection_info(Port,
                                    [{credentials, "user",
                                      "password"}, {cacertfile,
                                                    filename:join([CertDir,
                                                                   "rootCA/cert.pem"])},
                                     {ssl_opts, [{versions, ['tlsv1.1']}]}
                                    ])),

    lager:info("check that connections trying to use tls 1.0 fail"),
    ?assertError({badmatch, _},
                 pb_connection_info(Port,
                                    [{credentials, "user",
                                      "password"}, {cacertfile,
                                                    filename:join([CertDir,
                                                                   "rootCA/cert.pem"])},
                                     {ssl_opts, [{versions, ['tlsv1']}]}
                                    ])),
    lager:info("check that connections trying to use ssl 3.0 fail"),
    ?assertError({badmatch, _},
                 pb_connection_info(Port,
                                    [{credentials, "user",
                                      "password"}, {cacertfile,
                                                    filename:join([CertDir,
                                                                   "rootCA/cert.pem"])},
                                     {ssl_opts, [{versions, ['sslv3']}]}
                                    ])),

    lager:info("Enable ssl 3.0, tls 1.0 and tls 1.1 and disable tls 1.2"),
    rpc:call(Node, application, set_env, [riak_api, tls_protocols,
                                          [sslv3, tlsv1, 'tlsv1.1']]),

    lager:info("check that connections trying to use tls 1.2 fail"),
    ?assertError({badmatch, _},
                 pb_connection_info(Port,
                                    [{credentials, "user",
                                      "password"}, {cacertfile,
                                                    filename:join([CertDir,
                                                                   "rootCA/cert.pem"])},
                                     {ssl_opts, [{versions, ['tls1.2']}]}
                                    ])),

    lager:info("check tls 1.1 works"),
    ?assertMatch({ok, {'tlsv1.1', _}},
                 pb_connection_info(Port,
                                    [{credentials, "user",
                                      "password"}, {cacertfile,
                                                    filename:join([CertDir,
                                                                   "rootCA/cert.pem"])},
                                     {ssl_opts, [{versions, ['tlsv1.1']}]}
                                    ])),

    lager:info("check tls 1.0 works"),
    ?assertMatch({ok, {'tlsv1', _}},
                 pb_connection_info(Port,
                                    [{credentials, "user",
                                      "password"}, {cacertfile,
                                                    filename:join([CertDir,
                                                                   "rootCA/cert.pem"])},
                                     {ssl_opts, [{versions, ['tlsv1']}]}
                                    ])),

    lager:info("Reset tls protocols back to the default"),
    rpc:call(Node, application, set_env, [riak_api, tls_protocols,
                                          ['tlsv1.2']]),

    lager:info("checking CRLs are checked for client certificates by"
              " default"),

    ok = rpc:call(Node, riak_core_console, add_user, [["site5.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site5.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    lager:info("Checking revoked certificates are denied"),
    ?assertMatch({error, {tcp, _Reason}}, riakc_pb_socket:start("127.0.0.1", Port,
                                      [{credentials, "site5.basho.com",
                                        "password"},
                                       {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])},
                                       {certfile, filename:join([CertDir, "site5.basho.com/cert.pem"])},
                                       {keyfile, filename:join([CertDir, "site5.basho.com/key.pem"])}
                                      ])),

    lager:info("Disable CRL checking"),
    rpc:call(Node, application, set_env, [riak_api, check_crl,
                                          false]),

    lager:info("Checking revoked certificates are allowed"),
    {ok, PB} = riakc_pb_socket:start("127.0.0.1", Port,
                                     [{credentials, "site5.basho.com",
                                       ""},
                                      {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])},
                                      {certfile, filename:join([CertDir, "site5.basho.com/cert.pem"])},
                                      {keyfile, filename:join([CertDir, "site5.basho.com/key.pem"])}
                                     ]),
    ?assertEqual(pong, riakc_pb_socket:ping(PB)),
    riakc_pb_socket:stop(PB),
    pass.

pb_get_socket(PB) ->
    %% XXX this peeks into the pb_socket internal state and plucks out the
    %% socket. If the internal representation ever changes, this will break.
    element(6, sys:get_state(PB)).

pb_connection_info(Port, Config) ->
    {ok, PB} = riakc_pb_socket:start("127.0.0.1", Port, Config),
    ?assertEqual(pong, riakc_pb_socket:ping(PB)),

    ConnInfo = ssl:connection_information(pb_get_socket(PB)),

    riakc_pb_socket:stop(PB),
    ConnInfo.


