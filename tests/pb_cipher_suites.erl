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

    CipherList =
        "ECDHE-RSA-AES256-SHA384:ECDH-ECDSA-AES128-SHA:ECDH-ECDSA-AES256-SHA384",


    %% set a simple default cipher list, one good one a and one shitty one
    rpc:call(Node, riak_core_security, set_ciphers, [CipherList]),
    rpc:call(Node, application, set_env, [riak_api, honor_cipher_order, true]),

    GoodCiphers = element(1, riak_core_ssl_util:parse_ciphers(CipherList)),
    lager:info("Good ciphers: ~p", [GoodCiphers]),
    [AES256, AES128, _ECDSA] = 
        ParsedCiphers = 
            lists:map(fun(PC) -> cipher_format(PC) end, GoodCiphers),
    
    lager:info("Parsed Ciphers ~w", [ParsedCiphers]),

    lager:info("Check that the server's preference for ECDHE-RSA-AES256-SHA384"
               " is honored"),
    AES256T = convert_suite_to_tuple(AES256),
    {ok, {'tlsv1.2', AES256R}} =
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                            {cacertfile,
                                filename:join([CertDir, "rootCA/cert.pem"])},
                            {ssl_opts,
                                [{ciphers, ParsedCiphers}]}
                            ]),
    lager:info("With cipher order - ~w", [AES256R]),
    ?assertEqual(AES256T, {element(1, AES256R),
                            element(2, AES256R),
                            element(3, AES256R)}),
    lager:info("Ignoring reversal of cipher order!!"),
    {ok, {'tlsv1.2', AES256R}} =
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                            {cacertfile,
                                filename:join([CertDir, "rootCA/cert.pem"])},
                            {ssl_opts,
                                [{ciphers,
                                    lists:reverse(ParsedCiphers)}]}
                            ]),
    
    lager:info("Do we assume that cipher order is not honoured?"),
    
    SingleCipherProps =
        [{credentials, "user", "password"},
            {cacertfile, filename:join([CertDir, "rootCA/cert.pem"])},
            {ssl_opts, [{ciphers, [AES128]}]}],
    lager:info("Setting weak cipher now throws insufficient security"),
    insufficient_check(Port, SingleCipherProps),

    lager:info("check that connections trying to use tls 1.1 fail"),
    {error,{tcp,{tls_alert,ProtocolVersionError}}} =
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                                {cacertfile,
                                    filename:join([CertDir,
                                                    "rootCA/cert.pem"])},
                                {ssl_opts, [{versions, ['tlsv1.1']}]}
                            ]),

    lager:info("check that connections trying to use tls 1.0 fail"),
    {error,{tcp,{tls_alert,ProtocolVersionError}}} =
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                                {cacertfile,
                                    filename:join([CertDir,
                                                    "rootCA/cert.pem"])},
                                {ssl_opts, [{versions, ['tlsv1']}]}
                            ]),
    lager:info("check that connections trying to use ssl 3.0 fail"),
    OTP24SSL3Error =
        {error,{tcp,{options,{sslv3,{versions,[sslv3]}}}}},
    OTP22SSL3Error =
        {error,{tcp,{tls_alert,ProtocolVersionError}}},
    SSL3Error =
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                                {cacertfile,
                                    filename:join([CertDir,
                                                    "rootCA/cert.pem"])},
                                    {ssl_opts, [{versions, ['sslv3']}]}
                                    ]),
    ?assert(lists:member(SSL3Error, [OTP22SSL3Error, OTP24SSL3Error])),

    lager:info("Enable ssl 3.0, tls 1.0 and tls 1.1 and disable tls 1.2"),
    rpc:call(Node, application, set_env, [riak_api, tls_protocols,
                                            [sslv3, tlsv1, 'tlsv1.1']]),

    lager:info("check that connections trying to use tls 1.2 fail"),
    ?assertMatch({error,{tcp,{options,{'tls1.2',{versions,['tls1.2']}}}}},
                    pb_connection_info(Port,
                                    [{credentials, "user",
                                        "password"}, {cacertfile,
                                                    filename:join([CertDir,
                                                                    "rootCA/cert.pem"])},
                                        {ssl_opts, [{versions, ['tls1.2']}]}
                                    ])),

    lager:info("Re-enabling old protocols will work in OTP 22"),
    check_with_reenabled_protools(Port, CertDir),

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

    ok = check_reasons(ProtocolVersionError),

    pass.

pb_get_socket(PB) ->
    %% XXX this peeks into the pb_socket internal state and plucks out the
    %% socket. If the internal representation ever changes, this will break.
    element(6, sys:get_state(PB)).

pb_connection_info(Port, Config) ->
    case riakc_pb_socket:start("127.0.0.1", Port, Config) of
        {ok, PB} ->
            ?assertEqual(pong, riakc_pb_socket:ping(PB)),
            {ok, ConnInfo} = ssl:connection_information(pb_get_socket(PB)),
            {protocol, P} = lists:keyfind(protocol, 1, ConnInfo),
            {selected_cipher_suite, CS} =
                lists:keyfind(selected_cipher_suite, 1, ConnInfo),
            riakc_pb_socket:stop(PB),
            {ok, {P, convert_suite_to_tuple(CS)}};
        Error ->
            Error
    end.


convert_suite_to_tuple(CS) when is_tuple(CS) ->
    CS;
convert_suite_to_tuple(CS) when is_map(CS) ->
    {maps:get(key_exchange, CS), 
        maps:get(cipher, CS),
        maps:get(mac, CS)}.


cipher_format(Cipher) ->
    Cipher.

insufficient_check(Port, SingleCipherProps) ->
    {error,
        {tcp,
            {tls_alert,
                {insufficient_security, _ErrorMsg}}}} =
        pb_connection_info(Port, SingleCipherProps).

check_reasons(
    {protocol_version,
        "TLS client: In state hello received SERVER ALERT: Fatal - Protocol Version\n "}) ->
    ok;
check_reasons(
    {protocol_version,
        "TLS client: In state hello received SERVER ALERT: Fatal - Protocol Version\n"}) ->
    ok;
check_reasons(ProtocolVersionError) ->
    lager:info("Unexpected error ~s", [ProtocolVersionError]),
    error.


-ifdef(post_22).

check_with_reenabled_protools(_Port, _CertDir) -> ok.

-else.

check_with_reenabled_protools(Port, CertDir) ->
    lager:info("Check tls 1.1 succeeds - OK as long as cipher good?"),
    lager:info("Note that only 3-tuple returned for 1.1 - and sha not sha384"),
    ?assertMatch({ok,{'tlsv1.1',{ecdhe_rsa,aes_256_cbc,sha}}},
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                                {cacertfile,
                                    filename:join([CertDir,
                                                    "rootCA/cert.pem"])},
                            {ssl_opts, [{versions, ['tlsv1.1']}]}
                        ])),

    lager:info("check tls 1.0 succeeds - OK as long as cipher is good?"),
    ?assertMatch({ok,{'tlsv1',{ecdhe_rsa,aes_256_cbc,sha}}},
        pb_connection_info(Port,
                            [{credentials, "user", "password"},
                                    {cacertfile,
                                        filename:join([CertDir,
                                                        "rootCA/cert.pem"])},
                                {ssl_opts, [{versions, ['tlsv1']}]}
                            ])).

-endif.