-module(client_security_utils).

-export([setup/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

setup() ->
    application:start(crypto),
    application:start(asn1),
    application:start(public_key),
    application:start(ssl),
    application:start(inets),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/certs",

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
    inets:start(httpd, [{port, 8000}, {server_name, "localhost"},
                        {server_root, "/tmp"},
                        {document_root, CertDir},
                        {modules, [mod_get]}]),

    lager:info("Deploy some nodes"),
    _PrivDir = rt:priv_dir(),

    Conf = [{riak_kv, [
                        {secondary_index_sort_default, true}
                      ]},
            {riak_core, [
                            {default_bucket_props, [{allow_mult, true}]}
                        ]},
            {riak_api, [
                    {certfile, filename:join([CertDir,"site3.basho.com/cert.pem"])},
                    {keyfile, filename:join([CertDir, "site3.basho.com/key.pem"])},
                    {cacertfile, filename:join([CertDir, "site3.basho.com/cacerts.pem"])}
                    ]},
            {riak_search, [
                           {enabled, true}
                          ]}
           ],

    MD = riak_test_runner:metadata(),
    _HaveIndexes = case proplists:get_value(backend, MD) of
                      undefined -> false; %% default is da 'cask
                      bitcask -> false;
                      _ -> true
                  end,

    Nodes = rt:deploy_nodes(1, Conf),
    Node = hd(Nodes),
    [rt:wait_for_service(N, riak_search) || N <- Nodes],
    %% enable security on the cluster
    ok = rpc:call(Node, riak_core_console, security_enable, [[]]),

    %[_, {pb, {"127.0.0.1", Port}}] = rt:connection_info(Node),

    lager:info("Creating users"),


    %% grant the user credentials
    ok = rpc:call(Node, riak_core_console, add_user, [["testuser1", "password=testpassword1"]]),
    ok = rpc:call(Node, riak_core_console, add_user, [["testuser2", "password=testpassword2"]]),

    lager:info("Setting trust mode on user"),
    %% trust 'user' on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["testuser1", "127.0.0.1/32",
                                                    "trust"]]),

    lager:info("Setting password mode on user"),
    %% require password on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["testuser2", "127.0.0.1/32",
                                                            "testpassword2"]]),

    lager:info("Creating a certificate-authenticated user"),
    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site4.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site4.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    lager:info("Creating another cert-auth user"),
    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site5.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site5.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),


    lager:info("cert from intermediate CA should work"),
    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site1.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site1.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    lager:info("checking certificates from a revoked CA are denied"),
    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site6.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site6.basho.com",
                                                         "127.0.0.1/32",

                                                         "certificate"]]),

    lager:info("checking a certificate signed by a leaf CA is not honored"),
    %% grant the user credential
    ok = rpc:call(Node, riak_core_console, add_user, [["site7.basho.com"]]),

    %% require certificate auth on localhost
    ok = rpc:call(Node, riak_core_console, add_source, [["site7.basho.com",
                                                         "127.0.0.1/32",
                                                         "certificate"]]),

    grant(Node, ["riak_kv.put", "ON", "default", "hello", "TO", "testuser1"]),

    %% 1.4 counters
    %%
    grant(Node, ["riak_kv.put,riak_kv.get", "ON", "default", "counters", "TO", "testuser1"]),
    Nodes.
