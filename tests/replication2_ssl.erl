-module(replication2_ssl).
-behavior(riak_test).
-export([confirm/0]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

confirm() ->

    %% test requires allow_mult=false
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    NumNodes = rt_config:get(num_nodes, 6),
    ClusterASize = rt_config:get(cluster_a_size, 3),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/certs",

    %% make a bunch of crypto keys
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:intermediateCA(CertDir, "intCA", "rootCA"),
    make_certs:endusers(CertDir, "rootCA", ["site3.basho.com", "site4.basho.com"]),
    make_certs:endusers(CertDir, "intCA", ["site1.basho.com", "site2.basho.com"]),
    make_certs:enduser(CertDir, "intCA", "*.basho.com", "wildcard.basho.com"),

    lager:info("Deploy ~p nodes", [NumNodes]),
    BaseConf = [
        {riak_core,
            [
                {ssl_enabled, false}
            ]},
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]}
    ],

    PrivDir = rt:priv_dir(),

    SSLConfig1 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_core,
            [
                {ssl_enabled, true},
                {certfile, filename:join([CertDir,
                            "site1.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site1.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site1.basho.com/cacerts.pem"])}
            ]}
    ],

    SSLConfig2 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_core,
            [
                {ssl_enabled, true},
                {certfile, filename:join([CertDir,
                            "site2.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site2.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site2.basho.com/cacerts.pem"])}
            ]}
    ],

    SSLConfig3 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_core,
            [
                {ssl_enabled, true},
                {certfile, filename:join([CertDir,
                            "site3.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site3.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site3.basho.com/cacerts.pem"])}
            ]}
    ],

    %% same as above,with a depth of 0
    SSLConfig3A = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_core,
            [

                {ssl_enabled, true},
                {ssl_depth, 0},
                {certfile, filename:join([CertDir,
                            "site3.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site3.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site3.basho.com/cacerts.pem"])}
            ]}
    ],

    SSLConfig4 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_core,
            [
                {ssl_enabled, true},
                {ssl_depth, 0},
                {certfile, filename:join([CertDir,
                            "site4.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site4.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site4.basho.com/cacerts.pem"])}
            ]}
    ],

    SSLConfig5 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_core,
            [
                {ssl_enabled, true},
                {ssl_depth, 1},
                {peer_common_name_acl, ["*.basho.com"]},
                {certfile, filename:join([CertDir,
                            "site1.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site1.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site1.basho.com/cacerts.pem"])}
            ]}
    ],

    SSLConfig6 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_core,
            [
                {ssl_enabled, true},
                {ssl_depth, 1},
                {peer_common_name_acl, ["site1.basho.com"]},
                {certfile, filename:join([CertDir,
                            "site2.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "site2.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "site2.basho.com/cacerts.pem"])}
            ]}
    ],

    SSLConfig7 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_core,
            [
                {ssl_enabled, true},
                {peer_common_name_acl, ["ca.cataclysm-software.net"]},
                {certfile, filename:join([PrivDir,
                            "certs/cacert.org/ny-cert-old.pem"])},
                {keyfile, filename:join([PrivDir,
                            "certs/cacert.org/ny-key.pem"])},
                {cacertdir, filename:join([PrivDir,
                            "certs/cacert.org/ca"])}
            ]}
    ],

    SSLConfig8 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_core,
            [
                {ssl_enabled, true},
                {certfile, filename:join([CertDir,
                            "wildcard.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                            "wildcard.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                            "wildcard.basho.com/cacerts.pem"])}
            ]}
    ],

    SSLConfig9 = [
        {riak_repl,
            [
                {fullsync_on_connect, false},
                {fullsync_interval, disabled}
            ]},
        {riak_core,
            [
                {ssl_enabled, true},
                {peer_common_name_acl, ["*.basho.com"]},
                {certfile, filename:join([CertDir,
                    "wildcard.basho.com/cert.pem"])},
                {keyfile, filename:join([CertDir,
                    "wildcard.basho.com/key.pem"])},
                {cacertdir, filename:join([CertDir,
                    "wildcard.basho.com/cacerts.pem"])}
            ]}
    ],

    lager:info("===testing basic connectivity"),

    [Node1, Node2] = rt:deploy_nodes(2, BaseConf, [riak_kv, riak_repl]),

    repl_util:name_cluster(Node1, "A"),
    repl_util:name_cluster(Node2, "B"),

    %% we'll need to wait for cluster names before continuing
    rt:wait_until_ring_converged([Node1]),
    rt:wait_until_ring_converged([Node2]),

    rt:wait_for_service(Node1, [riak_kv, riak_repl]),
    rt:wait_for_service(Node2, [riak_kv, riak_repl]),

    rt:log_to_nodes([Node1, Node2], "Basic connectivity test"),

    {ok, {_IP, Port}} = rpc:call(Node2, application, get_env,
        [riak_core, cluster_mgr]),
    lager:info("connect cluster A:~p to B on port ~p", [Node1, Port]),
    rt:log_to_nodes([Node1, Node2], "connect A to B"),
    repl_util:connect_cluster(Node1, "127.0.0.1", Port),
    lager:info("Waiting for connection to B"),

    ?assertEqual(ok, repl_util:wait_for_connection(Node1, "B")),

    lager:info("===testing you can't connect to a server with a cert with the same common name"),
    rt:log_to_nodes([Node1, Node2], "Testing identical cert is disallowed"),
    ?assertMatch({fail, _}, test_connection({Node1, merge_config(SSLConfig1, BaseConf)},
            {Node2, merge_config(SSLConfig1, BaseConf)})),

    lager:info("===testing you can't connect when peer doesn't support SSL"),
    rt:log_to_nodes([Node1, Node2], "Testing missing ssl on peer fails"),
    ?assertMatch({fail, _}, test_connection({Node1, merge_config(SSLConfig1, BaseConf)},
            {Node2, BaseConf})),

    lager:info("===testing you can't connect when local doesn't support SSL"),
    rt:log_to_nodes([Node1, Node2], "Testing missing ssl locally fails"),
    ?assertMatch({fail, _}, test_connection({Node1, BaseConf},
            {Node2, merge_config(SSLConfig2, BaseConf)})),

    lager:info("===testing simple SSL connectivity"),
    rt:log_to_nodes([Node1, Node2], "Basic SSL test"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig1, BaseConf)},
            {Node2, merge_config(SSLConfig2, BaseConf)})),

    lager:info("testing SSL connectivity with an intermediate CA"),
    rt:log_to_nodes([Node1, Node2], "Intermediate CA test"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig1, BaseConf)},
            {Node2, merge_config(SSLConfig3, BaseConf)})),

    lager:info("===testing disallowing intermediate CAs works"),
    rt:log_to_nodes([Node1, Node2], "Disallowing intermediate CA test"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig3A, BaseConf)},
            {Node2, merge_config(SSLConfig4, BaseConf)})),

    lager:info("===testing disallowing intermediate CAs disallows connections"),
    rt:log_to_nodes([Node1, Node2], "Disallowing intermediate CA test 2"),
    ?assertMatch({fail, _}, test_connection({Node1, merge_config(SSLConfig3A, BaseConf)},
            {Node2, merge_config(SSLConfig1, BaseConf)})),

    lager:info("===testing wildcard and strict ACLs"),
    rt:log_to_nodes([Node1, Node2], "wildcard and strict ACL test"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig5, BaseConf)},
            {Node2, merge_config(SSLConfig6, BaseConf)})),

    %% WILDCARD CERT TESTS

    lager:info("===testing wildcard certifictes on both ends"),
    rt:log_to_nodes([Node1, Node2], "wildcard certificates test - both end"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig8, BaseConf)},
        {Node2, merge_config(SSLConfig8, BaseConf)})),

    lager:info("===testing wildcard certifictes on one end"),
    rt:log_to_nodes([Node1, Node2], "wildcard certificates test - one end"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig8, BaseConf)},
        {Node2, merge_config(SSLConfig3, BaseConf)})),

    lager:info("===testing wildcard certifictes on one end with ACL"),
    rt:log_to_nodes([Node1, Node2], "wildcard certificates test - one end with ACLs"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig8, BaseConf)},
        {Node2, merge_config(SSLConfig5, BaseConf)})),

    lager:info("===testing wildcard certifictes on both ends with ACLs"),
    rt:log_to_nodes([Node1, Node2], "wildcard certificates test - both end with ACLs"),
    ?assertEqual(ok, test_connection({Node1, merge_config(SSLConfig9, BaseConf)},
        {Node2, merge_config(SSLConfig9, BaseConf)})),

    %% END WILDCARD CERT TESTS

    lager:info("===testing expired certificates fail"),
    rt:log_to_nodes([Node1, Node2], "expired certificates test"),
    ?assertMatch({fail, _}, test_connection({Node1, merge_config(SSLConfig5, BaseConf)},
            {Node2, merge_config(SSLConfig7, BaseConf)})),

    lager:info("Connectivity tests passed"),

    repl_util:disconnect_cluster(Node1, "B"),

    lager:info("Re-deploying 6 nodes"),

    Nodes = rt:deploy_nodes(6, BaseConf, [riak_kv, riak_repl]),

    [rt:wait_until_pingable(N) || N <- Nodes],

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),

    lager:info("Reconfiguring nodes with SSL options"),
    [rt:update_app_config(N, merge_config(SSLConfig5, BaseConf)) || N <-
        ANodes],

    [rt:update_app_config(N, merge_config(SSLConfig6, BaseConf)) || N <-
        BNodes],

    [rt:wait_until_pingable(N) || N <- Nodes],

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    repl_util:disconnect_cluster(Node1, "B"),

    replication2:replication(ANodes, BNodes, false),

    pass.

merge_config(Mixin, Base) ->
    lists:ukeymerge(1, lists:keysort(1, Mixin), lists:keysort(1, Base)).

test_connection({Node1, Config1}, {Node2, Config2}) ->
    repl_util:disconnect_cluster(Node1, "B"),
    repl_util:wait_for_disconnect(Node1, "B"),
    rt:update_app_config(Node2, Config2),
    rt:wait_until_pingable(Node2),
    rt:update_app_config(Node1, Config1),
    rt:wait_until_pingable(Node1),
    rt:wait_for_service(Node1, [riak_kv, riak_repl]),
    rt:wait_for_service(Node2, [riak_kv, riak_repl]),
    {ok, {_IP, Port}} = rpc:call(Node2, application, get_env,
        [riak_core, cluster_mgr]),
    lager:info("connect cluster A:~p to B on port ~p", [Node1, Port]),
    rt:log_to_nodes([Node1, Node2], "connect A to B"),
    repl_util:connect_cluster(Node1, "127.0.0.1", Port),
    repl_util:wait_for_connection(Node1, "B").
