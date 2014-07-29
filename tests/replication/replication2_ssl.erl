%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2015 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(replication2_ssl).
-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

%% Certificate Names
-define(DEF_DOM,    ".basho.com").
-define(DOM_WC,     "*" ++ ?DEF_DOM).
-define(BAD_WC,     "*.bahso.com").
-define(CERTN(S),   S ++ ?DEF_DOM).
-define(SITEN(N),   ?CERTN("site" ++ ??N)).
-define(CERTP(S),   filename:join(CertDir, S)).

%% Certificate Information
-record(ci, {
    cn,             %% common name of the certificate
    rd = 0,         %% required ssl_depth
    wc = ?DOM_WC,   %% acceptable *.domain wildcard
    ssl             %% options returned from ssl_paths
}).

%%
%%  @doc    Tests various TLS (SSL) connection scenarios for MDC.
%%  The following configiration options are recognized:
%%
%%  num_nodes       [default 6]
%%  How many nodes to use to build two clusters.
%%
%%  cluster_a_size  [default (num_nodes div 2)]
%%  How many nodes to use in cluster "A". The remainder is used in cluster "B".
%%
%%  conn_fail_time  [default rt_max_wait_time]
%%  A (presumably shortened) timout to use in tests where the connection is
%%  expected to be rejected due to invalid TLS configurations. Something around
%%  one minute is appropriate. Using the default ten-minute timeout, this
%%  test will take more than an hour and a half to run successfully.
%%
confirm() ->

    %% test requires allow_mult=false
    rt_config:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    NumNodes = rt_config:get(num_nodes, 6),
    ClusterASize = rt_config:get(cluster_a_size, (NumNodes div 2)),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/certs",

    %% make some CAs
    make_certs:rootCA(CertDir, "CA_0"),
    make_certs:intermediateCA(CertDir, "CA_1", "CA_0"),
    make_certs:intermediateCA(CertDir, "CA_2", "CA_1"),

    %% make a bunch of certificates and matching ci records
    S1Name = ?SITEN(1),
    S2Name = ?SITEN(2),
    S3Name = ?SITEN(3),
    S4Name = ?SITEN(4),
    S5Name = ?SITEN(5),
    S6Name = ?SITEN(6),
    W1Name = ?CERTN("wildcard1"),
    W2Name = ?CERTN("wildcard2"),

    make_certs:endusers(CertDir, "CA_0", [S1Name, S2Name]),
    CIdep0s1 = #ci{cn = S1Name, rd = 0, ssl = ssl_paths(?CERTP(S1Name))},
    CIdep0s2 = #ci{cn = S2Name, rd = 0, ssl = ssl_paths(?CERTP(S2Name))},

    make_certs:endusers(CertDir, "CA_1", [S3Name, S4Name]),
    CIdep1s1 = #ci{cn = S3Name, rd = 1, ssl = ssl_paths(?CERTP(S3Name))},
    CIdep1s2 = #ci{cn = S4Name, rd = 1, ssl = ssl_paths(?CERTP(S4Name))},

    make_certs:endusers(CertDir, "CA_2", [S5Name, S6Name]),
    CIdep2s1 = #ci{cn = S5Name, rd = 2, ssl = ssl_paths(?CERTP(S5Name))},
    CIdep2s2 = #ci{cn = S6Name, rd = 2, ssl = ssl_paths(?CERTP(S6Name))},

    make_certs:enduser(CertDir, "CA_1", ?DOM_WC, W1Name),
    CIdep1wc = #ci{cn = ?DOM_WC, rd = 1, ssl = ssl_paths(?CERTP(W1Name))},

    make_certs:enduser(CertDir, "CA_2", ?DOM_WC, W2Name),
    CIdep2wc = #ci{cn = ?DOM_WC, rd = 2, ssl = ssl_paths(?CERTP(W2Name))},

    % crufty old certs really need to be replaced
    CIexpired = #ci{cn = "ny.cataclysm-software.net", rd = 0,
        wc = "*.cataclysm-software.net", ssl = ssl_paths(
            filename:join([rt:priv_dir(), "certs", "cacert.org"]),
            "ny-cert-old.pem", "ny-key.pem", "ca")},

    lager:info("Deploy ~p nodes", [NumNodes]),

    ConfRepl = {riak_repl,
        [{fullsync_on_connect, false}, {fullsync_interval, disabled}]},

    ConfTcpBasic = [ConfRepl, {riak_core, [{ssl_enabled, false}]}],

    %%
    %% !!! IMPORTANT !!!
    %% Properties added to node configurations CANNOT currently be removed,
    %% only overwritten.  As such, configurations that include ACLs MUST come
    %% after ALL non-ACL configurations!  This has been learned the hard way :(
    %% The same applies to the ssl_depth option, though it's much easier to
    %% contend with - make sure it works, then always use a valid depth.
    %%

    %%
    %% Connection Test descriptors
    %% Each is a tuple: {Description, Node1Config, Node2Config, Should Pass}
    %%
    SslConnTests = [
        %%
        %% basic tests
        %%
        {"non-SSL peer fails",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep0s1#ci.ssl }],
            ConfTcpBasic,
            false},
        {"non-SSL local fails",
            ConfTcpBasic,
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep0s2#ci.ssl }],
            false},
        {"basic SSL connectivity",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep0s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep0s2#ci.ssl }],
            true},
        {"expired peer certificate fails",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep0s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIexpired#ci.ssl }],
            false},
        {"expired local certificate fails",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIexpired#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep0s2#ci.ssl }],
            false},
        {"identical certificate CN is disallowed",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep0s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep0s1#ci.ssl }],
            false},
        {"identical wildcard certificate CN is allowed",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep1wc#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep1wc#ci.ssl }],
            true},
        {"SSL connectivity with one intermediate CA is allowed by default",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep1s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep1s2#ci.ssl }],
            true},
        {"SSL connectivity with two intermediate CAs is disallowed by default",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep2s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep2s2#ci.ssl }],
            false},
        {"wildcard certificates on both ends",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep1wc#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep1wc#ci.ssl }],
            true},
        {"wildcard certificate on one end",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep1wc#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIdep0s1#ci.ssl }],
            true},
        %%
        %% first use of ssl_depth, all subsequent tests must specify
        %%
        {"disallowing intermediate CA setting allows direct-signed certs",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, 0}
            ] ++ CIdep0s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, 0}
            ] ++ CIdep0s2#ci.ssl }],
            true},
        {"disallowing intermediate CA disallows intermediate-signed peer",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, 0}
            ] ++ CIdep0s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep0s1#ci.rd}
            ] ++ CIdep1s2#ci.ssl }],
            false},
        {"disallowing intermediate CA disallows intermediate-signed local",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep0s2#ci.rd}
            ] ++ CIdep1s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, 0}
            ] ++ CIdep0s2#ci.ssl }],
            false},
        {"allow arbitrary-depth intermediate CAs",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep2s2#ci.rd}
            ] ++ CIdep2s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep2s1#ci.rd}
            ] ++ CIdep2s2#ci.ssl }],
            true},
        %%
        %% first use of peer_common_name_acl, all subsequent tests must specify
        %%
        {"wildcard certificate on one end with matching ACL",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep1s1#ci.rd}
            ] ++ CIdep1wc#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep1wc#ci.rd}
                , {peer_common_name_acl, [?DOM_WC]}
            ] ++ CIdep1s1#ci.ssl }],
            true},
        {"wildcard certificate on one end with mismatched ACL",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep1s1#ci.rd}
            ] ++ CIdep1wc#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep1wc#ci.rd}
                , {peer_common_name_acl, [?BAD_WC]}
            ] ++ CIdep1s1#ci.ssl }],
            false},
        {"one wildcard ACL and one strict ACL",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep1s2#ci.rd}
                , {peer_common_name_acl, [?DOM_WC]}
            ] ++ CIdep1s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep1s1#ci.rd}
                , {peer_common_name_acl, [CIdep1s1#ci.cn]}
            ] ++ CIdep1s2#ci.ssl }],
            true},
        {"wildcard certificates on both ends with ACLs",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep2wc#ci.rd}
                , {peer_common_name_acl, [CIdep2wc#ci.wc]}
            ] ++ CIdep1wc#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep1wc#ci.rd}
                , {peer_common_name_acl, [CIdep1wc#ci.wc]}
            ] ++ CIdep2wc#ci.ssl }],
            true},
        {"explicit certificates with strict ACLs",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep2s2#ci.rd}
                , {peer_common_name_acl, [CIdep2s2#ci.cn]}
            ] ++ CIdep1s1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIdep1s1#ci.rd}
                , {peer_common_name_acl, [CIdep1s1#ci.cn]}
            ] ++ CIdep2s2#ci.ssl }],
            true}
    ],

    lager:info("Deploying 2 nodes for connectivity tests"),

    [Node1, Node2] = rt:deploy_nodes(2, ConfTcpBasic, [riak_kv, riak_repl]),

    repl_util:name_cluster(Node1, "A"),
    repl_util:name_cluster(Node2, "B"),

    %% we'll need to wait for cluster names before continuing
    rt:wait_until_ring_converged([Node1]),
    rt:wait_until_ring_converged([Node2]),

    rt:wait_for_service(Node1, [riak_kv, riak_repl]),
    rt:wait_for_service(Node2, [riak_kv, riak_repl]),

    lager:info("=== Testing basic connectivity"),
    rt:log_to_nodes([Node1, Node2], "Testing basic connectivity"),

    {ok, {_IP, Port}} = rpc:call(Node2, application, get_env,
        [riak_core, cluster_mgr]),
    lager:info("connect cluster A:~p to B on port ~p", [Node1, Port]),
    rt:log_to_nodes([Node1, Node2], "connect A to B"),
    repl_util:connect_cluster(Node1, "127.0.0.1", Port),
    lager:info("Waiting for connection to B"),

    ?assertEqual(ok, repl_util:wait_for_connection(Node1, "B")),

    %% run each of the SSL connectivity tests
    lists:foreach(fun({Desc, Conf1, Conf2, ShouldPass}) ->
            test_connection(Desc, {Node1, Conf1}, {Node2, Conf2}, ShouldPass)
        end, SslConnTests),

    lager:info("Connectivity tests passed"),

    repl_util:disconnect_cluster(Node1, "B"),

    lager:info("Re-deploying 6 nodes"),

    Nodes = rt:deploy_nodes(6, ConfTcpBasic, [riak_kv, riak_repl]),

    [rt:wait_until_pingable(N) || N <- Nodes],

    {ANodes, BNodes} = lists:split(ClusterASize, Nodes),

    lager:info("Reconfiguring nodes with SSL options"),
    ConfANodes = [ConfRepl, {riak_core, [{ssl_enabled, true}
        , {ssl_depth, CIdep1s2#ci.rd}
        , {peer_common_name_acl, [CIdep1s2#ci.cn]}
    ] ++ CIdep1s1#ci.ssl }],
    ConfBNodes = [ConfRepl, {riak_core, [{ssl_enabled, true}
        , {ssl_depth, CIdep1s1#ci.rd}
        , {peer_common_name_acl, [CIdep1s1#ci.cn]}
    ] ++ CIdep1s2#ci.ssl }],
    [rt:update_app_config(N, ConfANodes) || N <- ANodes],
    [rt:update_app_config(N, ConfBNodes) || N <- BNodes],

    [rt:wait_until_pingable(N) || N <- Nodes],

    lager:info("Build cluster A"),
    repl_util:make_cluster(ANodes),

    lager:info("Build cluster B"),
    repl_util:make_cluster(BNodes),

    repl_util:disconnect_cluster(Node1, "B"),

    replication2:replication(ANodes, BNodes, false),

    pass.

test_connection(Desc, {N1, C1}, {N2, C2}, ShouldPass) ->
    lager:info("=== Testing " ++ Desc),
    rt:log_to_nodes([N1, N2], "Testing " ++ Desc),
    test_connection({N1, C1}, {N2, C2}, ShouldPass).

test_connection(Left, Right, true) ->
    ?assertEqual(ok, test_connection(Left, Right)),
    lager:info("Connection succeeded");
test_connection(Left, Right, false) ->
    DefaultTimeout = rt_config:get(rt_max_wait_time),
    ConnFailTimeout = rt_config:get(conn_fail_time, DefaultTimeout),
    rt_config:set(rt_max_wait_time, ConnFailTimeout),
    ?assertMatch({fail, _}, test_connection(Left, Right)),
    rt_config:set(rt_max_wait_time, DefaultTimeout),
    lager:info("Connection rejected").

test_connection({Node1, Config1}, {Node2, Config2}) ->
    repl_util:disconnect_cluster(Node1, "B"),
    repl_util:wait_for_disconnect(Node1, "B"),
    rt_config:update_app_config(Node2, Config2),
    rt:wait_until_pingable(Node2),
    rt_config:update_app_config(Node1, Config1),
    rt:wait_until_pingable(Node1),
    rt:wait_for_service(Node1, [riak_kv, riak_repl]),
    rt:wait_for_service(Node2, [riak_kv, riak_repl]),
    {ok, {_IP, Port}} = rpc:call(Node2, application, get_env,
        [riak_core, cluster_mgr]),
    lager:info("connect cluster A:~p to B on port ~p", [Node1, Port]),
    rt:log_to_nodes([Node1, Node2], "connect A to B"),
    repl_util:connect_cluster(Node1, "127.0.0.1", Port),
    repl_util:wait_for_connection(Node1, "B").

ssl_paths(Dir) ->
    ssl_paths(Dir, "cert.pem", "key.pem", "cacerts.pem").
ssl_paths(Dir, Cert, Key, CaCerts) ->
    [{certfile,     filename:join(Dir, Cert)}
        ,{keyfile,      filename:join(Dir, Key)}
        ,{cacertdir,    filename:join(Dir, CaCerts)}].
