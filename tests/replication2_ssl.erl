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

confirm() ->

    %% test requires allow_mult=false
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    NumNodes = rt_config:get(num_nodes, 6),
    ClusterASize = rt_config:get(cluster_a_size, 3),

    CertDir = rt_config:get(rt_scratch_dir) ++ "/certs",

    %% make a bunch of certificates and matching ci records
    make_certs:rootCA(CertDir, "rootCA"),
    make_certs:intermediateCA(CertDir, "intCA", "rootCA"),

    S1Name = ?SITEN(1),
    S2Name = ?SITEN(2),
    make_certs:endusers(CertDir, "intCA", [S1Name, S2Name]),
    CIsite1 = #ci{cn = S1Name, rd = 1, ssl = ssl_paths(?CERTP(S1Name))},
    CIsite2 = #ci{cn = S2Name, rd = 1, ssl = ssl_paths(?CERTP(S2Name))},

    S3Name = ?SITEN(3),
    S4Name = ?SITEN(4),
    make_certs:endusers(CertDir, "rootCA", [S3Name, S4Name]),
    CIsite3 = #ci{cn = S3Name, rd = 0, ssl = ssl_paths(?CERTP(S3Name))},
    CIsite4 = #ci{cn = S4Name, rd = 0, ssl = ssl_paths(?CERTP(S4Name))},

    WCName = ?CERTN("wildcard"),
    make_certs:enduser(CertDir, "intCA", ?DOM_WC, WCName),
    CIsiteWC = #ci{cn = WCName, rd = 1, ssl = ssl_paths(?CERTP(WCName))},

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
    %%

    %%
    %% Connection Test descriptors
    %% Each is a tuple: {Description, Node1Config, Node2Config, Should Pass}
    %%
    SslConnTests = [
        {"identical certificate CN is disallowed",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsite1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsite1#ci.ssl }],
            false},
        {"non-SSL peer fails",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsite1#ci.ssl }],
            ConfTcpBasic,
            false},
        {"non-SSL local fails",
            ConfTcpBasic,
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsite2#ci.ssl }],
            false},
        {"basic SSL connectivity",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsite1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsite2#ci.ssl }],
            true},
        {"SSL connectivity with intermediate CA",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsite1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsite3#ci.ssl }],
            true},
        {"wildcard certifictes on both ends",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsiteWC#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsiteWC#ci.ssl }],
            true},
        {"wildcard certifictes on one end",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsiteWC#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
            ] ++ CIsite3#ci.ssl }],
            true},
        %% first use of ssl_depth, all subsequent tests must specify it
        {"disallowing intermediate CA setting",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, 0}
            ] ++ CIsite3#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, 0}
            ] ++ CIsite4#ci.ssl }],
            true},
        {"disallowing intermediate CA connection",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, 0}
            ] ++ CIsite3#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIsite3#ci.rd}
            ] ++ CIsite1#ci.ssl }],
            false},
        %% first use of peer_common_name_acl, all subsequent tests must specify it
        {"wildcard certifictes on one end with ACL",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIsite1#ci.rd}
            ] ++ CIsiteWC#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIsiteWC#ci.rd}
                , {peer_common_name_acl, [?DOM_WC]}
            ] ++ CIsite1#ci.ssl }],
            true},
        {"wildcard and strict ACL",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIsite2#ci.rd}
                , {peer_common_name_acl, [?DOM_WC]}
            ] ++ CIsite1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIsite1#ci.rd}
                , {peer_common_name_acl, [CIsite1#ci.cn]}
            ] ++ CIsite2#ci.ssl }],
            true},
        {"wildcard certifictes on both ends with ACLs",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIsiteWC#ci.rd}
                , {peer_common_name_acl, [CIsiteWC#ci.wc]}
            ] ++ CIsiteWC#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIsiteWC#ci.rd}
                , {peer_common_name_acl, [CIsiteWC#ci.wc]}
            ] ++ CIsiteWC#ci.ssl }],
            true},
        {"expired certificates fail",
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIexpired#ci.rd}
                , {peer_common_name_acl, [CIexpired#ci.wc]}
            ] ++ CIsite1#ci.ssl }],
            [ConfRepl, {riak_core, [{ssl_enabled, true}
                , {ssl_depth, CIsite1#ci.rd}
                , {peer_common_name_acl, [CIsite1#ci.wc]}
            ] ++ CIexpired#ci.ssl }],
            false}
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
        , {ssl_depth, CIsite2#ci.rd}
        , {peer_common_name_acl, [CIsite2#ci.cn]}
    ] ++ CIsite1#ci.ssl }],
    ConfBNodes = [ConfRepl, {riak_core, [{ssl_enabled, true}
        , {ssl_depth, CIsite1#ci.rd}
        , {peer_common_name_acl, [CIsite1#ci.cn]}
    ] ++ CIsite2#ci.ssl }],
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
    ?assertMatch({fail, _}, test_connection(Left, Right)),
    lager:info("Connection rejected").

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

ssl_paths(Dir) ->
    ssl_paths(Dir, "cert.pem", "key.pem", "cacerts.pem").
ssl_paths(Dir, Cert, Key, CaCerts) ->
    [{certfile,     filename:join(Dir, Cert)}
        ,{keyfile,      filename:join(Dir, Key)}
        ,{cacertdir,    filename:join(Dir, CaCerts)}].
