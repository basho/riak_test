-module(riak_control_upgrade).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-define(RC_ENABLE_CFG,
        [{riak_core,
          [
           {https, [{"127.0.0.1", 8069}]},
           {ssl,
            [{certfile, "./etc/cert.pem"},
             {keyfile, "./etc/key.pem"}
            ]}
          ]
         },
         {riak_control,
          [
           {enabled, true},
           {auth, none}
          ]
         }
        ]).

%% NOTE: Assumes config contains both `legacy' and `previous' settings.
confirm() ->
    verify_upgrade(legacy, host_rc_on_old),
    rt:setup_harness(ignored, ignored),
    verify_upgrade(legacy, host_rc_on_new),
    rt:setup_harness(ignored, ignored),
    verify_upgrade(previous, host_rc_on_old),
    rt:setup_harness(ignored, ignored),
    verify_upgrade(previous, host_rc_on_new),
    pass.

%% Verify an upgrade from the `FromVsn' version.  This assumes there
%% are `legacy' and `previous' keys in your `~/.riak_test.config'.
%% The `HostRCOn' parameter determines whether Riak Control is hosted
%% on the old or new version.
verify_upgrade(FromVsn, HostRCOn) ->
    lager:info("Verify upgrade ~p ~p", [FromVsn, HostRCOn]),
    Nodes = rt:build_cluster([{FromVsn, ?RC_ENABLE_CFG}|lists:duplicate(2, FromVsn)]),
    verify_alive(Nodes),
    UpgradeSeq = upgrade_seq(Nodes, HostRCOn),
    [upgrade_and_verify_alive(Nodes, ToUpgrade) || ToUpgrade <- UpgradeSeq].

verify_alive(Nodes) ->
    lager:info("Verify nodes ~p are alive", [Nodes]),
    [rt:wait_for_service(Node, riak_kv) || Node <- Nodes].

upgrade_and_verify_alive(Nodes, ToUpgrade) ->
    upgrade(ToUpgrade, current),
    %% Yes, sleeps suck, but I just want to give Riak Control a chance
    %% to crash the node (i.e. hit max restart frequency).
    timer:sleep(5000),
    verify_alive(Nodes).

%% Determine the upgrade sequence for `Nodes' based on `HostRCOn'.
%% This function assumes that the first node in `Nodes' is the RC
%% host.
upgrade_seq(Nodes, host_rc_on_old) ->
    %% Upgrade the RC host last because you want an old RC version to
    %% talk to new nodes.
    lists:reverse(Nodes);
upgrade_seq(Nodes, host_rc_on_new) ->
    %% Upgrade RC host first because you want new RC version to talk
    %% to old nodes.
    Nodes.

upgrade(Node, Vsn) ->
    lager:info("Upgrading ~p to ~p", [Node, Vsn]),
    rt:upgrade(Node, Vsn),
    rt:wait_for_service(Node, riak_kv),
    ok.
