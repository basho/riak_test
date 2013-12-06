-module(riak_repl2_leader_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl2_leader_orig).

set_leader_node3(LocalPid, LeaderNode, LeaderPid) ->
    io:format("Intercept triggered on node ~p with ~p ~p ~p",
              [node(), LocalPid, LeaderNode, LeaderPid]),
    ?I_INFO("Intercept triggered on node ~p with ~p ~p ~p",
            [node(), LocalPid, LeaderNode, LeaderPid]),
    ?M:set_leader_orig(LocalPid, 'dev3@127.0.0.1', undefined).

set_leader_node4(LocalPid, LeaderNode, LeaderPid) ->
    io:format("Intercept triggered on node ~p with ~p ~p ~p",
              [node(), LocalPid, LeaderNode, LeaderPid]),
    ?I_INFO("Intercept triggered on node ~p with ~p ~p ~p",
            [node(), LocalPid, LeaderNode, LeaderPid]),
    ?M:set_leader_orig(LocalPid, 'dev4@127.0.0.1', undefined).
