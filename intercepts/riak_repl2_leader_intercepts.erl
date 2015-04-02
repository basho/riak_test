-module(riak_repl2_leader_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl2_leader_orig).
-define(NODE_KEY, 'riak_repl2_leader_node').

leader_server(Node) ->
    receive
        {From, _} ->
            io:format("Letting ~p know that ~p is the leader~n", [From, Node]),
            lager:debug("Letting ~p know that ~p is the leader", [From, Node]),
            From ! {self(), Node},
            leader_server(Node)
    end.

set_leader_node(Node) ->
    LeaderPid = spawn(?MODULE, leader_server, [Node]),
    global:unregister_name(?NODE_KEY),
    global:register_name(?NODE_KEY, LeaderPid).

set_leader_node(LocalPid, LeaderNode, LeaderPid) ->
    io:format("Intercept triggered on node ~p with ~p ~p ~p",
              [node(), LocalPid, LeaderNode, LeaderPid]),
    ?I_INFO("Intercept triggered on node ~p with ~p ~p ~p",
            [node(), LocalPid, LeaderNode, LeaderPid]),

    NewLeader = case global:send(?NODE_KEY, {self(), node}) of
        {badarg, {Name, _Msg}} ->
            ?I_INFO("Can't find global registered process: ~p", [Name]),
            badarg;
        Pid when is_pid(Pid) ->
            receive
                {Pid, Node} ->
                    ?I_INFO("Got your node! ~p", [Node]),
                    Node
            after
                15000 ->
                    ?I_INFO("Timed out", []),
                    timeout
            end;
        Other ->
            ?I_INFO("What even happened? ~p", [Other])
    end,
    ?I_INFO("set_leader_node rigging election for ~p", [NewLeader]),
    ?M:set_leader_orig(LocalPid, NewLeader, undefined).
