-module(pr_pw).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    lager:info("Deploy some nodes"),
    Nodes = rt:deploy_nodes(4),
    repl_util:make_cluster(Nodes),
    timer:sleep(100),
    rt:wait_until_ring_converged(Nodes),

    %% calculate the preflist for foo/bar
    {ok, Ring} = rpc:call(hd(Nodes), riak_core_ring_manager, get_my_ring, []),
    UpNodes = rpc:call(hd(Nodes), riak_core_node_watcher, nodes, [riak_kv]),
    DocIdx = rpc:call(hd(Nodes), riak_core_util, chash_key, [{<<"foo">>,
                <<"bar">>}]),
    N = 3,
    Preflist2 = riak_core_apl:get_apl_ann(DocIdx, N, Ring, UpNodes),
    lager:info("Preflist is ~p", [Preflist2]),
    PLNodes = [Node || {{_Index, Node}, _Status} <- Preflist2],
    lager:info("Nodes in preflist ~p", [PLNodes]),
    [SafeNode] = Nodes -- PLNodes,
    lager:info("Node not involved in this preflist ~p", [SafeNode]),
    %% connect to the only node in the preflist we won't break, to avoid
    %% random put forwarding
    {ok, [{_IP, Port}|_]} = rpc:call(hd(PLNodes), application, get_env, [riak_core, http]),
    C = rhc:create("127.0.0.1", Port, "riak", []),
    Obj = riakc_obj:new(<<"foo">>, <<"bar">>, <<42:32/integer>>),
    ?assertEqual(ok, rhc:put(C, Obj, [{pw, all}])),
    ?assertMatch({ok, _}, rhc:get(C, <<"foo">>, <<"bar">>, [{pr, all}])),
    %% stop the last node in the preflist, if we stopped the first we'd time
    %% out the coordinating put
    DeadNode = lists:last(PLNodes),
    P1 = kill_dash_stop(DeadNode),
    %% wait for the net_kernel to notice this node is dead
    erlang:monitor_node(DeadNode, true),
    receive
        {nodedown, DeadNode} -> ok
    end,
    ?assertEqual(ok, rhc:put(C, Obj, [{pw, quorum}])),
    ?assertMatch({ok, _},
        rhc:get(C, <<"foo">>, <<"bar">>, [{pr, quorum}])),

    P2 = kill_dash_stop(lists:nth(2, PLNodes)),
    timer:sleep(1000),
    %% now, there's a magic window here where node_watcher thinks this node is
    %% up, but its not, so the initial validation of PR/PW works, even though
    %% we may not actually be getting PR/PW number of primaries responding
    ?assertMatch({error, timeout},
        rhc:get(C, <<"foo">>, <<"bar">>, [{pr, quorum}])),
    ?assertEqual({error, timeout}, rhc:put(C, Obj, [{pw, quorum}])),
    kill_dash_cont(P1),
    ?assertEqual(ok, rhc:put(C, Obj, [{pw, quorum}])),
    ?assertMatch({ok, _}, rhc:get(C, <<"foo">>, <<"bar">>, [{pr, quorum}])),
    ?assertMatch({error, {pr_val_unsatisfied, 3, 2}},
        rhc:get(C, <<"foo">>, <<"bar">>, [{pr, all}])),
    ?assertEqual({error, {pw_val_unsatisfied, 3, 2}}, rhc:put(C, Obj, [{pw, all}])),
    kill_dash_cont(P2),
    timer:sleep(100),
    ?assertEqual(ok, rhc:put(C, Obj, [{pw, all}])),
    ?assertMatch({ok, _}, rhc:get(C, <<"foo">>, <<"bar">>, [{pr, all}])),
    pass.

kill_dash_stop(Node) ->
   lager:info("kill -STOP node ~p", [Node]),
   OSPidToKill = rpc:call(Node, os, getpid, []),
   os:cmd(io_lib:format("kill -STOP ~s", [OSPidToKill])),
   lager:info("STOPped"),
   OSPidToKill.

kill_dash_cont(Pid) ->
   lager:info("kill -CONT pid ~p", [Pid]),
   os:cmd(io_lib:format("kill -CONT ~s", [Pid])),
   lager:info("CONTinued"),
   ok.


