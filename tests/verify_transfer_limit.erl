-module(verify_transfer_limit).
-behaviour(riak_test).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    lager:info("Deploying 2 nodes"),
    Nodes = rt:build_cluster(2, [{riak_core, [{handoff_concurrency, 11}]}]),
    [Node1, Node2] = Nodes,
    rt:wait_until_all_members(Nodes),
    test_transfer_limit(Nodes, rpc_transfer_limit),
    set_transfer_limit(Node2, "11"),
    test_transfer_limit(Nodes, transfer_limit),
    lager:info("Stopping Node2"),
    rt:stop(Node2),
    test_transfer_limit_node2_down(Node1, Node2),
    pass.

%% @doc These tests depend upon the status format in riak_core_status.hrl
test_transfer_limit([Node1,Node2]=_Nodes, TransferLimitFun) ->
    lager:info("Testing riak_core_status:~p", [TransferLimitFun]),
    Status = rpc:call(Node1, riak_core_status, TransferLimitFun, []),
    ?assertMatch([{table,[node,limit],[[Node1,11],[Node2,11]]}], Status),
    set_transfer_limit(Node2, "5"),
    Status2 = rpc:call(Node2, riak_core_status, TransferLimitFun, []),
    ?assertMatch([{table,[node,limit],[[Node1,11],[Node2,5]]}], Status2),
    Status3 = rpc:call(Node2, riak_core_status, TransferLimitFun, [Node1]),
    ?assertMatch([{table,[node,limit],[[Node1,11]]}], Status3).

test_transfer_limit_node2_down(Node1, Node2) ->
    Status = rpc:call(Node1, riak_core_status, transfer_limit, []),
    %% We still get all values when using cluster metadata. Note however that
    %% the value set for node2 may not yet have propogated to Node1.
    ?assertMatch([{table,[node,limit],[[Node1,11],[Node2,_]]}], Status),
    Status2 = rpc:call(Node1, riak_core_status, rpc_transfer_limit, []),
    %% Node 2 is down, so we get an alert as well
    ?assertMatch([{table, _, _}, {alert, _}], Status2).

set_transfer_limit(Node, LimitStr) ->
    LimitStrs = ["dev2@127.0.0.1", LimitStr],
    ok = rpc:call(Node, riak_core_console, transfer_limit, [LimitStrs]).
