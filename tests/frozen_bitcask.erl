-module(frozen_bitcask).
-export([confirm/0, index_chash/1]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% Ensure handoff doesn't happen automatically
    Config = [{riak_core, [{forced_ownership_handoff, 0},
                           {vnode_inactivity_timeout, 99999999}]}],
    Nodes = rt:deploy_nodes(2, Config),
    [Node1, Node2] = Nodes,
    rt:join(Node2, Node1),
    rt:wait_until_ring_converged(Nodes),

    Ring = rt:get_ring(Node1),
    Index = element(1, hd(riak_core_ring:pending_changes(Ring))),
    Bucket = <<(Index-1):160/integer>>,

    load_code(?MODULE, Nodes),

    rpc:call(Node1, riak_core_bucket, set_bucket,
             [Bucket, [{n_val, 1}, {chash_keyfun, {frozen_bitcask, index_chash}}]]),

    rt:wait_until_ring_converged(Nodes),

    lager:info("Writing data to vnode: ~p", [Index]),
    rt:systest_write(Node1, 0, 80000, Bucket, 1),

    lager:info("Triggering handoff of vnode"),
    {ok, Pid} = rpc:call(Node1, riak_core_vnode_master, get_vnode_pid, [Index, riak_kv_vnode]),
    rpc:call(Node1, riak_core_vnode, trigger_handoff, [Pid, Node2]),
    timer:sleep(3000),

    lager:info("Writing more data to vnode: ~p", [Index]),
    rt:systest_write(Node1, 0, 1000, Bucket, 1),

    lager:info("Triggering handoff again"),
    riak_core_vnode:trigger_handoff(Pid, Node2),

    lager:info("Writing data async"),
    [spawn(fun() ->
                   rt:systest_write(Node1, 0, 50, Bucket, 1)
           end) || _ <- lists:seq(1,1000)],

    [begin
         timer:sleep(1000),
         QS = message_queue(Node1, Pid),
         lager:info("Message queue: ~p", [QS]),
         ?assert(QS < 1000)
     end || _ <- lists:seq(1,10)],

    ok.

message_queue(Node, Pid) ->
    %% After handoff vnode may shutdown and RPC will fail, just return 0
    case rpc:call(Node, erlang, process_info, [Pid, message_queue_len]) of
        {message_queue_len, QS} ->
            QS;
        _ ->
            0
    end.

index_chash({Bucket, _}) ->
    Bucket.

load_code(Module, Nodes) ->
    {Module, Bin, File} = code:get_object_code(Module),
    {_, []} = rpc:multicall(Nodes, code, load_binary, [Module, File, Bin]).
