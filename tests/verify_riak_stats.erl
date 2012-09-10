-module(verify_riak_stats).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% You should have curl installed locally to do this.
confirm() ->
    Nodes = rt:deploy_nodes(1),
    [Node1] = Nodes,
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),
    Stats1 = get_stats(Node1),
    %% make sure a set of stats have valid values
    verify_nz(Stats1,[<<"cpu_nprocs">>,
                      <<"mem_total">>,
                      <<"mem_allocated">>,
                      <<"sys_logical_processors">>,
                      <<"sys_process_count">>,
                      <<"sys_thread_pool_size">>,
                      <<"sys_wordsize">>,
                      <<"ring_num_partitions">>,
                      <<"ring_creation_size">>,
                      <<"memory_total">>,
                      <<"memory_processes">>,
                      <<"memory_processes_used">>,
                      <<"memory_system">>,
                      <<"memory_atom">>,
                      <<"memory_atom_used">>,
                      <<"memory_binary">>,
                      <<"memory_code">>,
                      <<"memory_ets">>]),
    
    lager:info("perform 5 x  PUT and a GET to increment the stats"),
    lager:info("as the stat system only does calcs for > 5 readings"),
    
    %% @todo Why do get counters not increment when systest_write is called, but not curl?
    %% rt:systest_write(Node1, 5),
    os:cmd(io_lib:format("curl -s -S -X PUT ~s/riak/~s/~s -d '~s'", [rt:http_url(Node1), "systest", "1", "12345"])),
    os:cmd(io_lib:format("curl -s -S -X PUT ~s/riak/~s/~s -d '~s'", [rt:http_url(Node1), "systest", "2", "12345"])),
    os:cmd(io_lib:format("curl -s -S -X PUT ~s/riak/~s/~s -d '~s'", [rt:http_url(Node1), "systest", "3", "12345"])),
    os:cmd(io_lib:format("curl -s -S -X PUT ~s/riak/~s/~s -d '~s'", [rt:http_url(Node1), "systest", "4", "12345"])),
    os:cmd(io_lib:format("curl -s -S -X PUT ~s/riak/~s/~s -d '~s'", [rt:http_url(Node1), "systest", "5", "12345"])),
    
    rt:systest_read(Node1, 5),
    timer:sleep(10000),
    
    Stats2 = get_stats(Node1),
    
    %% make sure the stats that were supposed to increment did
    verify_inc(Stats1, Stats2, [{<<"node_gets">>, 10},
                                {<<"node_puts">>, 5},
                                {<<"node_gets_total">>, 10},
                                {<<"node_puts_total">>, 5},
                                {<<"vnode_gets">>, 30},
                                {<<"vnode_puts">>, 15},
                                {<<"vnode_gets_total">>, 30},
                                {<<"vnode_puts_total">>, 15}]),

    %% verify that fsm times were tallied
    verify_nz(Stats2, [<<"node_get_fsm_time_mean">>,
                       <<"node_get_fsm_time_median">>,
                       <<"node_get_fsm_time_95">>,
                       <<"node_get_fsm_time_99">>,
                       <<"node_get_fsm_time_100">>,
                       <<"node_put_fsm_time_mean">>,
                       <<"node_put_fsm_time_median">>,
                       <<"node_put_fsm_time_95">>,
                       <<"node_put_fsm_time_99">>,
                       <<"node_put_fsm_time_100">>]),
                       
    lager:info("Force Read Repair"),
    Stats3 = get_stats(Node1),

    lager:info("Make PBC Connection"),
    rt:systest_write(Node1, 1),
    
    _Pid = rt:pbc(Node1),
    timer:sleep(10000),
    %% make sure the stats that were supposed to increment did
    Stats4 = get_stats(Node1),
    verify_inc(Stats3, Stats4, [{<<"pbc_connects_total">>, 1},
                                {<<"pbc_connects">>, 1},
                                {<<"pbc_active">>, 1}]),
    
    pass.

verify_inc(Prev, Props, Keys) ->
    [begin
         Old = proplists:get_value(Key, Prev, 0),
         New = proplists:get_value(Key, Props, 0),
         lager:info("~s: ~p -> ~p (expected ~p)", [Key, Old, New, Old + Inc]),
         ?assertEqual(New, (Old + Inc))
     end || {Key, Inc} <- Keys].

verify_nz(Props, Keys) ->
    [?assertNotEqual(proplists:get_value(Key,Props,0), 0) || Key <- Keys].

get_stats(Node) ->
    StatString = os:cmd(io_lib:format("curl -s -S ~s/stats", [rt:http_url(Node)])),
    {struct, Stats} = mochijson2:decode(StatString),
    %%lager:debug(StatString),
    Stats.