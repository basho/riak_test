%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
-module(overload).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-cover_modules([riak_kv_vnode,
                riak_kv_ensemble_backend,
                riak_core_vnode_proxy]).

-define(NUM_REQUESTS, 200).
-define(THRESHOLD, 100).
-define(BUCKET, <<"test">>).
-define(KEY, <<"hotkey">>).

confirm() ->
    Nodes = setup(),

    NormalType = <<"normal_type">>,
    ConsistentType = <<"consistent_type">>,
    ok = create_bucket_type(Nodes, NormalType, [{n_val, 3}]),
    ok = create_bucket_type(Nodes, ConsistentType, [{consistent, true}, {n_val, 5}]),
    rt:wait_until(ring_manager_check_fun(hd(Nodes))),

    BKV1 = {{NormalType, ?BUCKET}, ?KEY, <<"test">>},
    BKV2 = {{ConsistentType, ?BUCKET}, ?KEY, <<"test">>},

    Tests = [test_no_overload_protection,
             test_vnode_protection,
             test_fsm_protection,
             test_cover_queries_overload],

    [ok = erlang:apply(?MODULE, Test, [Nodes, BKV, IsConsistent]) ||
        Test <- Tests,
        {BKV, IsConsistent} <- [{BKV1, false},
                                {BKV2, true}]],
    pass.


setup() ->
    Config = [{riak_core, [{ring_creation_size, 8},
                           {default_bucket_props, [{n_val, 5}]},
                           {vnode_management_timer, 1000},
                           {enable_health_checks, false},
                           {enable_consensus, true},
                           {vnode_overload_threshold, undefined}]},
              {riak_kv, [{fsm_limit, undefined},
                         {storage_backend, riak_kv_memory_backend},
                         {anti_entropy_build_limit, {100, 1000}},
                         {anti_entropy_concurrency, 100},
                         {anti_entropy_tick, 100},
                         {anti_entropy, {on, []}},
                         {anti_entropy_timeout, 5000}]}],
    ensemble_util:build_cluster(5, Config, 5).

test_no_overload_protection(_Nodes, _BKV, true) ->
    ok;
test_no_overload_protection(Nodes, BKV, ConsistentType) ->
    lager:info("Testing with no overload protection"),
    ProcFun = fun(X) ->
                      X >= (2*?NUM_REQUESTS * 0.9)
              end,
    QueueFun = fun(X) ->
                      X >= (?NUM_REQUESTS * 0.9)
              end,
    verify_test_results(run_test(Nodes, BKV), ConsistentType, ProcFun, QueueFun).

verify_test_results({_NumProcs, QueueLen}, true, _, QueueFun) ->
    ?assert(QueueFun(QueueLen));
verify_test_results({NumProcs, QueueLen}, false, ProcFun, QueueFun) ->
    ?assert(ProcFun(NumProcs)),
    ?assert(QueueFun(QueueLen)).

test_vnode_protection(Nodes, BKV, ConsistentType) ->
    %% Setting check_interval to one ensures that process_info is called
    %% to check the queue length on each vnode send.
    %% This allows us to artificially raise vnode queue lengths with dummy
    %% messages instead of having to go through the vnode path for coverage
    %% query overload testing.
    lager:info("Testing with vnode queue protection enabled"),
    lager:info("Setting vnode overload threshold to ~b", [?THRESHOLD]),
    lager:info("Setting vnode check interval to 1"),
    Config = [{riak_core, [{vnode_overload_threshold, ?THRESHOLD},
                           {vnode_check_interval, 1}]}],
    rt:pmap(fun(Node) ->
                    rt:update_app_config(Node, Config)
            end, Nodes),
    ProcFun = fun(X) ->
                      X =< (2*?THRESHOLD * 1.5)
              end,
    QueueFun = fun(X) ->
                      X =< (?THRESHOLD * 1.1)
              end,
    verify_test_results(run_test(Nodes, BKV), ConsistentType, ProcFun, QueueFun),

    %% This stats check often fails. Manual testing shows stats
    %% always incrementing properly. Plus, if I add code to Riak
    %% to log when the dropped stat is incremented I see it called
    %% the correct number of times. This looks like a stats bug
    %% that is outside the scope of this test. Punting for now.
    %%
    %% ShouldDrop = ?NUM_REQUESTS - ?THRESHOLD,
    %% ok = rt:wait_until(Node2, fun(Node) ->
    %%                                   dropped_stat(Node) =:= ShouldDrop
    %%                           end),

    [Node1 | _] = Nodes,
    CheckInterval = ?THRESHOLD div 2,
    Dropped = read_until_success(Node1),
    lager:info("Unnecessary dropped requests: ~b", [Dropped]),
    ?assert(Dropped =< CheckInterval),

    Victim = get_victim(Node1, BKV),

    lager:info("Suspending vnode proxy for ~p", [Victim]),
    Pid = suspend_vnode_proxy(Victim),
    ProcFun2 = fun(X) ->
                      X >= (2*?NUM_REQUESTS * 0.9)
              end,
    QueueFun2 = fun(X) ->
                      X =< (?THRESHOLD * 1.1)
              end,
    verify_test_results(run_test(Nodes, BKV), ConsistentType, ProcFun2, QueueFun2),
    Pid ! resume,
    ok.

test_fsm_protection(Nodes, BKV, ConsistentType) ->
    lager:info("Testing with coordinator protection enabled"),
    lager:info("Setting FSM limit to ~b", [?THRESHOLD]),
    Config = [{riak_kv, [{fsm_limit, ?THRESHOLD}]}],
    rt:pmap(fun(Node) ->
                    rt:update_app_config(Node, Config)
            end, Nodes),
    ProcFun = fun(X) ->
                      X =< (?THRESHOLD * 1.1)
              end,
    QueueFun = fun(X) ->
                      X =< (?THRESHOLD * 1.1)
              end,
    verify_test_results(run_test(Nodes, BKV), ConsistentType, ProcFun, QueueFun),
    ok.

test_cover_queries_overload(_Nodes, _, true) ->
    ok;
test_cover_queries_overload(Nodes, _, false) ->
    lager:info("Testing cover queries with vnode queue protection enabled"),
    lager:info("Setting vnode overload threshold to ~b", [?THRESHOLD]),
    lager:info("Setting vnode check interval to 1"),

    Config = [{riak_core, [{vnode_overload_threshold, ?THRESHOLD},
                           {vnode_request_check_interval, 1},
                           {vnode_check_interval, 2}]}],
    rt:pmap(fun(Node) ->
                    rt:update_app_config(Node, Config)
            end, Nodes),

    [rt:wait_for_service(Node, riak_kv) || Node <- Nodes],
    rt:load_modules_on_nodes([?MODULE], Nodes),

    [Node1, Node2, Node3, Node4, Node5] = Nodes,
    Pids = [begin
                lager:info("Suspending all kv vnodes on ~p", [N]),
                suspend_and_overload_all_kv_vnodes(N)
            end || N <- [Node2, Node3, Node4, Node5]],

    wait_until_vnodes_suspended(Node1),

    [?assertEqual({error, <<"mailbox_overload">>}, KeysRes) ||
        KeysRes <- [list_keys(Node1) || _ <- lists:seq(1, 3)]],

    lager:info("list_keys correctly handled overload"),

    [?assertEqual({error, mailbox_overload}, BucketsRes) ||
        BucketsRes <- [list_buckets(Node1) || _ <- lists:seq(1, 3)]],
    lager:info("list_buckets correctly handled overload"),

    lager:info("Resuming all kv vnodes"),
    [resume_all_vnodes(Pid) || Pid <- Pids],

    lager:info("Waiting for vnode queues to empty"),
    wait_for_all_vnode_queues_empty(Node2).

run_test(Nodes, BKV) ->
    [Node1 | _RestNodes] = Nodes,
    rt:wait_for_cluster_service(Nodes, riak_kv),
    lager:info("Sleeping for 10s to let process count stablize"),
    timer:sleep(10000),
    rt:load_modules_on_nodes([?MODULE], Nodes),
    Victim = get_victim(Node1, BKV),
    lager:info("Suspending vnode ~p/~p",
               [element(1, Victim), element(2, Victim)]),
    Suspended = suspend_vnode(Victim),

    NumProcs1 = process_count(Node1),
    lager:info("Initial process count on ~p: ~b", [Node1, NumProcs1]),
    lager:info("Sending ~b read requests", [?NUM_REQUESTS]),
    write_once(Node1, BKV),
    Reads = spawn_reads(Node1, BKV, ?NUM_REQUESTS),
    timer:sleep(5000),

    NumProcs2 = process_count(Node1),
    lager:info("Final process count on ~p: ~b", [Node1, NumProcs2]),

    QueueLen = vnode_queue_len(Victim),
    lager:info("Final vnode queue length for ~p: ~b",
               [Victim, QueueLen]),
    resume_vnode(Suspended),
    rt:wait_until(fun() ->
                          vnode_queue_len(Victim) =:= 0
                  end),
    kill_pids(Reads),
    {NumProcs2 - NumProcs1, QueueLen}.

get_victim(ExcludeNode, {Bucket, Key, _}) ->
    Hash = riak_core_util:chash_std_keyfun({Bucket, Key}),
    PL = lists:sublist(riak_core_ring:preflist(Hash, rt:get_ring(ExcludeNode)), 5),
    hd([IdxNode || {_, Node}=IdxNode <- PL, Node /= ExcludeNode]).

ring_manager_check_fun(Node) ->
    fun() ->
            case rpc:call(Node, riak_core_ring_manager, get_chash_bin, []) of
                {ok, _R} ->
                    true;
                _ ->
                    false
            end
    end.

create_bucket_type(Nodes, Type, Props) ->
    lager:info("Create bucket type ~p, wait for propagation", [Type]),
    rt:create_and_activate_bucket_type(hd(Nodes), Type, Props),
    rt:wait_until_bucket_type_status(Type, active, Nodes),
    rt:wait_until_bucket_props(Nodes, {Type, <<"bucket">>}, Props),
    ok.

wait_until_vnodes_suspended(Node) ->
    rt:wait_until(list_keys_timeout_check(Node)).

list_keys_timeout_check(Node) ->
    fun() ->
       case list_keys(Node) of
           {error, <<"mailbox_overload">>} ->
               true;
           _ ->
               false
       end
    end.

list_keys(Node) ->
    Pid = rt:pbc(Node, [{auto_reconnect, true}, {queue_if_disconnected, true}]),
    Res = riakc_pb_socket:list_keys(Pid, {<<"normal_type">>, ?BUCKET}, 30000),
    riakc_pb_socket:stop(Pid),
    Res.

list_buckets(Node) ->
    {ok, C} = riak:client_connect(Node),
    riak_client:list_buckets(30000, C).

wait_for_all_vnode_queues_empty(Node) ->
    rt:wait_until(Node, fun(N) ->
                                vnode_queues_empty(N)
                        end).

vnode_queues_empty(Node) ->
    rpc:call(Node, ?MODULE, remote_vnode_queues_empty, []).

remote_vnode_queues_empty() ->
    lists:all(fun({_, _, Pid}) ->
                      {message_queue_len, Len} =
                          process_info(Pid, message_queue_len),
                      Len =:= 0
              end, riak_core_vnode_manager:all_vnodes()).

write_once(Node, {Bucket, Key, Value}) ->
    lager:info("Writing to node ~p", [Node]),
    PBC = rt:pbc(Node, [{auto_reconnect, true}, {queue_if_disconnected, true}]),
    rt:pbc_write(PBC, Bucket, Key, Value),
    riakc_pb_socket:stop(PBC).

read_until_success(Node) ->
    {ok, C} = riak:client_connect(Node),
    read_until_success(C, 0).

read_until_success(C, Count) ->
    case C:get(?BUCKET, ?KEY) of
        {error, mailbox_overload} ->
            read_until_success(C, Count+1);
        _ ->
            Count
    end.

spawn_reads(Node, {Bucket, Key, _}, Num) ->
    [spawn(fun() ->
                   PBC = rt:pbc(Node,
                                [{auto_reconnect, true},
                                 {queue_if_disconnected, true}]),
                   _ = riakc_pb_socket:get(PBC, Bucket, Key),
                   riakc_pb_socket:stop(PBC)
           end) || _ <- lists:seq(1, Num)].

kill_pids(Pids) ->
    [exit(Pid, kill) || Pid <- Pids].

suspend_and_overload_all_kv_vnodes(Node) ->
    lager:info("Suspending vnodes on ~p", [Node]),
    Pid = rpc:call(Node, ?MODULE, remote_suspend_and_overload, []),
    Pid ! {overload, self()},
    receive overloaded ->
            Pid
    end.

remote_suspend_and_overload() ->
    spawn(fun() ->
                  Vnodes = riak_core_vnode_manager:all_vnodes(),
                  [begin
                       lager:info("Suspending vnode pid: ~p", [Pid]),
                       erlang:suspend_process(Pid, []) end || {riak_kv_vnode, _, Pid}
                                                                  <- Vnodes],
                  receive {overload, From} ->
                          io:format("Overloading vnodes ~n"),
                          [?MODULE:overload(Pid) ||
                              {riak_kv_vnode, _, Pid} <- Vnodes],
                          From ! overloaded
                  end,
                  receive resume ->
                          io:format("Resuming vnodes~n"),
                          [erlang:resume_process(Pid) || {riak_kv_vnode, _, Pid}
                                                             <- Vnodes]
                  end
          end).

overload(Pid) ->
    %% The actual message doesn't matter. This one just has the least
    %% side effects.
    [Pid ! {set_concurrency_limit, some_lock, 1} ||
        _ <- lists:seq(1, ?NUM_REQUESTS)].

suspend_vnode({Idx, Node}) ->
    suspend_vnode(Node, Idx).

suspend_vnode(Node, Idx) ->
    rpc:call(Node, ?MODULE, remote_suspend_vnode, [Idx], infinity).

remote_suspend_vnode(Idx) ->
    spawn(fun() ->
                  {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Idx, riak_kv_vnode),
                  lager:info("Suspending vnode pid: ~p", [Pid]),
                  erlang:suspend_process(Pid, []),
                  receive resume ->
                          erlang:resume_process(Pid)
                  end
          end).

suspend_vnode_proxy({Idx, Node}) ->
    suspend_vnode_proxy(Node, Idx).

suspend_vnode_proxy(Node, Idx) ->
    rpc:call(Node, ?MODULE, remote_suspend_vnode_proxy, [Idx], infinity).

remote_suspend_vnode_proxy(Idx) ->
    spawn(fun() ->
                  Name = riak_core_vnode_proxy:reg_name(riak_kv_vnode, Idx),
                  Pid = whereis(Name),
                  erlang:suspend_process(Pid, []),
                  receive resume ->
                          erlang:resume_process(Pid)
                  end
          end).

resume_all_vnodes(Pid) ->
    Pid ! resume.

resume_vnode(Pid) ->
    Pid ! resume.

process_count(Node) ->
    rpc:call(Node, erlang, system_info, [process_count]).

vnode_queue_len({Idx, Node}) ->
    vnode_queue_len(Node, Idx).

vnode_queue_len(Node, Idx) ->
    rpc:call(Node, ?MODULE, remote_vnode_queue, [Idx]).

dropped_stat(Node) ->
    Stats = rpc:call(Node, riak_core_stat, get_stats, []),
    proplists:get_value(dropped_vnode_requests_total, Stats).

remote_vnode_queue(Idx) ->
    {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Idx, riak_kv_vnode),
    {message_queue_len, Len} = process_info(Pid, message_queue_len),
    Len.
