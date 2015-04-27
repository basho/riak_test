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
%%

-module(replication2_rt_with_stuck_vnode).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%-define(HB_TIMEOUT,  2000).
-define(SEND_ERROR_INTERVAL, 500).
%-define(OBJ_SIZE, (64)).
-define(OBJ_SIZE, (64*1024)).

confirm() ->
    NumNodes = 2,

    lager:info("Deploy ~p nodes", [NumNodes]),
    Conf = [
            {riak_core,
             [{ring_creation_size, 8}
             ]},
            {riak_repl,
             [

              %% turn off fullsync
              {fullsync_on_connect, false},
              {fullsync_interval, disabled},
              %%{rtq_overload_threshold, 10},
              {rtq_max_bytes, 1073741824},
              %%{rtq_overload_recover, 5},
              {rtsink_min_workers, 1},
              {rtsink_max_workers, 1},
              {rtsink_max_pending, 100}
              %% override defaults for RT heartbeat so that we
              %% can see faults sooner and have a quicker test.
              %{rt_heartbeat_interval, ?HB_TIMEOUT},
              %{rt_heartbeat_timeout, ?HB_TIMEOUT}
             ]}
    ],

    [SrcNode,SinkNode] = rt:deploy_nodes(NumNodes, Conf, [riak_kv, riak_repl]),

    lager:info("SrcNode: ~p", [SrcNode]),
    lager:info("SinkNode: ~p", [SinkNode]),

    lager:info("Loading intercepts."),
    rt_intercept:load_code(SinkNode),
    rt_intercept:add(SinkNode, {riak_kv_vnode, [{{handle_command, 3}, stuck_handle_command}]}),
    %rt_intercept:add(SinkNode, {riak_repl2_rtsink_helper, [{{do_write_objects, 3}, slow_do_write_objects}]}),
    %rt_intercept:add(SinkNode, {riak_repl_fullsync_worker, [{{do_binputs_internal, 4}, slow_do_binputs_internal}]}),

    lager:info("Build cluster A"),
    repl_util:make_cluster([SrcNode]),
    repl_util:name_cluster(SrcNode, "A"),

    lager:info("Build cluster B"),
    repl_util:make_cluster([SinkNode]),
    repl_util:name_cluster(SinkNode, "B"),

    lager:info("waiting for leader to converge on cluster A"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge([SrcNode])),

    lager:info("waiting for leader to converge on cluster B"),
    ?assertEqual(ok, repl_util:wait_until_leader_converge([SinkNode])),

    connect_clusters(SrcNode, SinkNode),
    enable_rt(SrcNode, [SrcNode]),

    Watcher = spawn_link(fun() -> msg_q_watcher(SinkNode) end),

    Concurrency = 10240,
    lager:info("putting 10k objs into cluster A from ~p workers", [Concurrency]),
    do_write([SrcNode], 1000, <<"rt-test-bucket">>, 2, Concurrency),
    wait_for_put(Concurrency),

    %% TODO: ensure rt repl started
    timer:sleep(3000),

    Watcher ! done,
    case get_rtsink_helper_msgq_length(SinkNode) of
        0 -> pass;
        MsgQ ->
            lager:error("rt_sink_conn's message queue length: ~p", [MsgQ]),
            fail
    end.

msg_q_watcher(SinkNode) ->
    Len = get_rtsink_helper_msgq_length(SinkNode),
    lager:info("total msg_q length: ~p", [Len]),
    % case Len of _ when Len > 5000 -> halt(-1); _ -> ok end,
    receive
        done -> ok
    after 3000 ->
            msg_q_watcher(SinkNode)
    end.

do_write(Nodes, PutCount, Bucket, W, Concurrency) ->
    Self = self(),
    NodeNum = length(Nodes),
    Fun = fun(X) ->
                  spawn_link(
                    fun() ->
                            Node = lists:nth((X rem NodeNum) + 1, Nodes),
                            lager:debug("Starting a worker: start=~p, end=~p", [1+((X-1)*PutCount), X*PutCount]),
                            rt:systest_write(Node, 1+((X-1)*PutCount), X*PutCount, Bucket, W, <<"0":(?OBJ_SIZE)>>),
                            lager:debug("worker~p done", [X]),
                            Self ! done
                    end)
          end,
    lists:map(Fun, lists:seq(1, Concurrency)).

wait_for_put(0) ->
    ok;
wait_for_put(Num) ->
    receive
        done -> wait_for_put(Num-1)
    end.

get_rtsink_helper_msgq_length(Node) ->
    Pid = rpc:call(Node, erlang, whereis, [riak_repl2_rtsink_conn_sup]),
    {links, Links} = rpc:call(Node, erlang, process_info, [Pid, links]),
    Procs = map_process_info(Node, Links),
    SinkConnInfos = lists:filter(filter_fun(riak_repl2_rtsink_conn), Procs),
    lists:foldl(fun(SinkConn, Acc) ->
                        Len = get_helper_msg_q_len(Node, SinkConn),
                        Acc + Len
                end, 0, SinkConnInfos).

get_helper_msg_q_len(Node, {Pid, SinkConnInfo}) ->
    Links2 = proplists:get_value(links, SinkConnInfo),
    FilteredLinks = lists:filter(fun erlang:is_pid/1, Links2),
    Procs2 = map_process_info(Node, FilteredLinks),
    [{_, HelperInfo}] = lists:filter(filter_fun(riak_repl2_rtsink_helper), Procs2),
    %% lager:debug("rtsink helper's msgq: ~p", [proplists:get_value(messages, HelperInfo)]),
    Status = rpc:call(Node, riak_repl2_rtsink_conn, status, [Pid]),
    Drop = proplists:get_value(source_drops, Status),
    ExpSeq = proplists:get_value(expect_seq, Status),
    AckSeq = proplists:get_value(acked_seq,  Status),
    Pending = proplists:get_value(pending, Status),
    lager:debug("~p: drop=~p exp=~p ack=~p pending=~p or exp-ack=~p",
                [Pid,Drop,ExpSeq,AckSeq,Pending,ExpSeq-AckSeq]),
    MQLen = proplists:get_value(message_queue_len, HelperInfo),
    lager:debug("msg_q length of rtsink_helper: ~p", [MQLen]),
    MQLen.

map_process_info(Node, Pids) ->
    % lager:debug("pids: ~p", [Pids]),
    lists:map(fun(P) -> {P, rpc:call(Node, erlang, process_info, [P])} end, Pids).

filter_fun(Module) ->
    fun({_Pid, Info}) ->
            {dictionary, Dict} = proplists:lookup(dictionary, Info),
            {'$initial_call', ModFunArgs} = proplists:lookup('$initial_call', Dict),
            %% lager:debug("module fun args: ~p", [ModFunArgs]),
            case ModFunArgs of
                {Module, init, 1} -> true;
                _ -> false
            end
    end.

%% @doc Connect two clusters for replication using their respective leader nodes.
connect_clusters(LeaderA, LeaderB) ->
    {ok, {_IP, Port}} = rpc:call(LeaderB, application, get_env,
                                 [riak_core, cluster_mgr]),
    lager:info("Connect cluster A:~p to B on port ~p", [LeaderA, Port]),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", Port).

%% @doc Turn on Realtime replication on the cluster lead by LeaderA.
%%      The clusters must already have been named and connected.
enable_rt(LeaderA, ANodes) ->
    lager:info("Enabling RT replication: ~p ~p.", [LeaderA, ANodes]),
    repl_util:enable_realtime(LeaderA, "B"),
    repl_util:start_realtime(LeaderA, "B").
