%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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
%% uses the following configs with given defaults:
%%
%% ## default_timeout = 1000 :: timeout()
%%
%% Base timeout value; some tests will use a larger value (multiple of).
%%
%% ## run_rt_cascading_1_3_tests = false :: any()
%%
%% Some tests (new_to_old and mixed_version_clusters) only make sense to
%% run if one is testing the version before cascading was introduced and
%% the version it was added; eg current being riak 1.4 and previous being
%% riak 1.3. If this is set to anything (other than 'false') those tests
%% are run. They will not function properly unless the correct versions
%% for riak are available. The tests check if the versions under test are
%% too old to be valid however.
%%
%% With this set to default, the tests that depend on this option will
%% emit a log message saying they are not configured to run.
%%
%% -------------------------------------------------------------------

-module(rt_cascading).
-export([
    conf/0,
    connect_rt/3,
    get_cluster_mgr_port/1,
    get_node/2,
    get_port/2,
    make_cluster/2,
    make_clusters/1,
    maybe_eventually_exists/3,
    maybe_eventually_exists/4,
    maybe_eventually_exists/5,
    maybe_eventually_exists/6,
    maybe_reconnect_rt/3,
    timeout/1,
    wait_exit/2,
    wait_for_rt_started/2,
    wait_until_pending_count_zero/1,
    write_n_keys/4
]).

-include_lib("eunit/include/eunit.hrl").

%% =====
%% utility functions
%% ====

wait_exit(Pids, Timeout) ->
    Mons = [{erlang:monitor(process, Pid), Pid} || Pid <- Pids],
    lists:map(fun({Mon, Pid}) ->
        receive
            {'DOWN', Mon, process, Pid, Cause} ->
                Cause
        after Timeout ->
            timeout
        end
              end, Mons).

make_clusters(UnNormalClusterConfs) ->
    ClusterConfs = lists:map(fun
                                 ({Name, Size}) ->
                                     {Name, Size, []};
                                 ({_Name, _Size, _ConnectsTo} = E) ->
                                     E
                             end, UnNormalClusterConfs),
    DeployList = lists:foldl(fun
                                 ({_Name, Size, _ConnectTo}, ConfAcc) ->
                                     Conf = conf(),
                                     AddToAcc = lists:duplicate(Size, {current, Conf}),
                                     ConfAcc ++ AddToAcc
                             end, [], ClusterConfs),
    Nodes = rt:deploy_nodes(DeployList),
    lager:info("nodes deployed: ~p", [Nodes]),
    {NamesAndNodes, []} = lists:foldl(fun
                                          ({Name, Size, _ConnectTo}, {Clusters, NodesLeft}) ->
                                              {ForCluster, NewNodesLeft} = lists:split(Size, NodesLeft),
                                              {Clusters ++ [{Name, ForCluster}], NewNodesLeft}
                                      end, {[], Nodes}, ClusterConfs),
    NamesAndNodes = lists:map(fun({Name, ForClusterNodes}) ->
        {Name, make_cluster(Name, ForClusterNodes)}
                              end, NamesAndNodes),
    ok = lists:foreach(fun({Name, _Size, ConnectsTo}) ->
        lists:foreach(fun(ConnectToName) ->
            connect_rt(get_node(Name, NamesAndNodes), get_port(ConnectToName, NamesAndNodes), ConnectToName)
                      end, ConnectsTo)
                       end, ClusterConfs),
    NamesAndNodes.

make_cluster(Name, Nodes) ->
    repl_util:make_cluster(Nodes),
    _ = [repl_util:wait_until_is_leader(N) || N <- Nodes],
    [ANode | _] = Nodes,
    repl_util:name_cluster(ANode, Name),
    Nodes.

get_node(Name, NamesAndNodes) ->
    [Node | _] = proplists:get_value(Name, NamesAndNodes),
    Node.

get_port(Name, NamesAndNodes) ->
    get_cluster_mgr_port(get_node(Name, NamesAndNodes)).

conf() ->
    [{lager, [
        {handlers, [
            {lager_console_backend,info},
            {lager_file_backend, [
                {"./log/error.log",error,10485760,"$D0",5},
                {"./log/console.log",info,10485760,"$D0",5},
                {"./log/debug.log",debug,10485760,"$D0",5}
            ]}
        ]},
        {crash_log,"./log/crash.log"},
        {crash_log_msg_size,65536},
        {crash_log_size,10485760},
        {crash_log_date,"$D0"},
        {crash_log_count,5},
        {error_logger_redirect,true}
    ]},
        {riak_repl, [
            {fullsync_on_connect, false},
            {fullsync_interval, disabled},
            {diff_batch_size, 10},
            {rt_heartbeat_interval, undefined}
        ]}].

get_cluster_mgr_port(Node) ->
    {ok, {_Ip, Port}} = rpc:call(Node, application, get_env, [riak_core, cluster_mgr]),
    Port.

maybe_reconnect_rt(SourceNode, SinkPort, SinkName) ->
    case repl_util:wait_for_connection(SourceNode, SinkName) of
        fail ->
            connect_rt(SourceNode, SinkPort, SinkName);
        Oot ->
            Oot
    end.

connect_rt(SourceNode, SinkPort, SinkName) ->
    repl_util:connect_cluster(SourceNode, "127.0.0.1", SinkPort),
    repl_util:wait_for_connection(SourceNode, SinkName),
    repl_util:enable_realtime(SourceNode, SinkName),
    repl_util:start_realtime(SourceNode, SinkName).

exists(Nodes, Bucket, Key) ->
    exists({error, notfound}, Nodes, Bucket, Key).

exists(Got, [], _Bucket, _Key) ->
    Got;
exists({error, notfound}, [Node | Tail], Bucket, Key) ->
    Pid = rt:pbc(Node),
    Got = riakc_pb_socket:get(Pid, Bucket, Key),
    riakc_pb_socket:stop(Pid),
    exists(Got, Tail, Bucket, Key);
exists(Got, _Nodes, _Bucket, _Key) ->
    Got.

maybe_eventually_exists(Node, Bucket, Key) ->
    Timeout = timeout(10),
    WaitTime = rt_config:get(default_wait_time, 1000),
    maybe_eventually_exists(Node, Bucket, Key, Timeout, WaitTime).

maybe_eventually_exists(Node, Bucket, Key, Timeout) ->
    WaitTime = rt_config:get(default_wait_time, 1000),
    maybe_eventually_exists(Node, Bucket, Key, Timeout, WaitTime).

maybe_eventually_exists(Node, Bucket, Key, Timeout, WaitMs) when is_atom(Node) ->
    maybe_eventually_exists([Node], Bucket, Key, Timeout, WaitMs);

maybe_eventually_exists(Nodes, Bucket, Key, Timeout, WaitMs) ->
    Got = exists(Nodes, Bucket, Key),
    maybe_eventually_exists(Got, Nodes, Bucket, Key, Timeout, WaitMs).

maybe_eventually_exists({error, notfound}, Nodes, Bucket, Key, Timeout, WaitMs) when Timeout > 0 ->
    timer:sleep(WaitMs),
    Got = exists(Nodes, Bucket, Key),
    Timeout2 = case Timeout of
                   infinity ->
                       infinity;
                   _ ->
                       Timeout - WaitMs
               end,
    maybe_eventually_exists(Got, Nodes, Bucket, Key, Timeout2, WaitMs);

maybe_eventually_exists({ok, RiakObj}, _Nodes, _Bucket, _Key, _Timeout, _WaitMs) ->
    riakc_obj:get_value(RiakObj);

maybe_eventually_exists(Got, _Nodes, _Bucket, _Key, _Timeout, _WaitMs) ->
    Got.

wait_for_rt_started(Node, ToName) ->
    Fun = fun(_) ->
        Status = rpc:call(Node, riak_repl2_rt, status, []),
        Started = proplists:get_value(started, Status, []),
        lists:member(ToName, Started)
          end,
    rt:wait_until(Node, Fun).

write_n_keys(Source, Destination, M, N) ->
    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
        <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-rt_test_a">>,
    First = M,
    Last = N,

    %% Write some objects to the source cluster (A),
    lager:info("Writing ~p keys to ~p, which should RT repl to ~p",
        [Last-First+1, Source, Destination]),
    ?assertEqual([], repl_util:do_write(Source, First, Last, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading ~p keys written from ~p", [Last-First+1, Destination]),
    ?assertEqual(0, repl_util:wait_for_reads(Destination, First, Last, TestBucket, 2)).

timeout(MultiplyBy) ->
    case rt_config:get(default_timeout, 1000) of
        infinity ->
            infinity;
        N ->
            N * MultiplyBy
    end.

wait_until_pending_count_zero(Nodes) ->
    WaitFun = fun() ->
        {Statuses, _} =  rpc:multicall(Nodes, riak_repl2_rtq, status, []),
        Out = [check_status(S) || S <- Statuses],
        not lists:member(false, Out)
              end,
    ?assertEqual(ok, rt:wait_until(WaitFun)),
    ok.

check_status(Status) ->
    case proplists:get_all_values(consumers, Status) of
        undefined ->
            true;
        [] ->
            true;
        Cs ->
            PendingList = [proplists:lookup_all(pending, C) || {_, C} <- lists:flatten(Cs)],
            PendingCount = lists:sum(proplists:get_all_values(pending, lists:flatten(PendingList))),
            ?debugFmt("RTQ status pending on test node:~p", [PendingCount]),
            PendingCount == 0
    end.