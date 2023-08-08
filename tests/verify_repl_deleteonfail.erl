%% -------------------------------------------------------------------
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
%% @doc When replicating deletes, if a node is down in the cluster, clusters
%% will get out of sync, unless the deletion of tombstones is deferred when
%% non-primaries are unavailable

-module(verify_repl_deleteonfail).
-export([confirm/0]).
-import(location, [plan_and_wait/2]).

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(SNK_WORKERS, 4).
-define(MAX_RESULTS, 512).

-define(CFG_REPL(SrcQueueDefns, NVal),
        [{riak_kv,
          [
           % Speedy AAE configuration
            {anti_entropy, {off, []}},
            {tictacaae_active, active},
            {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
            {tictacaae_rebuildwait, 4},
            {tictacaae_rebuilddelay, 3600},
            {tictacaae_rebuildtick, 3600000},
            {replrtq_enablesrc, true},
            {replrtq_srcqueue, SrcQueueDefns},
            {log_readrepair, true},
            {read_repair_log, true},
            {delete_mode, immediate}
          ]},
         {riak_core,
          [
            {ring_creation_size, ?DEFAULT_RING_SIZE},
            {vnode_management_timer, 2000},
            {vnode_inactivity_timeout, 4000},
            {handoff_concurrency, 16},
            {default_bucket_props,
                    [
                    {n_val, NVal},
                    {allow_mult, true},
                    {dvv_enabled, true}
                    ]}
          ]}]
       ).

-define(SNK_CONFIG(ClusterName, PeerList), 
       [{riak_kv, 
           [{replrtq_enablesink, true},
               {replrtq_sinkqueue, ClusterName},
               {replrtq_sinkpeers, PeerList},
               {replrtq_sinkworkers, ?SNK_WORKERS}]}]).

-define(FS_CONFIG(PeerIP, PeerPort, LocalClusterName, RemoteClusterName),
        [{riak_kv,
            [{ttaaefs_scope, all},
            {ttaaefs_localnval, 3},
            {ttaaefs_remotenval, 3},
            {ttaaefs_peerip, PeerIP},
            {ttaaefs_peerport, PeerPort},
            {ttaaefs_peerprotocol, pb},
            {ttaaefs_allcheck, 0},
            {ttaaefs_autocheck, 0},
            {ttaaefs_daycheck, 0},
            {ttaaefs_hourcheck, 0},
            {ttaaefs_nocheck, 24},
            {ttaaefs_maxresults, ?MAX_RESULTS},
            {ttaaefs_queuename, LocalClusterName},
            {ttaaefs_queuename_peer, RemoteClusterName},
            {ttaaefs_logrepairs, true}]}]).

-define(NUM_KEYS_PERNODE, 1000).
-define(BUCKET, {<<"test_type">>, <<"test_bucket">>}).

confirm() ->
    [ClusterA, ClusterB] =
        rt:deploy_clusters([
            {5, ?CFG_REPL("cluster_b:any", 3)},
            {1, ?CFG_REPL("cluster_a:any", 1)}]),

    lager:info("Discover Peer IP/ports and restart with peer config"),
    reset_peer_config(ClusterA, ClusterB),

    lists:foreach(
        fun(N) -> rt:wait_until_ready(N), rt:wait_until_pingable(N) end,
        ClusterA ++ ClusterB
    ),
    
    rt:join_cluster(ClusterA),
    rt:join_cluster(ClusterB),
    
    ok = verify_repl_delete(ClusterA, ClusterB),
    pass.


verify_repl_delete(Nodes, ClusterB) ->
    
    lager:info("Commencing object load"),
    KeyLoadFun =
        fun(B, V) ->
            fun(Node, KeyCount) ->
                lager:info("Loading from key ~w on node ~w", [KeyCount, Node]),
                KVs = 
                    test_data(
                        KeyCount + 1, KeyCount + ?NUM_KEYS_PERNODE, V),
                ok = write_data(Node, B, KVs, []),
                KeyCount + ?NUM_KEYS_PERNODE
            end
        end,

    lists:foldl(KeyLoadFun(<<"B1">>, <<"v1">>), 1, Nodes),
    lager:info("Loaded ~w objects", [?NUM_KEYS_PERNODE * length(Nodes)]),
    wait_for_queues_to_drain(Nodes, cluster_b),
    lists:foldl(KeyLoadFun(<<"B2">>, <<"v1">>), 1, Nodes),
    lager:info("Loaded ~w objects", [?NUM_KEYS_PERNODE * length(Nodes)]),
    wait_for_queues_to_drain(Nodes, cluster_b),
    lists:foldl(KeyLoadFun(<<"B3">>, <<"v1">>), 1, Nodes),
    lager:info("Loaded ~w objects", [?NUM_KEYS_PERNODE * length(Nodes)]),
    wait_for_queues_to_drain(Nodes, cluster_b),
    lists:foldl(KeyLoadFun(<<"B4">>, <<"v1">>), 1, Nodes),
    lager:info("Loaded ~w objects", [?NUM_KEYS_PERNODE * length(Nodes)]),
    wait_for_queues_to_drain(Nodes, cluster_b),
    
    lager:info(
        "Stopping a node - now tombstones will not be auto-reaped in A"),
    FiddlingNode = hd(tl(Nodes)),
    rt:stop_and_wait(FiddlingNode),
    
    lager:info("Delete all keys for B1 bucket"),
    delete_data(
        hd(Nodes), <<"B1">>, lists:seq(1, ?NUM_KEYS_PERNODE * length(Nodes))),
    lager:info("Delete attempts completed"),
    wait_for_queues_to_drain(Nodes -- [FiddlingNode], cluster_b),

    rt:start_and_wait(FiddlingNode),

    TCA = count_tombs(hd(Nodes), <<"B1">>) ,
    TCB = count_tombs(hd(ClusterB), <<"B1">>) ,

    lager:info("TombCount A ~w B ~w", [TCA, TCB]),

    ?assertMatch(TCA, TCB).

count_tombs(Node, Bucket) ->
    {ok, A1C} =
        rpc:call(Node,
            riak_client,
            aae_fold,
            [{find_tombs, Bucket, all, all, all}]),
    length(A1C).


to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].


write_data(Node, Bucket, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
         O =
         case riakc_pb_socket:get(PB, Bucket, K) of
             {ok, Prev} ->
                 riakc_obj:update_value(Prev, V);
             _ ->
                 riakc_obj:new(Bucket, K, V)
         end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    ok.

delete_data(Node, Bucket, Ns) ->
    PB = rt:pbc(Node),
    lists:foreach(
        fun(N) -> ok = riakc_pb_socket:delete(PB, Bucket, to_key(N)) end, Ns),
    riakc_pb_socket:stop(PB),
    ok.

reset_peer_config(ClusterA, ClusterB) ->
    FoldToPeerConfigPB = 
        fun(Node, Acc) ->
            {pb, {IP, Port}} =
                lists:keyfind(pb, 1, rt:connection_info(Node)),
            Acc0 = case Acc of "" -> ""; _ -> Acc ++ "|" end,
            Acc0 ++ IP ++ ":" ++ integer_to_list(Port) ++ ":pb"
        end,
    ClusterASnkPL = lists:foldl(FoldToPeerConfigPB, "", ClusterB),
    ClusterBSnkPL = lists:foldl(FoldToPeerConfigPB, "", ClusterA),
    ClusterASNkCfg = ?SNK_CONFIG(cluster_a, ClusterASnkPL),
    ClusterBSNkCfg = ?SNK_CONFIG(cluster_b, ClusterBSnkPL),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterASNkCfg) end, ClusterA),
    lists:foreach(
        fun(N) -> rt:set_advanced_conf(N, ClusterBSNkCfg) end, ClusterB),           

    lists:foreach(
        fun(N) -> rt:wait_for_service(N, riak_kv) end,
        ClusterA ++ ClusterB).


wait_for_queues_to_drain([], QueueName) ->
    lager:info("Queue ~w drained on nodes", [QueueName]);
wait_for_queues_to_drain([N|Rest], QueueName) ->
    rt:wait_until(
        fun() ->
            {QueueName, {0, 0, 0}} ==
                rpc:call(N, riak_kv_replrtq_src, length_rtq, [QueueName])
        end
    ),
    wait_for_queues_to_drain(Rest, QueueName).

        