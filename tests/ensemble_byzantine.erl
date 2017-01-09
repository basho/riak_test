%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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

-module(ensemble_byzantine).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

-define(NUM_NODES, 8).
-define(NVAL, 5).

confirm() ->
    NumNodes = ?NUM_NODES,
    NVal = ?NVAL,
    _Quorum = NVal div 2 + 1,
    Config = config(),
    lager:info("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(NumNodes, Config, NVal),
    vnode_util:load(Nodes),
    Node = hd(Nodes),
    ensemble_util:wait_until_stable(Node, NVal),

    create_strong_bucket_type(Node, NVal),

    Bucket = {<<"strong">>, <<"test">>},
    Key = <<"test-key">>,
    Val = <<"test-val">>,

    {ok, PL} = get_preflist(Node, Bucket, Key, NVal),
    ?assertEqual(NVal, length(PL)),
    lager:info("PREFERENCE LIST: ~n  ~p", [PL]),

    PBC = rt:pbc(Node),

    normal_write_and_read(PBC, Bucket, Key, Val),
    test_lose_one_node_one_partition(PBC, Bucket, Key, Val, PL),
    test_lose_all_but_one_partition(PBC, Bucket, Key, Val, PL),
    test_lose_minority_synctrees(PBC, Bucket, Key, Val, PL),
    test_lose_majority_synctrees(PBC, Bucket, Key, Val, PL),
    test_lose_minority_synctrees_one_node_partitioned(PBC, Bucket, Key, Val,
                                                      PL, Nodes),
    test_lose_all_data_and_trees_except_one_node(PBC, Bucket, Key, Val, PL),
    {ok, _NewVal} = test_backup_restore_data_not_trees(Bucket, Key, Val, PL),
    test_lose_all_data(PBC, Bucket, Key, PL),

    pass.

config() ->
    [{riak_core, [{default_bucket_props,
                   [
                    {n_val, 5},
                    {allow_mult, true},
                    {dvv_enabled, true}
                   ]},
                  {vnode_management_timer, 1000},
                  {ring_creation_size, 16},
                  {enable_consensus, true},
                  {target_n_val, 8}]}].

test_lose_minority_synctrees(PBC, Bucket, Key, Val, PL) ->
    Minority = minority_vnodes(PL),
    assert_lose_synctrees_and_recover(PBC, Bucket, Key, Val, PL, Minority).

test_lose_majority_synctrees(PBC, Bucket, Key, Val, PL) ->
    Majority= majority_vnodes(PL),
    assert_lose_synctrees_and_recover(PBC, Bucket, Key, Val, PL, Majority).

test_lose_minority_synctrees_one_node_partitioned(PBC, Bucket, Key, Val, PL,
                                                  Nodes) ->
    Minority = minority_vnodes(PL),
    {{Idx0, Node0}, primary} = hd(PL),
    Ensemble = {kv, Idx0, 5},
    Rest = [Node || {{_, Node}, _} <- lists:sublist(PL, ?NVAL-1)],

    %% Partition off the last node
    {{_, PartitionedNode},_} = lists:nth(?NVAL, PL),
    PartInfo = rt:partition([PartitionedNode], Nodes -- [PartitionedNode]),

    %% Wipe a minority of nodes
    [wipe_tree(Ensemble, Idx, Node) || {{Idx, Node}, _} <- Minority],
    kill_peers(Ensemble, Rest),

    %% With a majority of nodes down (minority reboot, 1 partitioned) we
    %% shouldn't be able to reach quorum. This is because we now have a majority
    %% of untrusted synctrees, and all nodes are not online.
    timer:sleep(10000),
    {error, <<"timeout">>} = riakc_pb_socket:get(PBC, Bucket, Key, []),

    %% Heal the partition so that we can get quorum
    rt:heal(PartInfo),
    ensemble_util:wait_until_quorum(Node0, Ensemble),
    assert_valid_read(PBC, Bucket, Key, Val).

test_lose_all_but_one_partition(PBC, Bucket, Key, Val, PL) ->
    Wiped = tl(PL),
    {{Idx0, Node0}, primary} = hd(PL),
    Ensemble = {kv, Idx0, 5},
    lager:info("Wiping Data on Following Vnodes: ~p", [Wiped]),
    wipe_partitions(Wiped),
    ensemble_util:wait_until_quorum(Node0, Ensemble),
    assert_valid_read(PBC, Bucket, Key, Val).

test_lose_one_node_one_partition(PBC, Bucket, Key, Val, PL) ->
    {{Idx0, Node0}, primary} = hd(PL),
    Ensemble = {kv, Idx0, 5},
    Leader = ensemble_util:get_leader_pid(Node0, Ensemble),
    LeaderNode = node(Leader),
    LeaderIdx = get_leader_idx(PL, LeaderNode),
    lager:info("Wiping Idx ~p data on LeaderNode ~p", [LeaderIdx, LeaderNode]),
    wipe_partition(LeaderIdx, LeaderNode),
    ensemble_util:wait_until_quorum(LeaderNode, Ensemble),
    assert_valid_read(PBC, Bucket, Key, Val).

test_lose_all_data_and_trees_except_one_node(PBC, Bucket, Key, Val, PL) ->
    Wiped = tl(PL),
    {{Idx0, Node0}, primary} = hd(PL),
    Ensemble = {kv, Idx0, 5},
    wipe_partitions(Wiped),
    wipe_trees(Ensemble, Wiped),
    ensemble_util:wait_until_quorum(Node0, Ensemble),
    assert_valid_read(PBC, Bucket, Key, Val).

test_backup_restore_data_not_trees(Bucket, Key, _Val, PL) ->
    {{Idx, Node}, primary} = hd(PL),
    Ensemble = {kv, Idx, 5},
    stop_nodes(PL),
    backup_data(1, PL),
    start_nodes(PL),
    PBC = rt:pbc(Node),
    ensemble_util:wait_until_quorum(Node, Ensemble),
    timer:sleep(10000),

    Obj0 = rt:pbc_read(PBC, Bucket, Key),
    NewVal = <<"test-val2">>,
    Obj = riakc_obj:update_value(Obj0, NewVal),
    riakc_pb_socket:put(PBC, Obj),
    assert_valid_read(PBC, Bucket, Key, NewVal),

    stop_nodes(PL),
    %% Backup the new data.
    backup_data(2, PL),
    %% Restore old data
    restore_data(1, PL),
    start_nodes(PL),
    PBC1 = rt:pbc(Node),
    ensemble_util:wait_until_quorum(Node, Ensemble),

    %% Fail to read the restored data. Trees match newer data than what was
    %% restored
    assert_failed_read(PBC1, Bucket, Key),
    stop_nodes(PL),

    %% Restore New Data that matches trees
    restore_data(2, PL),
    start_nodes(PL),
    PBC2 = rt:pbc(Node),
    ensemble_util:wait_until_quorum(Node, Ensemble),

    assert_valid_read(PBC2, Bucket, Key, NewVal),
    {ok, NewVal}.

test_lose_all_data(PBC, Bucket, Key, PL) ->
    wipe_partitions(PL),
    {error, _}=E = riakc_pb_socket:get(PBC, Bucket, Key, []),
    lager:info("All data loss error = ~p", [E]).

assert_valid_read(PBC, Bucket, Key, Val) ->
    ReadFun = fun() ->
                      Obj = rt:pbc_read(PBC, Bucket, Key),
                      Val =:= riakc_obj:get_value(Obj)
              end,
    ?assertEqual(ok, rt:wait_until(ReadFun)).

assert_failed_read(PBC, Bucket, Key) ->
    ?assertMatch({error, _}, riakc_pb_socket:get(PBC, Bucket, Key, [])).

normal_write_and_read(PBC, Bucket, Key, Val) ->
    lager:info("Writing a consistent key"),
    ok = rt:pbc_write(PBC, Bucket, Key, Val),
    lager:info("Read key to verify it exists"),
    assert_valid_read(PBC, Bucket, Key, Val).

stop_nodes(PL) ->
    [rt:stop_and_wait(Node) || {{_, Node}, _} <- PL].

start_nodes(PL) ->
    [rt:start_and_wait(Node) || {{_, Node}, _} <- PL].

data_path(Node) ->
    ?HARNESS:node_path(Node) ++ "/data/"++backend_dir().

backup_path(Node, N) ->
    data_path(Node) ++ integer_to_list(N) ++ ".bak".

backup_data(N, PL) ->
    [backup_node(Node, N) || {{_, Node}, _} <- PL].

backup_node(Node, N) ->
    Path = data_path(Node),
    BackupPath = backup_path(Node, N),
    Cmd = "cp -R "++Path++" "++BackupPath,
    lager:info("~p", [os:cmd(Cmd)]).

restore_data(N, PL) ->
    [restore_node(Node, N) || {{_, Node}, _} <- PL].

restore_node(Node, N) ->
    Path = data_path(Node),
    BackupPath = backup_path(Node, N),
    rm_backend_dir(Node),
    Cmd = "mv "++BackupPath++" "++Path,
    ?assertEqual([], os:cmd(Cmd)).

assert_lose_synctrees_and_recover(PBC, Bucket, Key, Val, PL, ToLose) ->
    {{Idx0, Node0}, primary} = hd(PL),
    Ensemble = {kv, Idx0, 5},
    [wipe_tree(Ensemble, Idx, Node) || {{Idx, Node}, _} <- ToLose],
    ensemble_util:wait_until_quorum(Node0, Ensemble),
    assert_valid_read(PBC, Bucket, Key, Val).

majority_vnodes(PL) ->
    Num = ?NVAL div 2 + 1,
    {Majority, _} = lists:split(Num, PL),
    Majority.

minority_vnodes(PL) ->
    Num = ?NVAL div 2,
    {Minority, _} = lists:split(Num, PL),
    Minority.

get_leader_idx(PL, LeaderNode) ->
    [{LeaderIdx, _}] = [{Idx, N}  || {{Idx, N}, _} <- PL, N =:= LeaderNode],
    LeaderIdx.

kill_peers(Ensemble, Nodes) ->
    Node = hd(Nodes),
    {_, [View | _]} = rpc:call(Node, riak_ensemble_manager, get_views, [Ensemble]),
    Peers = [P || P={_Id, N} <- View, lists:member(N, Nodes)],
    lager:info("Killing Peers: ~p", [Peers]),
    Pids = [rpc:call(Node, riak_ensemble_manager, get_peer_pid,
                     [Ensemble, Peer]) || Peer <- Peers],
    [exit(Pid, kill) || Pid <- Pids, Pid =/= undefined].

wipe_partitions(PL) ->
    [wipe_partition(Idx, Node) || {{Idx, Node}, _} <- PL].

wipe_trees(Ensemble, PL) ->
    [wipe_tree(Ensemble, Idx, Node) || {{Idx, Node}, _} <- PL].

wipe_tree(Ensemble, Idx, Node) ->
    rt:clean_data_dir([Node], "ensembles/trees/kv_"++integer_to_list(Idx)),
    {_, [View | _]} = rpc:call(Node, riak_ensemble_manager, get_views, [Ensemble]),
    [Peer] = [P || P={_Id, N} <- View, Node =:= N],
    Pid = rpc:call(Node, riak_ensemble_manager, get_peer_pid, [Ensemble, Peer]),
    lager:info("Peer= ~p, Pid = ~p", [Peer, Pid]),
    exit(Pid, kill).

wipe_partition(Idx, Node) ->
    rm_partition_dir(Idx, Node),
    vnode_util:kill_vnode({Idx, Node}).

rm_backend_dir(Node) ->
    rt:clean_data_dir([Node], backend_dir()).

rm_partition_dir(Idx, Node) ->
    RelativePath = backend_dir() ++ "/" ++ integer_to_list(Idx),
    rt:clean_data_dir([Node], RelativePath).

backend_dir() ->
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    backend_dir(KVBackend).

backend_dir(undefined) ->
    %% riak_test defaults to bitcask when undefined
    backend_dir(eleveldb);
backend_dir(bitcask) ->
    "bitcask";
backend_dir(eleveldb) ->
    "leveldb".

get_preflist(Node, Bucket, Key, NVal) ->
    DocIdx = rpc:call(Node, riak_core_util, chash_std_keyfun, [{Bucket, Key}]),
    PL = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, NVal, riak_kv]),
    {ok, PL}.

create_strong_bucket_type(Node, NVal) ->
    lager:info("Creating/activating 'strong' bucket type"),
    rt:create_and_activate_bucket_type(Node, <<"strong">>,
                                       [{consistent, true}, {n_val, NVal}]),
    ensemble_util:wait_until_stable(Node, NVal).
