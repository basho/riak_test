%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(partition_repair).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-behavior(riak_test).
-export([confirm/0]).

-define(FMT(S, L), lists:flatten(io_lib:format(S, L))).

%% @doc This test verifies that partition repair successfully repairs
%% all data after it has wiped out by a simulated disk crash.
confirm() ->
    SpamDir = rt_config:config_or_os_env(spam_dir),
    RingSize = list_to_integer(rt_config:config_or_os_env(ring_size, "16")),
    NVal = rt_config:config_or_os_env(n_val, undefined),
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),

    NumNodes = rt_config:config_or_os_env(num_nodes, 4),
    HOConcurrency = rt_config:config_or_os_env(ho_concurrency, 2),
    {_KVBackendMod, KVDataDir} = backend_mod_dir(KVBackend),
    Bucket = <<"scotts_spam">>,

    lager:info("Build a cluster"),
    lager:info("ring_creation_size: ~p", [RingSize]),
    lager:info("n_val: ~p", [NVal]),
    lager:info("num nodes: ~p", [NumNodes]),
    lager:info("riak_core handoff_concurrency: ~p", [HOConcurrency]),
    lager:info("riak_core vnode_management_timer 1000"),
    Conf = [
            {riak_core,
             [
              {ring_creation_size, RingSize},
              {handoff_manager_timeout, 1000},
              {vnode_management_timer, 1000},
              {handoff_concurrency, HOConcurrency}
             ]},
            %% @TODO This is only to test whether the test failure happens
            %% without AAE. The AAE errors found in the logs could be unrelated
            {riak_kv,
             [
              {anti_entropy, {off, []}}
             ]
            }
            %% {lager,
            %%  [{handlers,
            %%    [{lager_file_backend,
            %%      [{"./log/console.log",debug,10485760,"$D0",5}]}]}]}
           ],

    Nodes = rt:build_cluster(NumNodes, Conf),

    case NVal of
        undefined ->
            ok;
        _ ->
            lager:info("Set n_val to ~p", [NVal])
    end,

    lager:info("Insert Scott's spam emails"),
    Pbc = rt:pbc(hd(Nodes)),
    rt:pbc_put_dir(Pbc, Bucket, SpamDir),

    lager:info("Stash ITFs for each partition"),
    %% @todo Should riak_test guarantee that the scratch pad is clean instead?
    ?assertCmd("rm -rf " ++ base_stash_path()),
    %% need to load the module so riak can see the fold fun
    rt:load_modules_on_nodes([?MODULE], Nodes),
    Ring = rt:get_ring(hd(Nodes)),
    Owners = riak_core_ring:all_owners(Ring),
    [stash_data(riak_search, Owner) || Owner <- Owners],

    lager:info("Stash KV data for each partition"),
    [stash_data(riak_kv, Owner) || Owner <- Owners],

    %% TODO: parameterize backend
    lager:info("Emulate data loss for riak_kv, repair, verify correct data"),
    [kill_repair_verify(Owner, KVDataDir, riak_kv) || Owner <- Owners],

    lager:info("TEST PASSED"),
    pass.

kill_repair_verify({Partition, Node}, DataSuffix, Service) ->
    StashPath = stash_path(Service, Partition),
    {ok, [Stash]} = file:consult(StashPath),
    ExpectToVerify = dict:size(Stash),
    VNodeName = list_to_atom(atom_to_list(Service) ++ "_vnode"),

    %% kill the partition data
    Path = DataSuffix ++ "/" ++ integer_to_list(Partition),
    lager:info("Killing data for ~p on ~p at ~s", [Partition, Node, Path]),
    rt:clean_data_dir([Node], Path),

    %% force restart of vnode since some data is kept in memory
    lager:info("Restarting ~p vnode for ~p on ~p", [Service, Partition, Node]),
    {ok, Pid} = rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
                         [Partition, VNodeName]),
    ?assert(rpc:call(Node, erlang, exit, [Pid, kill_for_test])),
    
    %% We used to wait for the old pid to die here, but there is a delay between
    %% the vnode process dying and a new one being registered with the vnode
    %% manager. If we don't wait for the manager to return a new vnode pid, it's
    %% possible for the test to fail with a gen_server:call timeout.
    rt:wait_until(fun() -> {ok, Pid} =/=
                           rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
                                    [Partition, VNodeName])
                  end),

    lager:info("Verify data is missing"),
    ?assertEqual(0, count_data({Partition, Node})),

    %% repair the partition, ignore return for now
    lager:info("Invoking repair for ~p on ~p", [Partition, Node]),
    %% TODO: Don't ignore return, check version of Riak and if greater
    %% or equal to 1.x then expect OK.
    Return = rpc:call(Node, riak_kv_vnode, repair, [Partition]),

    %% Kill sending vnode to verify HO sender is killed
    %% {ok, [{KPart, KNode}|_]} = Return,
    %% {ok, NewPid} = rpc:call(KNode, riak_core_vnode_manager, get_vnode_pid,
    %%                         [KPart, VNodeName]),
    %% lager:info("killing src pid: ~p/~p ~p", [KNode, KPart, NewPid]),
    %% KR = rpc:call(KNode, erlang, exit, [NewPid, kill]),
    %% lager:info("result of kill: ~p", [KR]),
    %% timer:sleep(1000),
    %% ?assertNot(rpc:call(KNode, erlang, is_process_alive, [NewPid])),


    lager:info("return value of repair_index ~p", [Return]),
    lager:info("Wait for repair to finish"),
    wait_for_repair({Partition, Node}, 30),

    lager:info("Verify ~p on ~p is fully repaired", [Partition, Node]),
    Data2 = get_data({Partition, Node}),
    {Verified, NotFound} = dict:fold(verify(Node, Service, Data2), {0, []}, Stash),

    case NotFound of
        [] -> ok;
        _ ->
            NF = StashPath ++ ".nofound",
            lager:info("Some data not found, writing that to ~s", [NF]),
            ?assertEqual(ok, file:write_file(NF, io_lib:format("~p.", [NotFound])))
    end,
    %% NOTE: If the following assert fails then check the .notfound
    %% file written above...it contains all postings that were in the
    %% stash that weren't found after the repair.
    ?assertEqual({Service, ExpectToVerify}, {Service, Verified}),

    {ok, [{BeforeP, _BeforeOwner}=B, _, {AfterP, _AfterOwner}=A]} = Return,
    lager:info("Verify before src partition ~p still has data", [B]),
    StashPathB = stash_path(Service, BeforeP),
    {ok, [StashB]} = file:consult(StashPathB),
    ExpectToVerifyB = dict:size(StashB),
    BeforeData = get_data(B),
    {VerifiedB, NotFoundB} = dict:fold(verify(Node, Service, BeforeData), {0, []}, StashB),

    case NotFoundB of
        [] -> ok;
        _ ->
            NFB = StashPathB ++ ".notfound",
            ?assertEqual(ok, file:write_file(NFB, io_lib:format("~p.", [NotFoundB]))),
            throw({src_partition_missing_data, NFB})
    end,
    ?assertEqual(ExpectToVerifyB, VerifiedB),

    lager:info("Verify after src partition ~p still has data", [A]),
    StashPathA = stash_path(Service, AfterP),
    {ok, [StashA]} = file:consult(StashPathA),
    ExpectToVerifyA = dict:size(StashA),

    AfterData = get_data(A),
    {VerifiedA, NotFoundA} = dict:fold(verify(Node, Service, AfterData), {0, []}, StashA),

    case NotFoundA of
        [] -> ok;
        _ ->
            NFA = StashPathA ++ ".notfound",
            ?assertEqual(ok, file:write_file(NFA, io_lib:format("~p.", [NotFoundA]))),
            throw({src_partition_missing_data, NFA})
    end,
    ?assertEqual(ExpectToVerifyA, VerifiedA).


verify(Node, riak_kv, DataAfterRepair) ->
    fun(BKey, StashedValue, {Verified, NotFound}) ->
            StashedData={BKey, StashedValue},
            case dict:find(BKey, DataAfterRepair) of
                error -> {Verified, [StashedData|NotFound]};
                {ok, Value} ->
                    %% NOTE: since kv679 fixes, the binary values may
                    %% not be equal where a new epoch-actor-entry has
                    %% been added to the repaired value in the vnode
                    case gte(Node, Value, StashedValue, BKey) of
                        true -> {Verified+1, NotFound};
                        false -> {Verified, [StashedData|NotFound]}
                    end
            end
    end.

%% @private gte checks that `Value' is _at least_ `StashedValue'. With
%% the changes for kv679 when a vnode receives a write of a key that
%% contains the vnode's id as an entry in the version vector, it adds
%% a new actor-epoch-entry to the version vector to guard against data
%% loss from repeated events (remember a VV history is supposed to be
%% unique!) This function then must deserialise the stashed and vnode
%% data and check that they are equal, or if not, the only difference
%% is an extra epoch actor in the vnode value's vclock.
gte(Node, Value, StashedData, {B, K}) ->
    VnodeObject = riak_object:from_binary(B, K, Value),
    StashedObject = riak_object:from_binary(B, K, StashedData),
    %% NOTE: we need a ring and all that jazz for bucket props, needed
    %% by merge, so use an RPC to merge on a riak node.
    Merged = rpc:call(Node, riak_object, syntactic_merge, [VnodeObject, StashedObject]),
    riak_object:equal(VnodeObject, Merged).

is_true(X) ->
    X == true.

count_data({Partition, Node}) ->
    dict:size(get_data({Partition, Node})).

get_data({Partition, Node}) ->
    VMaster = riak_kv_vnode_master,

    %% TODO: add compile time support for riak_test
    Req = {riak_core_fold_req_v1, fun stash_kv/3, dict:new()},

    Data = riak_core_vnode_master:sync_command({Partition, Node},
                                               Req,
                                               VMaster,
                                               rt_config:get(rt_max_wait_time)),
    Data.

stash_data(Service, {Partition, Node}) ->
    File = stash_path(Service, Partition),
    ?assertEqual(ok, filelib:ensure_dir(File)),
    lager:info("Stashing ~p/~p at ~p to ~p", [Service, Partition, Node, File]),
    Postings = get_data({Partition, Node}),
    ?assertEqual(ok, file:write_file(File, io_lib:format("~p.", [Postings]))).

stash_kv(Key, Value, Stash) ->
    dict:store(Key, Value, Stash).

base_stash_path() ->
    rt_config:get(rt_scratch_dir) ++ "/dev/data_stash/".

stash_path(Service, Partition) ->
    base_stash_path() ++ atom_to_list(Service) ++ "/" ++ integer_to_list(Partition) ++ ".stash".

file_list(Dir) ->
    filelib:wildcard(Dir ++ "/*").

wait_for_repair(_, 0) ->
    throw(wait_for_repair_max_tries);
wait_for_repair({Partition, Node}, Tries) ->
    Reply = rpc:call(Node, riak_kv_vnode, repair_status, [Partition]),
    case Reply of
        not_found -> ok;
        in_progress ->
            timer:sleep(timer:seconds(1)),
            wait_for_repair({Partition, Node}, Tries - 1)
    end.

data_path(Node, Suffix, Partition) ->
    [Name, _] = string:tokens(atom_to_list(Node), "@"),
    Base = rt_config:get('rtdev_path.current') ++ "/dev/" ++ Name ++ "/data",
    Base ++ "/" ++ Suffix ++ "/" ++ integer_to_list(Partition).

backend_mod_dir(undefined) ->
    %% riak_test defaults to bitcask when undefined
    backend_mod_dir(bitcask);
backend_mod_dir(bitcask) ->
    {riak_kv_bitcask_backend, "bitcask"};
backend_mod_dir(eleveldb) ->
    {riak_kv_eleveldb_backend, "leveldb"};
backend_mod_dir(leveled) ->
    {riak_kv_leveled_backed, "leveled"}.
