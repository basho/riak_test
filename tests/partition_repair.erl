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

-import(rt, [deploy_nodes/1,
             enable_search_hook/2,
             get_os_env/1,
             get_os_env/2,
             get_ring/1,
             join/2,
             update_app_config/2]).

-define(FMT(S, L), lists:flatten(io_lib:format(S, L))).

%% @doc This test verifies that partition repair successfully repairs
%% all data after it has wiped out by a simulated disk crash.
confirm() ->
    SpamDir = get_os_env("SPAM_DIR"),
    RingSize = list_to_integer(get_os_env("RING_SIZE", "16")),
    NVal = get_os_env("N_VAL", undefined),
    KVBackend = get_os_env("KV_BACKEND", "bitcask"),
    NumNodes = list_to_integer(get_os_env("NUM_NODES", "4")),
    HOConcurrency = list_to_integer(get_os_env("HO_CONCURRENCY", "2")),
    {KVBackendMod, KVDataDir} = backend_mod_dir(KVBackend),
    Bucket = <<"scotts_spam">>,

    lager:info("Build a cluster"),
    lager:info("riak_search enabled: true"),
    lager:info("ring_creation_size: ~p", [RingSize]),
    lager:info("n_val: ~p", [NVal]),
    lager:info("KV backend: ~s", [KVBackend]),
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
            {riak_kv,
             [
              {storage_backend, KVBackendMod}
             ]},
            {riak_search,
             [
              {enabled, true}
             ]}
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
            lager:info("Set n_val to ~p", [NVal]),
            set_search_schema_nval(Bucket, NVal)
    end,

    lager:info("Enable search hook"),
    enable_search_hook(hd(Nodes), Bucket),

    lager:info("Insert Scott's spam emails"),
    {ok, C} = riak:client_connect(hd(Nodes)),
    [put_file(C, Bucket, F) || F <- file_list(SpamDir)],

    lager:info("Stash ITFs for each partition"),
    %% need to load the module so riak can see the fold fun
    load_module_on_riak(Nodes, ?MODULE),
    Ring = get_ring(hd(Nodes)),
    Owners = riak_core_ring:all_owners(Ring),
    [stash_data(riak_search, Owner) || Owner <- Owners],

    lager:info("Stash KV data for each partition"),
    [stash_data(riak_kv, Owner) || Owner <- Owners],

    lager:info("Emulate data loss for riak_search, repair, verify correct data"),
    [kill_repair_verify(Owner, "merge_index", riak_search) || Owner <- Owners],

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
    Path = data_path(Node, DataSuffix, Partition),
    %% [Name, _] = string:tokens(atom_to_list(Node), "@"),
    %% Path = rt:config(rtdev_path) ++ "/dev/" ++ Name ++ "/data/merge_index/" ++ integer_to_list(Partition),
    lager:info("Killing data for ~p on ~p at ~p", [Partition, Node, Path]),
    
    %% @todo Warning: ?assertCmd assumes rtdev, and assuming rtdev is a riak_test no-no going forward.
    %% This will break (as will most all other instances of ?assertCmds)
    ?assertCmd("rm -rf " ++ Path),

    %% force restart of vnode since some data is kept in memory
    lager:info("Restarting ~p vnode for ~p on ~p", [Service, Partition, Node]),
    {ok, Pid} = rpc:call(Node, riak_core_vnode_manager, get_vnode_pid,
                         [Partition, VNodeName]),
    ?assert(rpc:call(Node, erlang, exit, [Pid, kill_for_test])),
    timer:sleep(100),
    ?assertNot(rpc:call(Node, erlang, is_process_alive, [Pid])),

    lager:info("Verify data is missing"),
    ?assertEqual(0, count_data(Service, {Partition, Node})),

    %% repair the partition, ignore return for now
    lager:info("Invoking repair for ~p on ~p", [Partition, Node]),
    %% TODO: Don't ignore return, check version of Riak and if greater
    %% or equal to 1.x then expect OK.
    Return =
        case Service of
            riak_kv ->
                rpc:call(Node, riak_kv_vnode, repair, [Partition]);
            riak_search ->
                rpc:call(Node, riak_search_vnode, repair, [Partition])
        end,

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
    wait_for_repair(Service, {Partition, Node}, 30),

    lager:info("Verify ~p on ~p is fully repaired", [Partition, Node]),
    Data2 = get_data(Service, {Partition, Node}),
    {Verified, NotFound} = dict:fold(verify(Service, Data2), {0, []}, Stash),
    case NotFound of
        [] -> ok;
        _ ->
            NF = StashPath ++ ".nofound",
            ?assertEqual(ok, file:write_file(NF, io_lib:format("~p.", [NotFound])))
    end,
    %% NOTE: If the following assert fails then check the .notfound
    %% file written above...it contains all postings that were in the
    %% stash that weren't found after the repair.
    ?assertEqual(ExpectToVerify, Verified),

    {ok, [{BeforeP, _BeforeOwner}=B, _, {AfterP, _AfterOwner}=A]} = Return,
    lager:info("Verify before src partition ~p still has data", [B]),
    StashPathB = stash_path(Service, BeforeP),
    {ok, [StashB]} = file:consult(StashPathB),
    ExpectToVerifyB = dict:size(StashB),
    BeforeData = get_data(Service, B),
    {VerifiedB, NotFoundB} = dict:fold(verify(Service, BeforeData), {0, []}, StashB),
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
    AfterData = get_data(Service, A),
    {VerifiedA, NotFoundA} = dict:fold(verify(Service, AfterData), {0, []}, StashA),
    case NotFoundA of
        [] -> ok;
        _ ->
            NFA = StashPathA ++ ".notfound",
            ?assertEqual(ok, file:write_file(NFA, io_lib:format("~p.", [NotFoundA]))),
            throw({src_partition_missing_data, NFA})
    end,
    ?assertEqual(ExpectToVerifyA, VerifiedA).


verify(riak_kv, DataAfterRepair) ->
    fun(BKey, StashedValue, {Verified, NotFound}) ->
            StashedData={BKey, StashedValue},
            case dict:find(BKey, DataAfterRepair) of
                error -> {Verified, [StashedData|NotFound]};
                {ok, Value} ->
                    if Value == StashedValue -> {Verified+1, NotFound};
                       true -> {Verified, [StashedData|NotFound]}
                    end
            end
    end;

verify(riak_search, PostingsAfterRepair) ->
    fun(IFT, StashedPostings, {Verified, NotFound}) ->
            StashedPosting={IFT, StashedPostings},
            case dict:find(IFT, PostingsAfterRepair) of
                error -> {Verified, [StashedPosting|NotFound]};
                {ok, RepairedPostings} ->
                    case lists:all(fun is_true/1,
                                   [lists:member(P, RepairedPostings)
                                    || P <- StashedPostings]) of
                        true -> {Verified+1, NotFound};
                        false -> {Verified, [StashedPosting|NotFound]}
                    end
            end
    end.

is_true(X) ->
    X == true.

count_data(Service, {Partition, Node}) ->
    dict:size(get_data(Service, {Partition, Node})).

get_data(Service, {Partition, Node}) ->
    VMaster =
        case Service of
            riak_kv -> riak_kv_vnode_master;
            riak_search -> riak_search_vnode_master
        end,
    %% TODO: add compile time support for riak_test
    Req =
        case Service of
            riak_kv ->
                {riak_core_fold_req_v1, fun stash_kv/3, dict:new()};
            riak_search ->
                {riak_core_fold_req_v1, fun stash_search/3, dict:new()}
        end,
    Data = riak_core_vnode_master:sync_command({Partition, Node},
                                               Req,
                                               VMaster,
                                               infinity),
    Data.

stash_data(Service, {Partition, Node}) ->
    File = stash_path(Service, Partition),
    ?assertEqual(ok, filelib:ensure_dir(File)),
    lager:info("Stashing ~p/~p at ~p to ~p", [Service, Partition, Node, File]),
    Postings = get_data(Service, {Partition, Node}),
    ?assertEqual(ok, file:write_file(File, io_lib:format("~p.", [Postings]))).

stash_kv(Key, Value, Stash) ->
    dict:store(Key, Value, Stash).

stash_search({_I,{_F,_T}}=K, _Postings=V, Stash) ->
    dict:append_list(K, V, Stash).


%% @todo broken when run in the style of rtdev_mixed.
stash_path(Service, Partition) ->
    Path = rt:config(rtdev_path) ++ "/dev/data_stash",
    Path ++ "/" ++ atom_to_list(Service) ++ "/" ++ integer_to_list(Partition) ++ ".stash".

file_list(Dir) ->
    filelib:wildcard(Dir ++ "/*").

wait_for_repair(_, _, 0) ->
    throw(wait_for_repair_max_tries);
wait_for_repair(Service, {Partition, Node}, Tries) ->
    Reply =
        case Service of
            riak_kv ->
                rpc:call(Node, riak_kv_vnode, repair_status, [Partition]);
            riak_search ->
                rpc:call(Node, riak_search_vnode, repair_status, [Partition])
        end,
    case Reply of
        not_found -> ok;
        in_progress ->
            timer:sleep(timer:seconds(1)),
            wait_for_repair(Service, {Partition, Node}, Tries - 1)
    end.

data_path(Node, Suffix, Partition) ->
    [Name, _] = string:tokens(atom_to_list(Node), "@"),
    Base = rt:config(rtdev_path) ++ "/dev/" ++ Name ++ "/data",
    Base ++ "/" ++ Suffix ++ "/" ++ integer_to_list(Partition).

backend_mod_dir("bitcask") ->
    {riak_kv_bitcask_backend, "bitcask"};
backend_mod_dir("leveldb") ->
    {riak_kv_eleveldb_backend, "leveldb"}.


%%
%% STUFF TO MOVE TO rt?
%%
put_file(C, Bucket, File) ->
    K = list_to_binary(string:strip(os:cmd("basename " ++ File), right, $\n)),
    {ok, Val} = file:read_file(File),
    O = riak_object:new(Bucket, K, Val, "text/plain"),
    ?assertEqual(ok, C:put(O)).

load_module_on_riak(Nodes, Mod) ->
    {Mod, Bin, File} = code:get_object_code(Mod),
    [?assertEqual({module, Mod},
                  rpc:call(Node, code, load_binary, [Mod, File, Bin]))
     || Node <- Nodes].

-spec set_search_schema_nval(binary(), pos_integer()) -> ok.
set_search_schema_nval(Bucket, NVal) ->
    %% TODO: Search currently offers no easy way to pragmatically
    %% change a schema and save it.  This is because the external and
    %% internal formats of the schema are different.  The parser reads
    %% the external format and an internal representation is created
    %% which is then stored/access via `riak_search_config'.  Rather
    %% than allowing the internal format to be modified and set you
    %% must send the update in the external format.
    BucketStr = binary_to_list(Bucket),
    SearchCmd = ?FMT("~s/dev/dev1/bin/search-cmd", [rt:config(rtdev_path)]),
    GetSchema = ?FMT("~s show-schema ~s > current-schema",
                     [SearchCmd, BucketStr]),
    ModifyNVal = ?FMT("sed -E 's/n_val, [0-9]+/n_val, ~s/' "
                      "current-schema > new-schema",
                      [NVal]),
    SetSchema = ?FMT("~s set-schema ~s new-schema", [SearchCmd, BucketStr]),
    ClearCache = ?FMT("~s clear-schema-cache", [SearchCmd]),
    ?assertCmd(GetSchema),
    ?assertCmd(ModifyNVal),
    ?assertCmd(SetSchema),
    ?assertCmd(ClearCache).
