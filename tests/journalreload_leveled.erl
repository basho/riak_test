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
%% @doc Prove restart from an empty ledger in recalc mode 
%%

-module(journalreload_leveled).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(DEFAULT_RING_SIZE, 8).
-define(NUM_NODES, 4).
-define(NUM_KEYS_PERNODE, 2048).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(VAL_FLAG1, "U1abcdefghijklmnopqrstuvwxyz").
-define(VAL_FLAG2, "U2abcdefghijklmnopqrstuvwxyz").
-define(VAL_FLAG3, "U3abcdefghijklmnopqrstuvwxyz").
-define(VAL_FLAG4, "U4abcdefghijklmnopqrstuvwxyz").
-define(VAL_FLAG5, "U5abcdefghijklmnopqrstuvwxyz").
-define(INDEX, "accidx").
-define(DELTA_COUNT, 10).
-define(COMPACTION_WAIT, 30).
-define(CFG(Reload, DeleteMode),
        [{riak_kv,
          [
           {anti_entropy, {off, []}},
           {tictacaae_active, passive},
           {leveled_reload_recalc, Reload},
           {delete_mode, DeleteMode}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]},
          {leveled,
          [
              {journal_objectcount, 4000},
              {max_pencillercachesize, 8000}
          ]}]
       ).

confirm() ->
    Nodes0 = rt:build_cluster(?NUM_NODES, ?CFG(true, immediate)),
    journal_reload(Nodes0, false),
    ok = rt:clean_cluster(Nodes0),

    Nodes1 = rt:build_cluster(?NUM_NODES, ?CFG(true, immediate)),
    ok = journal_reload(Nodes1, true),
    ok = rt:clean_cluster(Nodes1),

    Nodes2 = rt:build_cluster(?NUM_NODES, ?CFG(true, keep)),
    ok = journal_reload(Nodes2, true),

    pass.

journal_reload(Nodes, WithCompact) ->

    KVBackend = proplists:get_value(backend, riak_test_runner:metadata()),
    KeyCount = key_count(KVBackend, Nodes),
    NumKeysPerNode = KeyCount div length(Nodes),
    
    KeyLoadFun =
        fun(Value) ->
            fun(Node, AccCount) ->
                KVs = test_data(AccCount,
                                    AccCount + NumKeysPerNode - 1,
                                    list_to_binary(Value)),
                ok = write_data(Node, KVs),
                AccCount + NumKeysPerNode
            end
        end,
    
    DeleteFun =
        fun(Node, AccCount) ->
            PBC = rt:pbc(Node),
            ok = lists:foreach(fun(N) -> 
                                    ok = riakc_pb_socket:delete(PBC,
                                                                ?BUCKET,
                                                                to_key(N))
                                end,
                                lists:seq(AccCount,
                                            AccCount + NumKeysPerNode -1)),
            AccCount + NumKeysPerNode
        end,

    lists:foldl(KeyLoadFun(?VAL_FLAG1), 1, Nodes),
    lager:info("Loaded ~w objects", [KeyCount]),
    lists:foldl(DeleteFun, 1, Nodes),
    lager:info("Deleted ~w objects", [KeyCount]),
    lists:foldl(KeyLoadFun(?VAL_FLAG2), 1, Nodes),
    lager:info("Reloaded ~w objects", [KeyCount]),
    lists:foldl(KeyLoadFun(?VAL_FLAG3), 1, Nodes),
    lager:info("Reloaded ~w objects", [KeyCount]),
    lists:foldl(KeyLoadFun(?VAL_FLAG4), 1, Nodes),
    lager:info("Reloaded ~w objects", [KeyCount]),

    check_objects(hd(Nodes), 1, KeyCount, ?VAL_FLAG4),

    IndexMultiple = 3,
    
    test_by_backend(KVBackend, Nodes, WithCompact, IndexMultiple).


test_by_backend(undefined, Nodes, WithCompact, IndexMultiple) ->
    test_by_backend(bitcask, Nodes, WithCompact, IndexMultiple);
test_by_backend(bitcask, Nodes, _WC, _IM) ->
    not_supported_test(Nodes);
test_by_backend(eleveldb, Nodes, _WC, IndexMultiple) ->
    KeyCount = key_count(eleveldb, Nodes),
    IndexEntries = check_index(hd(Nodes)),
    lager:info("Number of IndexEntries ~w~n", [IndexEntries]),
    true = IndexEntries == IndexMultiple * KeyCount,
    not_supported_test(Nodes);
test_by_backend(CapableBackend, Nodes, WithCompact, IndexMultiple) ->

    KeyCount= key_count(CapableBackend, Nodes),
    IndexEntries = check_index(hd(Nodes)),
    lager:info("Number of IndexEntries ~w~n", [IndexEntries]),
    true = IndexEntries == IndexMultiple * KeyCount,

    lager:info("Clean backup folder if present"),
    rt:clean_data_dir(Nodes, "backup"),

    case WithCompact of
        true ->
            prompt_compactions(Nodes),
            lager:info("Sleep for ~w seconds to wait for compaction",
                        [?COMPACTION_WAIT]),
            timer:sleep(?COMPACTION_WAIT * 1000);
        false ->
            ok
    end,

    {CoverNumber, RVal} = {?N_VAL, 2},

    lager:info("Testing capable backend ~w", [CapableBackend]),
    {ok, C} = riak:client_connect(hd(Nodes)),

    lager:info("Backup all nodes to succeed"),
    {ok, true} =
        riak_client:hotbackup("./data/backup/", ?N_VAL, CoverNumber, C),
    
    lager:info("Change some keys"),
    Changes2 = test_data(1, ?DELTA_COUNT, list_to_binary(?VAL_FLAG5)),
    ok = write_data(hd(Nodes), Changes2),
    check_objects(hd(Nodes), 1, ?DELTA_COUNT, ?VAL_FLAG5),

    lager:info("Stop the primary cluster and start from backup"),
    lists:foreach(fun rt:stop_and_wait/1, Nodes),
    rt:clean_data_dir(Nodes, backend_dir()),
    rt:restore_data_dir(Nodes, backend_dir(), "backup/"),
    lists:foreach(fun rt:start_and_wait/1, Nodes),

    rt:wait_for_cluster_service(Nodes, riak_kv),

    lager:info("Confirm changed objects are unchanged"),
    check_objects(hd(Nodes), 1, ?DELTA_COUNT, ?VAL_FLAG4, RVal),
    lager:info("Confirm last 5K unchanged objects are unchanged"),
    check_objects(hd(Nodes), 1, KeyCount, ?VAL_FLAG4, RVal),
    IndexEntries = check_index(hd(Nodes)),
    lager:info("Number of IndexEntries ~w~n", [IndexEntries]),
    true = IndexEntries == IndexMultiple * KeyCount,

    ok.


key_count(undefined, Nodes) ->
    key_count(bitcask, Nodes);
key_count(bitcask, Nodes) ->
    (?NUM_KEYS_PERNODE div 10) * length(Nodes);
key_count(eleveldb, Nodes) ->
    (?NUM_KEYS_PERNODE div 10) * length(Nodes);
key_count(leveled, Nodes) ->
    ?NUM_KEYS_PERNODE * length(Nodes).

not_supported_test(Nodes) ->
    {ok, C} = riak:client_connect(hd(Nodes)),
    lager:info("Backup all nodes to fail"),
    {ok, false} = riak_client:hotbackup("./data/backup/", ?N_VAL, ?N_VAL, C),
    ok.


to_key(N) ->
    list_to_binary(io_lib:format("K~6..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].

write_data(Node, KVs) ->
    write_data(Node, KVs, []).

write_data(Node, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
        Obj0 =
            case riakc_pb_socket:get(PB, ?BUCKET, K) of
                {ok, Prev} ->
                    riakc_obj:update_value(Prev, V);
                _ ->
                    riakc_obj:new(?BUCKET, K, V)
            end,
            {_Mega, Secs, _Micro} = os:timestamp(),
            SecIdx = [{{binary_index, ?INDEX},
                        [integer_to_binary(Secs)]}],
            MD0 = riakc_obj:get_update_metadata(Obj0),
            MD1 = riakc_obj:add_secondary_index(MD0, SecIdx),
            Obj1 = riakc_obj:update_metadata(Obj0, MD1),
            ?assertMatch(ok, riakc_pb_socket:put(PB, Obj1, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    ok.


check_objects(Node, KCStart, KCEnd, VFlag) ->
    check_objects(Node, KCStart, KCEnd, VFlag, 2).

check_objects(Node, KCStart, KCEnd, VFlag, RVal) ->
    V = list_to_binary(VFlag),
    PBC = rt:pbc(Node),
    Opts = [{notfound_ok, false}, {r, RVal}],
    CheckFun = 
        fun(K, Acc) ->
            Key = to_key(K),
            case riakc_pb_socket:get(PBC, ?BUCKET, Key, Opts) of
                {ok, Obj} ->
                    RetValue = riakc_obj:get_value(Obj),
                    ?assertMatch(RetValue, <<Key/binary, V/binary>>),
                    Acc;
                {error, notfound} ->
                    lager:error("Search for Key ~w not found", [K]),
                    [K|Acc]
            end
        end,
    MissedKeys = lists:foldl(CheckFun, [], lists:seq(KCStart, KCEnd)),
    ?assertMatch([], MissedKeys),
    riakc_pb_socket:stop(PBC),
    true.

check_index(Node) ->
    PBC = rt:pbc(Node),
    {ok, Results} = 
        riakc_pb_socket:get_index_range(PBC,
                                        ?BUCKET, ?INDEX ++ "_bin",
                                        integer_to_binary(0),
                                        integer_to_binary(999999)),
    length(Results?INDEX_RESULTS.keys).

backend_dir() ->
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    backend_dir(KVBackend).

backend_dir(undefined) ->
    %% riak_test defaults to bitcask when undefined
    backend_dir(bitcask);
backend_dir(bitcask) ->
    "bitcask";
backend_dir(eleveldb) ->
    "leveldb";
backend_dir(leveled) ->
    "leveled".

prompt_compactions(Nodes) ->
    Ring = rt:get_ring(hd(Nodes)),
    Owners = riak_core_ring:all_owners(Ring),
    Req = {backend_callback, make_ref(), compact_journal},
    CompactFun =
        fun(Owner) ->
            riak_core_vnode_master:command(Owner,
                                            Req,
                                            riak_kv_vnode_master)
        end,
    lists:foreach(CompactFun, Owners).