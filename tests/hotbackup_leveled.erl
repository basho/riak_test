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
%% @doc Run a hot backup and a restore
%%
%% Confirm that if the backend is not leveled, then not_supported is
%% returned as expeceted

-module(hotbackup_leveled).
-export([confirm/0, hot_backup/1]).
-include_lib("eunit/include/eunit.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(RING, 32).
-define(NUM_NODES, 5).
-define(NUM_KEYS_PERNODE, 5000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(DELTA_COUNT, 10).
-define(VAL_FLAG1, "U1").
-define(VAL_FLAG2, "U2").
-define(HARNESS, (rt_config:get(rt_harness))).
-define(CFG, [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [
                                                 {n_val, ?N_VAL},
                                                 {allow_mult, true},
                                                 {dvv_enabled, true},
                                                 {ring_creation_size, ?RING}
                                                ]}]}]).


confirm() ->
    Nodes0 = rt:build_cluster(?NUM_NODES, ?CFG),
    ok = hot_backup(Nodes0),
    pass.


hot_backup(Nodes) ->
    
    KeyLoadFun = 
        fun(Node, KeyCount) ->
            KVs = test_data(KeyCount + 1,
                                KeyCount + ?NUM_KEYS_PERNODE,
                                list_to_binary(?VAL_FLAG1)),
            ok = write_data(Node, KVs),
            KeyCount + ?NUM_KEYS_PERNODE
        end,

    KeyCount= ?NUM_KEYS_PERNODE * length(Nodes),
    lists:foldl(KeyLoadFun, 1, Nodes),
    lager:info("Loaded ~w objects", [KeyCount]),

    check_objects(hd(Nodes), KeyCount, ?VAL_FLAG1),

    KVBackend = proplists:get_value(backend, riak_test_runner:metadata()),
    test_by_backend(KVBackend, Nodes).


test_by_backend(undefined, Nodes) ->
    test_by_backend(bitcask, Nodes);
test_by_backend(bitcask, Nodes) ->
    not_supported_test(Nodes);
test_by_backend(eleveldb, Nodes) ->
    not_supported_test(Nodes);
test_by_backend(CapableBackend, Nodes) ->
    KeyCount= ?NUM_KEYS_PERNODE * length(Nodes),
    lager:info("Testing capable backend ~w", [CapableBackend]),
    {ok, C} = riak:client_connect(hd(Nodes)),

    lager:info("Backup to self to fail"),
    {ok, true} = riak_client:hot_backup("./data/", ?N_VAL, ?N_VAL, C),

    lager:info("Backup all nodes to succeed"),
    {ok, true} = riak_client:hot_backup("./data/backup/", ?N_VAL, ?N_VAL, C),
    
    lager:info("Change some keys"),
    Changes2 = test_data(1, ?DELTA_COUNT, list_to_binary(?VAL_FLAG2)),
    ok = write_data(hd(Nodes), Changes2),
    check_objects(hd(Nodes), ?DELTA_COUNT, ?VAL_FLAG2),

    lager:info("Stop the primary cluster and start from backup"),
    lists:foreach(fun rt:stop_and_wait/1, Nodes),

    lager:info("Confirm changed objects are unchanged"),
    
    rt:clean_data_dir(Nodes, backend_dir()),
    rt:restore_datadir(Nodes, backend_dir(), "./data/backup"),
    
    lists:foreach(fun rt:start_and_wait/1, Nodes),
    check_objects(hd(Nodes), KeyCount, ?VAL_FLAG1),
    ok.



not_supported_test(Nodes) ->
    {ok, C} = riak:client_connect(hd(Nodes)),
    lager:info("Backup all nodes to fail"),
    {ok, false} = riak_client:hot_backup("./data/backup/", ?N_VAL, ?N_VAL, C).



to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].

write_data(Node, KVs) ->
    write_data(Node, KVs, []).

write_data(Node, KVs, Opts) ->
    PB = rt:pbc(Node),
    [begin
         O =
         case riakc_pb_socket:get(PB, ?BUCKET, K) of
             {ok, Prev} ->
                 riakc_obj:update_value(Prev, V);
             _ ->
                 riakc_obj:new(?BUCKET, K, V)
         end,
         ?assertMatch(ok, riakc_pb_socket:put(PB, O, Opts))
     end || {K, V} <- KVs],
    riakc_pb_socket:stop(PB),
    ok.

check_objects(_Node, _KC, _VFlag) ->
    true.

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