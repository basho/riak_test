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
-module(verify_basic_upgrade).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(NUM_KEYS, 60000).

-define(CONFIG_CORE(RingSize, NVal),
    {riak_core,
        [
        {ring_creation_size, RingSize},
        {default_bucket_props,
            [
                {n_val, NVal},
                {allow_mult, true},
                {dvv_enabled, true}
            ]}
        ]
    }).


-define(CONFIG_PLAINTEXT(CoreConfig),
[
    CoreConfig,
        {leveled,
            [
                {journal_objectcount, 2000},
                {compression_point, on_compact}
            ]},
        {eleveldb,
            [
                {compression, false}
            ]
        }
        ]).

-define(CONFIG_NATIVE(CoreConfig), 
    [
        CoreConfig,
        {leveled,
            [
                {journal_objectcount, 2000},
                {compression_point, on_receipt},
                {compression_method, native}
            ]},
        {eleveldb,
            [
                {compression, snappy}
            ]
        }
        ]).

-define(CONFIG_LZ4(CoreConfig), 
    [
        CoreConfig,
        {leveled,
            [
                {journal_objectcount, 2000},
                {compression_point, on_receipt},
                {compression_method, lz4}
            ]},
        {eleveldb,
            [
                {compression, lz4}
            ]
        }
    ]).

confirm() ->
    
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    lager:info("*****************************"),
    lager:info("Testing without compression"),
    lager:info("*****************************"),

    [NodesPlainText] =
        rt:build_clusters([{4, OldVsn, ?CONFIG_PLAINTEXT(?CONFIG_CORE(8,3))}]),
    pass = verify_basic_upgrade(NodesPlainText, OldVsn),

    case KVBackend of
        bitcask ->
            pass;
        _ ->
            rt:clean_cluster(NodesPlainText),

            lager:info("*****************************"),
            lager:info("Testing with native compression"),
            lager:info("*****************************"),
            [NodesNative] =
                rt:build_clusters(
                    [{4, OldVsn, ?CONFIG_NATIVE(?CONFIG_CORE(8,3))}]),
            pass = verify_basic_upgrade(NodesNative, OldVsn),
            
            rt:clean_cluster(NodesNative),

            lager:info("*****************************"),
            lager:info("Testing with lz4 compression"),
            lager:info("*****************************"),
            [NodesLZ4] =
                rt:build_clusters(
                    [{4, OldVsn, ?CONFIG_LZ4(?CONFIG_CORE(8, 3))}]),
            verify_basic_upgrade(NodesLZ4, OldVsn)
    end.
    
verify_basic_upgrade(Nodes, OldVsn) ->
    [Node1|_] = Nodes,
    V = compressable_value(),

    lager:info("Writing ~w keys to ~p", [?NUM_KEYS, Node1]),
    rt:systest_write(Node1, 1, ?NUM_KEYS, <<"B1">>, 2, V),
    validate_value(Node1, <<"B1">>, 1, ?NUM_KEYS, V, 0.1),

    [upgrade(Node, current) || Node <- Nodes],
    
    validate_value(Node1, <<"B1">>, 1, ?NUM_KEYS, V, 1.0),
    
    lager:info("Writing ~w keys to ~p", [?NUM_KEYS div 4, Node1]),
    rt:systest_write(Node1, 1, ?NUM_KEYS div 4, <<"B2">>, 2, V),

    %% Umm.. technically, it'd downgrade
    [upgrade(Node, OldVsn) || Node <- Nodes],

    validate_value(Node1, <<"B1">>, 1, ?NUM_KEYS, V, 0.1),
    validate_value(Node1, <<"B2">>, 1, ?NUM_KEYS div 4, V, 1.0),

    lager:info("Backend size ~s", [backend_size(Node1)]),
    pass.

upgrade(Node, NewVsn) ->
    lager:info("Upgrading ~p to ~p", [Node, NewVsn]),
    rt:upgrade(Node, NewVsn),
    rt:wait_for_service(Node, riak_kv),
    ok.

backend_size(Node) ->
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    {ok, DataDir} =
        rpc:call(Node, application, get_env, [riak_core, platform_data_dir]),
    BackendDir = filename:join(DataDir, base_dir_for_backend(KVBackend)),
    rpc:call(Node, os, cmd, ["du -sh " ++ BackendDir]).

base_dir_for_backend(leveled) ->
    "leveled";
base_dir_for_backend(bitcask) ->
    "bitcask";
base_dir_for_backend(eleveldb) ->
    "leveldb".

compressable_value() ->
    T = "Value which repeats value and then says value again",
    list_to_binary(
        lists:flatten(lists:map(fun(_I) -> T end, lists:seq(1, 10)))
    ).

validate_value(Node, Bucket, StartKey, EndKey, Value, CheckPerc) ->
    KeyCount = (1 + EndKey - StartKey),
    lager:info(
        "Verifying ~w keys of ~w from Bucket ~p Node ~p",
        [floor(CheckPerc * KeyCount), KeyCount, Bucket, Node]),
    {ok, C} = riak:client_connect(Node),
    lists:foreach(
        fun(N) ->
            case rand:uniform() of
                R when R < CheckPerc ->
                    {ok, O} = riak_client:get(Bucket, <<N:32/integer>>, C),
                    ?assert(value_matches(riak_object:get_value(O), N, Value));
                _ ->
                    skip
            end
        end,
        lists:seq(StartKey, EndKey)
    ).

value_matches(<<N:32/integer, CommonValBin/binary>>, N, CommonValBin) ->
    true;
value_matches(_WrongVal, _N, _CommonValBin) ->
    false.