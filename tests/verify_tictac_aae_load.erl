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
%% @doc Verification of Active Anti Entropy performance.

-module(verify_tictac_aae_load).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 8).

% Amend defaults for 
-define(EXCHANGE_TICK, 30).
-define(MAX_RESULTS, 128).
-define(REPAIR_LOOPS, 4).
-define(KEY_RANGE, false).
-define(RANGE_BOOST, 2).

-define(TEST_VERSION, current).

-define(CFG,
        [{riak_kv,
          [
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_exchangetick, ?EXCHANGE_TICK * 1000},
           {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
           {tictacaae_maxresults, ?MAX_RESULTS},
           {tictacaae_repairloops, ?REPAIR_LOOPS},
           {tictacaae_enablekeyrange, ?KEY_RANGE},
           {tictacaae_rangeboost, ?RANGE_BOOST}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).

-define(NUM_NODES, 4).
-define(PRELOAD_KEYS_PERBUCKET, 150000).
-define(N1_KEYS_PERBUCKET, 2500).
-define(N2_KEYS_SINGLEBUCKET, 15000).

-define(ALT_BUCKET1, <<"alt_bucket1">>).
-define(ALT_BUCKET2, <<"alt_bucket2">>).
-define(ALT_BUCKET3, <<"alt_bucket3">>).
-define(ALT_BUCKET4, <<"alt_bucket4">>).

-define(N_VAL, 3).
-define(STATS_DELAY, 1000).
-define(VERIFY_DELAY, 10000).
-define(MICRO, 1000000).

confirm() ->

    lager:info("Not to be considered as a functional test"),
    lager:info("Useful only for comparing repair performance"),
    lager:info("e.g. between current and previous"),

    [Nodes] = rt:build_clusters([{?NUM_NODES, ?TEST_VERSION, ?CFG}]),
    ok = verify_aae_defaults(Nodes),
    
    pass.


verify_aae_defaults(Nodes) ->
    lager:info("Tictac AAE tests for large load to time recovery"),
    Node1 = hd(Nodes),

    % Recovery without tree rebuilds

    % Test recovery from too few replicas written
    lager:info("Generating ~w Keys/Values", [?PRELOAD_KEYS_PERBUCKET div 2]),
    KV1 = test_data(1, ?PRELOAD_KEYS_PERBUCKET div 2),
    write_data(Node1, KV1, [{n_val, 3}], ?ALT_BUCKET1),
    write_data(Node1, KV1, [{n_val, 3}], ?ALT_BUCKET2),
    write_data(Node1, KV1, [{n_val, 3}], ?ALT_BUCKET3),
    write_data(Node1, KV1, [{n_val, 3}], ?ALT_BUCKET4),

    lager:info("Generating ~w Keys/Values", [?PRELOAD_KEYS_PERBUCKET div 2]),
    KV2 =
        test_data(1 + ?PRELOAD_KEYS_PERBUCKET div 2, ?PRELOAD_KEYS_PERBUCKET),
    write_data(Node1, KV2, [{n_val, 3}], ?ALT_BUCKET1),
    write_data(Node1, KV2, [{n_val, 3}], ?ALT_BUCKET2),
    write_data(Node1, KV2, [{n_val, 3}], ?ALT_BUCKET3),
    write_data(Node1, KV2, [{n_val, 3}], ?ALT_BUCKET4),

    SW0 = os:timestamp(),
    lager:info("Start to introduce discrepancy"),

    KV3 = test_data(?PRELOAD_KEYS_PERBUCKET + 1,
                    ?PRELOAD_KEYS_PERBUCKET + ?N1_KEYS_PERBUCKET),
    write_data(Node1, KV3, [{n_val, 1}], ?ALT_BUCKET1),
    write_data(Node1, KV3, [{n_val, 1}], ?ALT_BUCKET2),
    write_data(Node1, KV3, [{n_val, 1}], ?ALT_BUCKET3),
    write_data(Node1, KV3, [{n_val, 1}], ?ALT_BUCKET4),
    KV4 = test_data(?PRELOAD_KEYS_PERBUCKET + ?N1_KEYS_PERBUCKET + 1,
                    ?PRELOAD_KEYS_PERBUCKET + ?N1_KEYS_PERBUCKET + ?N2_KEYS_SINGLEBUCKET),
    write_data(Node1, KV4, [{n_val, 2}], ?ALT_BUCKET1),

    SW1 = os:timestamp(),
    lager:info("Discrepancies written in ~w s", [timer:now_diff(SW1, SW0) div ?MICRO]),
    lager:info("Writes completed - attempting verify"),

    verify_data(Node1, KV3 ++ KV4, ?ALT_BUCKET1),
    verify_data(Node1, KV3, ?ALT_BUCKET2),
    verify_data(Node1, KV3, ?ALT_BUCKET3),
    verify_data(Node1, KV3, ?ALT_BUCKET3),

    SW2 = os:timestamp(),
    lager:info("Verification complete in ~w s", [timer:now_diff(SW2, SW1) div ?MICRO]),
    lager:info("Overall time ~w s", [timer:now_diff(SW2, SW0) div ?MICRO]),

    ok.


to_key(N) ->
    list_to_binary(io_lib:format("K~9..0B", [N])).

test_data(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].


write_data(Node, KVs, Opts, Bucket) ->
    lager:info("Loading batch of ~w keys to ~s bucket", [length(KVs), Bucket]),
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


verify_data(Node, KeyValues, Bucket) ->
    lager:info("Verify all replicas are eventually correct"),
    PB = rt:pbc(Node),
    CheckFun =
        fun() ->
                Matches = [verify_replicas(Node, Bucket, K, V, ?N_VAL)
                        || {K, V} <- KeyValues],
                CountTrues = fun(true, G) -> G+1; (false, G) -> G end,
                NumGood = lists:foldl(CountTrues, 0, Matches),
                Num = length(KeyValues),
                case Num == NumGood of
                    true -> true;
                    false ->
                        lager:info("Data not yet correct: ~p mismatches",
                                [Num-NumGood]),
                        false
                end
        end,
    MaxTime = rt_config:get(rt_max_wait_time),
    Delay = ?VERIFY_DELAY, % every two seconds until max time.
    Retry = MaxTime div Delay,
    ok = 
        case rt:wait_until(CheckFun, Retry, Delay) of
            ok ->
                lager:info("Data is now correct. Yay!");
            fail ->
                lager:error("AAE failed to fix data"),
                aae_failed_to_fix_data
        end,
    riakc_pb_socket:stop(PB),
    ok.

merge_values(O) ->
    Vals = riak_object:get_values(O),
    lists:foldl(fun(NV, V) ->
                        case size(NV) > size(V) of
                            true -> NV;
                            _ -> V
                        end
                end, <<>>, Vals).

verify_replicas(Node, B, K, V, N) ->
    Replies = [rt:get_replica(Node, B, K, I, N)
               || I <- lists:seq(1,N)],
    Vals = [merge_values(O) || {ok, O} <- Replies],
    Expected = [V || _ <- lists:seq(1, N)],
    Vals == Expected.



