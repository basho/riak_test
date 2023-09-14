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
%% @doc Verification of Active Anti Entropy during node down - with or without
%% configuration to run AAE only between primary vnodes.


-module(verify_tictacaae_nodedown).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 8).
-define(AAE_THROTTLE_LIMITS, [{-1, 0}, {100, 10}]).
-define(CFG_NOREBUILD(PrimaryOnly),
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
           {tictacaae_exchangetick, 5 * 1000}, % 5 seconds
           {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
           {tictacaae_primaryonly, PrimaryOnly},
           {tictacaae_stepinitialtick, false}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(NUM_NODES, 4).
-define(NUM_KEYS, 5000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(RETRY_LOOPS, 15).
-define(RETRY_PAUSE, 2000).

confirm() ->
    C0 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(false)),
    verify_aae(C0, true),

    ok = rt:clean_cluster(C0),

    C1 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(true)),
    verify_aae(C1, false),
    pass.


verify_aae(Nodes, ExpectSuccess) ->
    Node1 = hd(Nodes),

    % Test recovery from to few replicas written
    KV1 = test_data(1, ?NUM_KEYS),
    write_data(Node1, KV1, [{n_val, 3}]),
    verify_data(Node1, KV1, all_correct_data),

    FailNode1 = lists:nth(2, Nodes),
    rt:stop_and_wait(FailNode1),

    case ExpectSuccess of
        true ->
            verify_data(Node1, KV1, aae_fixed_data);
        false ->
            verify_data(Node1, KV1, aae_failed_to_fix_data)
    end,
    
    rt:start_and_wait(FailNode1),
    verify_data(Node1, KV1, all_correct_data).

to_key(N) ->
    list_to_binary(io_lib:format("K~6..0B", [N])).

test_data(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].

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

% @doc Verifies that the data is eventually restored to the expected set.
verify_data(Node, KeyValues, Expectation) ->
    lager:info("Verify all replicas are eventually correct"),
    PB = rt:pbc(Node),
    CheckFun =
    fun() ->
            Matches = [verify_replicas(Node, ?BUCKET, K, V, ?N_VAL)
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
    
    case rt:wait_until(CheckFun, ?RETRY_LOOPS, ?RETRY_PAUSE) of
        ok ->
            lager:info("Data is now correct. Yay!");
        {fail, false} ->
            lager:error("AAE failed to fix data"),
            ?assertEqual(aae_failed_to_fix_data, Expectation)
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
    Replies = [get_replica(Node, B, K, I, N) || I <- lists:seq(1,N)],
    Vals = [merge_values(O) || {ok, O} <- Replies],
    Expected = [V || _ <- lists:seq(1, N)],
    Vals == Expected.
    
% @doc Reads a single replica of a value. This issues a get command directly
% to the vnode handling the Nth primary partition of the object's preflist.
get_replica(Node, Bucket, Key, I, N) ->
    BKey = {Bucket, Key},
    Chash = rpc:call(Node, riak_core_util, chash_key, [BKey]),
    Pl = rpc:call(Node, riak_core_apl, get_apl, [Chash, N, riak_kv]),
    {Partition, PNode} = lists:nth(I, Pl),
    Ref = Reqid = make_ref(),
    Sender = {raw, Ref, self()},
    rpc:call(PNode, riak_kv_vnode, get,
             [{Partition, PNode}, BKey, Ref, Sender]),
    receive
        {Ref, {r, Result, _, Reqid}} ->
            Result;
        {Ref, Reply} ->
            Reply
    after
        60000 ->
            lager:error("Replica ~p get for ~p/~p timed out",
                        [I, Bucket, Key]),
            ?assert(false)
    end.