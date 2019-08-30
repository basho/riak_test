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
%% @doc Verification of Active Anti Entropy token resets.


-module(verify_aae_resettoken).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(AAE_THROTTLE_LIMITS, [{-1, 0}, {100, 10}]).
-define(CFG,
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {on, []}},
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 100},
           {anti_entropy_expire, 24 * 60 * 60 * 1000}, % Not for now!
           {anti_entropy_tick, 500},
           {aae_throttle_limits, ?AAE_THROTTLE_LIMITS},
           {anti_entropy_max_async, ?NUM_KEYS}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(NUM_NODES, 3).
-define(NUM_KEYS, 1000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).

confirm() ->
    Nodes = rt:build_cluster(?NUM_NODES, ?CFG),
    verify_token_reset(Nodes),
    pass.

verify_token_reset(Cluster) ->
    {infinity, undefined} = 
        rpc:call(hd(Cluster), riak_kv_util, report_hashtree_tokens, []),
    {infinity, undefined} = 
        rpc:call(hd(Cluster), riak_kv_util, report_hashtree_tokens, []),
    
    KVL0 = test_data(1, ?NUM_KEYS),
    verify_aae:test_less_than_n_writes(hd(Cluster), KVL0),

    {Min0, Max0} =
        rpc:call(hd(Cluster), riak_kv_util, report_hashtree_tokens, []),
    lager:info("Initial Min and Max ~w ~w", [Min0, Max0]),
    ?assertMatch(true, Max0 >= Min0),
    ?assertMatch(true, Max0 =< ?NUM_KEYS),
    ?assertMatch(true, Min0 >= 0),

    ok = rpc:call(hd(Cluster), riak_kv_util, reset_hashtree_tokens, [1000, 2000]),

    {Min1, Max1} =
        rpc:call(hd(Cluster), riak_kv_util, report_hashtree_tokens, []),
    lager:info("Reset Min and Max ~w ~w", [Min1, Max1]),
    ?assertMatch(true, Max1 >= Min1),
    ?assertMatch(true, Max1 =< 2000),
    ?assertMatch(true, Min1 >= 1000),

    KVL1 = test_data(?NUM_KEYS + 1, ?NUM_KEYS + ?NUM_KEYS),
    verify_aae:test_less_than_n_writes(hd(Cluster), KVL1),

    {Min2, Max2} =
        rpc:call(hd(Cluster), riak_kv_util, report_hashtree_tokens, []),
    lager:info("Reset Min and Max ~w ~w after further writes", [Min1, Max1]),
    ?assertMatch(true, Max2 >= Min2),
    ?assertMatch(true, Max2 < Max1),
    ?assertMatch(true, Min2 < Min1),

    ok = rpc:call(hd(Cluster), riak_kv_util, reset_hashtree_tokens, [Min0, Max0]),
    {Min3, Max3} =
        rpc:call(hd(Cluster), riak_kv_util, report_hashtree_tokens, []),
    lager:info("Re-reset Min and Max ~w ~w", [Min3, Max3]),
    ?assertMatch(true, Max3 >= Min3),
    ?assertMatch(true, Max3 =< Max0),
    ?assertMatch(true, Min3 >= Min0),

    ok = rpc:call(hd(Cluster), riak_kv_util, reset_hashtree_tokens, [1234, 1234]),
    {1234, 1234} =
        rpc:call(hd(Cluster), riak_kv_util, report_hashtree_tokens, []),

    pass.



to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

test_data(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].
