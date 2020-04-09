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
%% @doc Verification of AAE fold's find_keys and object stats
%% operational fold features

-module(verify_aaefold_listbuckets).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 8).
-define(CFG_NOREBUILD(ParallelStore),
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, ParallelStore},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 3600},
           {tictacaae_exchangetick, 5 * 1000}, % 5 seconds
           {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE},
           {default_bucket_props, [{allow_mult, true}]}
          ]}]
       ).

-define(NUM_NODES, 3).
-define(NUM_KEYS, 10000).
-define(N_VAL, 3).
-define(DELTA_COUNT, 10).

confirm() ->

    lager:info("Testing AAE bucket list - key-ordered parallel store"),
    Cko = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(leveled_ko)),
    ok = verify_list_buckets(Cko, ?NUM_KEYS),

    rt:clean_cluster(Cko),

    lager:info("Testing AAE bucket list - segment-ordered parallel store"),
    lager:info("This is a repeat test if backend is leveled"),
    Cso = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(leveled_so)),
    lager:info("Testing with less keys as segment-ordered"),
    ok = verify_list_buckets(Cso, ?NUM_KEYS div 10),


    pass.

verify_list_buckets(Nodes, KeyCount) ->
    [Node|_Rest] = Nodes,
    {ModP, ClientP} = {riakc_pb_socket, rt:pbc(Node)},
    {ModH, ClientH} = {rhc, rt:httpc(Node)},
    rt:create_and_activate_bucket_type(Node,
                                       <<"nval4">>,
                                       [{n_val, 4},
                                            {allow_mult, false}]),
    rt:create_and_activate_bucket_type(Node,
                                       <<"nval2">>,
                                       [{n_val, 2},
                                            {allow_mult, false}]),

    KVL1 = test_data(1, KeyCount, <<"TestDataSet1">>),
    KVL2 = test_data(1, KeyCount, <<"TestDataSet2">>),
    KVL3 = test_data(1, KeyCount, <<"TestDataSet3">>),
    KVL4 = test_data(1, KeyCount, <<"TestDataSet4">>),
    KVL5 = test_data(1, KeyCount, <<"TestDataSet5">>),

    ok = write_data(<<"Bucket1">>, Node, KVL1, []),
    ok = write_data(<<"Bucket2">>, Node, KVL2, []),
    ok = write_data(<<"Bucket3">>, Node, KVL3, []),
    ok = write_data({<<"nval4">>, <<"Bucket1">>}, Node, KVL4, []),
    ok = write_data({<<"nval2">>, <<"Bucket2">>}, Node, KVL5, []),

    {TS0, {ok, BL}} = timer:tc(ModP, aae_list_buckets, [ClientP]),
    ?assertMatch(5, length(BL)),
    ?assertMatch(BL, lists:usort(BL)),

    % Becase of distribution of keys - should get same answer even if min nval
    % is set to ring size
    {TS1, {ok, BL}} =
        timer:tc(ModP, aae_list_buckets, [ClientP, ?DEFAULT_RING_SIZE]),

    lager:info("List bucket queries took ~w ms (nv=1) and ~w ms (nv=8)",
                [TS0/1000, TS1/1000]),

    lager:info("Testing with HTTP client"),
    {ok, BL} = ModH:aae_list_buckets(ClientH),
    {ok, BL} = ModH:aae_list_buckets(ClientH, ?DEFAULT_RING_SIZE),

    ok.


to_key(N) ->
    list_to_binary(io_lib:format("K~6..0B", [N])).

test_data(Start, End, V) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, <<K/binary, V/binary>>} || K <- Keys].

write_data(Bucket, Node, KVs, Opts) ->
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
