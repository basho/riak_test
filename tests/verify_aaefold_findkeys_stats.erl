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

-module(verify_aaefold_findkeys_stats).
-export([confirm/0, verify_aae_fold/1]).
-include_lib("eunit/include/eunit.hrl").

% I would hope this would come from the testing framework some day
% to use the test in small and large scenarios.
-define(DEFAULT_RING_SIZE, 8).
-define(CFG_NOREBUILD,
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
           {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE},
           {default_bucket_props, [{allow_mult, true}]}
          ]}]
       ).

-define(NUM_NODES, 3).
-define(NUM_KEYS_PERNODE, 10000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(DELTA_COUNT, 10).

confirm() ->
    Nodes = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD),
    ok = verify_aae_fold(Nodes),
    ok = verify_stats(Nodes),

    pass.


verify_aae_fold(Nodes) ->

    HttpCH = rt:httpc(hd(Nodes)),
    lager:info("Find Keys for no data "),

    {ok, {keys, SiblingCnts}} = rhc:aae_find_keys(HttpCH, ?BUCKET, all, all, {sibling_count, 1}),
    {ok, {keys, ObjSize}} = rhc:aae_find_keys(HttpCH, ?BUCKET, all, all, {object_size, 1}),

    ?assertEqual([], SiblingCnts),
    ?assertEqual([], ObjSize),

    lager:info("Commencing object load"),
    KeyLoadFun =
        fun(Node, KeyCount) ->
                KVs = test_data(KeyCount + 1,
                                KeyCount + ?NUM_KEYS_PERNODE,
                                list_to_binary("U1")),
                ok = write_data(Node, KVs),
                KeyCount + ?NUM_KEYS_PERNODE
        end,

    lists:foldl(KeyLoadFun, 0, Nodes),
    lager:info("Loaded ~w objects", [?NUM_KEYS_PERNODE * length(Nodes)]),

    lager:info("Add siblings"),
    ExpectedSibs = write_siblings(hd(Nodes)),

    lager:info("Find keys with siblings"),
    {ok, {keys, SiblingCnts2}} = rhc:aae_find_keys(HttpCH, ?BUCKET, all, all, {sibling_count, 1}),
    %% can't account for the fixed overhead, so I ran the test and
    %% peeked, everything without a sibling is 142-143 bytes, so I set
    %% the bar at 200 bytes
    {ok, {keys, ObjSize2}} = rhc:aae_find_keys(HttpCH, ?BUCKET, all, all, {object_size, 200}),

    ?assertEqual(ExpectedSibs, SiblingCnts2),
    %% verify that all the keys are there, and that all the objects
    %% are greater than 200 in size
    ExpectedKeys = [K || {K, _} <- ExpectedSibs],
    ?assertEqual(ExpectedKeys, [K || {K, _S} <- ObjSize2]),
    [?assertMatch(S when S > 200, S) || {_K, S} <- ObjSize2],

    lager:info("Find range of keys with siblings"),
    Range = {Lo, Hi} = {to_key(50), to_key(69)},
    ExpectedSibsRange = [{K, C} || {K, C} <- ExpectedSibs, K >= Lo, K =< Hi],

    {ok, {keys, SiblingCntsRange}} = rhc:aae_find_keys(HttpCH, ?BUCKET, Range , all, {sibling_count, 1}),
    ?assertEqual(ExpectedSibsRange, SiblingCntsRange),

    %% only keys from 95-100 should be returned as over 200 bytes
    {ok, {keys, ObjSizeRange}} = rhc:aae_find_keys(HttpCH, ?BUCKET, {to_key(95), to_key(105)}, all, {object_size, 200}),
    ExpectedKeysRange = [to_key(N) || N <- lists:seq(95, 100)],
    ?assertEqual(ExpectedKeysRange, [K || {K, _S} <- ObjSizeRange]),

    {ok, {keys, ObjSizeBig}} = rhc:aae_find_keys(HttpCH, ?BUCKET, all, all, {object_size, 500}),
    ExpectedKeysBig = [to_key(N) || N <- lists:seq(1, 20)],
    ?assertEqual(ExpectedKeysBig, [K || {K, _S} <- ObjSizeBig]),
    [?assertMatch(S when S > 500, S) || {_K, S} <- ObjSizeBig],

    ok.

verify_stats(Nodes) ->
    HttpCH = rt:httpc(hd(Nodes)),
    lager:info("get stats"),
    {ok, {stats, Stats}} = rhc:aae_object_stats(HttpCH, ?BUCKET, all, all),
    %% Erm, what do we know? They should have keys
    ?assertEqual(10000, proplists:get_value(<<"total_count">>, Stats)),
    %% at least 100 bytes per key
    ?assertMatch(N when is_integer(N) andalso N > (10000 * 100), proplists:get_value(<<"total_size">>, Stats)),
    ?assertMatch(L when is_list(L), proplists:get_value(<<"sizes">>, Stats)),
    ?assertMatch(L when is_list(L), proplists:get_value(<<"siblings">>, Stats)),
    ok.




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

write_siblings(Node) ->
    %% 1st 100 objects generate a sibling
    %% 1st 90 object generate 2 siblings
    %% 1st 80 objects generate 3 siblings
    %% etc etc
    PB = rt:pbc(Node),
    InitialAcc = lists:foldl(fun(K, Acc) ->
                                     orddict:update_counter(K, 1, Acc)
                             end,
                             orddict:new(),
                             [to_key(N) || N <- lists:seq(1, 100)]),
    Bytes = crypto:rand_bytes(10),
    ExpectedSibs = write_siblings(100, PB, Bytes, InitialAcc),
    riakc_pb_socket:stop(PB),
    ExpectedSibs.

write_siblings(N, _Client, _Bytes, Acc) when N < 1 ->
    Acc;
write_siblings(N, Client, Bytes, Acc) ->
    %% I think riak's merge logic will collapse siblings of the same
    %% value into a single value, so make each sibling have a
    %% different value. I also _think_ that the object size is all the
    %% is siblings, so object will by sibs * size(bytes)
    KVs = test_data(1, N, <<"sibling", N:32/integer, Bytes/binary>>),
    Acc2 = lists:foldl(fun({K, V}, InnerAcc) ->
                               O = riakc_obj:new(?BUCKET, K, V),
                               %% By doing a "blind put" a sibling is generated
                               ?assertMatch(ok, riakc_pb_socket:put(Client, O, [])),
                               orddict:update_counter(K, 1, InnerAcc)
                       end,
                       Acc,
                       KVs),
    %% Write MOAR siblings for a shrinking subset of keys
    write_siblings(N-10, Client, Bytes, Acc2).

