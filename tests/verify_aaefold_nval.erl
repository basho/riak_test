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
%% @doc Verification of AAE fold base don n_val (with cached trees)
%%
%% Confirm that trees are returned that vary along with the data in the
%% store

-module(verify_aaefold_nval).
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
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).

-define(NUM_NODES, 3).
-define(NUM_KEYS_PERNODE, 10000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).

confirm() ->
    Nodes0 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD),
    ok = verify_aae_fold(Nodes0),
    pass.


verify_aae_fold(Nodes) ->
    
    {ok, CH} = riak:client_connect(hd(Nodes)),
    {ok, CT} = riak:client_connect(lists:last(Nodes)),

    {ok, RH0} = riak_clinet:aae_fold([{merge_root_nval, ?N_VAL}], CH),
    {ok, RT0} = riak_clinet:aae_fold([{merge_root_nval, ?N_VAL}], CT),

    KeyLoadFun = 
        fun(Node, KeyCount) ->
            KV = test_data(KeyCount + 1, KeyCount + ?NUM_KEYS_PERNODE),
            ok = write_data(KV, Node),
            KeyCount + ?NUM_KEYS_PERNODE
        end,

    lists:foldl(KeyLoadFun, 1, Nodes),

    {ok, RH1} = riak_clinet:aae_fold([{merge_root_nval, ?N_VAL}], CH),
    {ok, RT1} = riak_clinet:aae_fold([{merge_root_nval, ?N_VAL}], CT),
    
    ?assertMatch(true, RH1 == RT1),
    ?assertMatch(true, RH0 == RT0),
    ?assertMatch(false, RH0 == RH1).


to_key(N) ->
    list_to_binary(io_lib:format("K~4..0B", [N])).

test_data(Start, End) ->
    Keys = [to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].

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

