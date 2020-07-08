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
%% @doc Verification of Active Anti Entropy.


-module(verify_tictac_aae_transition).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 8).
-define(CFG(TictacAAE),
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {on, []}},
           {tictacaae_active, TictacAAE},
           {tictacaae_parallelstore, leveled_ko}
                % if backend not leveled will use parallel key-ordered
                % store
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).

-define(NUM_NODES, 4).
-define(NUM_KEYS, 400000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).

confirm() ->
    Nodes = rt:build_cluster(?NUM_NODES, ?CFG(passive)),

    lager:info("Writing ~w items of data", [?NUM_KEYS]),
    ok = write_data(hd(Nodes), test_data(1, ?NUM_KEYS), false),

    lager:info("Changing config to enable tictac_aae"),
    lists:foreach(fun(N) -> rt:set_advanced_conf(N, ?CFG(active)) end, Nodes),

    rt:wait_until_ring_converged(Nodes),

    lager:info("Waiting to see what happens"),
    timer:sleep(2400000),

    pass.


to_key(N) ->
    list_to_binary(io_lib:format("K~9..0B", [N])).

test_data(Start, End) ->
    lists:map(fun(N) -> {to_key(N), <<N:32/integer>>} end,
                lists:seq(Start, End)).

write_data(Node, KVs, MaybePresent) ->
    write_data(Node, KVs, MaybePresent, []).

write_data(Node, KVs, MaybePresent, Opts) ->
    PB = rt:pbc(Node),
    lists:foreach(fun({K, V}) ->
                        write_data(K, V, Opts, PB, MaybePresent)
                    end,
                    KVs).

write_data(K, V, Opts, PB, true) ->
    Obj = 
        case riakc_pb_socket:get(PB, ?BUCKET, K) of
            {ok, Prev} ->
                riakc_obj:update_value(Prev, V);
            _ ->
                riakc_obj:new(?BUCKET, K, V)
        end,
    ?assertMatch(ok, riakc_pb_socket:put(PB, Obj, Opts));
write_data(K, V, Opts, PB, false) ->
    Obj = riakc_obj:new(?BUCKET, K, V),
    ?assertMatch(ok, riakc_pb_socket:put(PB, Obj, Opts)).