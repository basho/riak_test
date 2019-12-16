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
%% @doc Experiment with binary metadata

-module(verify_binary_metadata).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

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
           {default_bucket_props, [{allow_mult, false}]}
          ]}]
       ).

-define(NUM_NODES, 3).
-define(NUM_KEYS, 10000).
-define(N_VAL, 3).
-define(DELTA_COUNT, 10).

confirm() ->

    lager:info("Testing AAE bucket list - key-ordered parallel store"),
    Cko = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD(leveled_ko)),
    ok = verify_binary_metadata(Cko),

    pass.

verify_binary_metadata(Nodes) ->
    [Node|_Rest] = Nodes,
    PB = rt:pbc(Node),
    RHC = rt:httpc(Node),
    Bucket = <<"test_bucket">>,
    K = to_key(1), 
    V = crypto:strong_rand_bytes(32),

    O = riakc_obj:new(Bucket, K, V),
    M = riakc_obj:get_metadata(O),
    ME1 = {<<"x-riak-meta-randbin">>, crypto:strong_rand_bytes(4)},
    ME2 = {<<"x-riak-meta-randint">>, integer_to_binary(erlang:phash2(ME1))},
    ME3 = {<<"x-riak-meta-randstr">>, <<"some text to test">>},
    M0 = lists:foldl(fun(MDE, MD) ->
                            riakc_obj:set_user_metadata_entry(MD, MDE)
                        end,
                        M,
                        [ME1, ME2, ME3]),

    O0 = riakc_obj:update_metadata(O, M0),

    lager:info("Added by PB"),
    ?assertMatch(ok, riakc_pb_socket:put(PB, O0, [])),
    {ok, ReturnedPB0} = riakc_pb_socket:get(PB, Bucket, K),
    {ok, ReturnedHTTP0} = rhc:get(RHC, Bucket, K),

    lager:info("User metadata:"),
    lager:info("PB ~w",
                [riakc_obj:get_user_metadata_entries(
                    riakc_obj:get_metadata(ReturnedPB0))]),
    lager:info("HTTP ~w",
                [riakc_obj:get_user_metadata_entries(
                    riakc_obj:get_metadata(ReturnedHTTP0))]),

    lager:info("Added by HTTP"),
    ?assertMatch(ok, rhc:put(RHC, O0, [])),
    {ok, ReturnedPB1} = riakc_pb_socket:get(PB, Bucket, K),
    {ok, ReturnedHTTP1} = rhc:get(RHC, Bucket, K),

    lager:info("User metadata:"),
    lager:info("PB ~w",
                [riakc_obj:get_user_metadata_entries(
                    riakc_obj:get_metadata(ReturnedPB1))]),
    lager:info("HTTP ~w",
                [riakc_obj:get_user_metadata_entries(
                    riakc_obj:get_metadata(ReturnedHTTP1))]),

    ok.


to_key(N) ->
    list_to_binary(io_lib:format("K~6..0B", [N])).
