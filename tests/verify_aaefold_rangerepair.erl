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
%% @doc Verification of AAE fold based on n_val (with cached trees)
%%
%% Confirm that read repair can be accelerated by an aae_fold

-module(verify_aaefold_rangerepair).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(CFG_NOREBUILD,
        [{riak_kv,
          [
           {anti_entropy, {off, []}},
           {tictacaae_active, active},
           {tictacaae_parallelstore, leveled_ko},
                % if backend not leveled will use parallel key-ordered
                % store
           {tictacaae_rebuildwait, 4},
           {tictacaae_rebuilddelay, 3600},
           {tictacaae_exchangetick, 3600000}, 
                % don't tick for an hour!
                % don't want to repair via AAE
           {tictacaae_rebuildtick, 3600000} % don't tick for an hour!
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(NUM_NODES, 4).
-define(NUM_KEYS, 20000).
-define(BUCKET, <<"test_bucket">>).
-define(N_VAL, 3).
-define(DELTA_COUNT, 10).

confirm() ->
    lager:info("Testing without rebuilds - using http api"),
    Nodes0 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD),
    CH_HTTP = rt:httpc(hd(Nodes0)),
    ok = verify_aae_repair(Nodes0, rhc, CH_HTTP, Nodes0),

    rt:clean_cluster(Nodes0),

    Nodes1 = rt:build_cluster(?NUM_NODES, ?CFG_NOREBUILD),
    CH_PB = rt:pbc(hd(Nodes1)),
    ok = verify_aae_repair(Nodes1, riakc_pb_socket, CH_PB, Nodes1),

    pass.


verify_aae_repair(Nodes, ClientMod, ClientHead, Nodes) ->

    TailNode = lists:last(Nodes),
    HeadNode = hd(Nodes),

    lager:info("Generating KVs (3 lots)"),

    TestKVs0 = test_data(1, ?NUM_KEYS, list_to_binary("U1")),
    TestKVs1 = test_data(?NUM_KEYS + 1, ?NUM_KEYS * 2, list_to_binary("U2")),
    TestKVs2 = test_data(?NUM_KEYS * 2 + 1, ?NUM_KEYS * 3, list_to_binary("U3")),

    TS0 = os:timestamp(),
    timer:sleep(1000),

    lager:info("Commencing object load"),
    ok = write_data(HeadNode, TestKVs0),
    lager:info("Loaded ~w objects", [length(TestKVs0)]),
    wait_until_root_stable(ClientMod, ClientHead),

    timer:sleep(1000),
    TS1 = os:timestamp(),
    timer:sleep(1000),

    lager:info("Second object load"),
    ok = write_data(HeadNode, TestKVs1),
    lager:info("Loaded ~w objects", [length(TestKVs1)]),
    wait_until_root_stable(ClientMod, ClientHead),

    timer:sleep(1000),
    TS2 = os:timestamp(),
    timer:sleep(1000),

    rt:stop_and_wait(TailNode),

    lager:info("Third object load - with node down"),
    ok = write_data(HeadNode, TestKVs2),
    lager:info("Loaded ~w objects", [length(TestKVs2)]),
    wait_until_root_stable(ClientMod, ClientHead),

    lager:info("Cleaning data directory on stopped node"),
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    rt:clean_data_dir([TailNode], base_dir_for_backend(KVBackend)),

    rt:start_and_wait(TailNode),

    rt:wait_until_transfers_complete(Nodes),
    
    object_count(ClientMod, ClientHead, "after restart - particpating"),
    object_count(ClientMod, ClientHead, "after restart - particpating"),
    object_count(ClientMod, ClientHead, "after restart - particpating"),
    object_count(ClientMod, ClientHead, "after restart - particpating"),
    object_count(ClientMod, ClientHead, "after restart - particpating"),

    rpc:call(TailNode, riak_client, remove_node_from_coverage, []),

    TC1 = object_count(ClientMod, ClientHead, "afetr restart - not particpating"),
    ?assertMatch(TC1, 3 * ?NUM_KEYS),

    RR0 = get_read_repair_total(Nodes),

    verify_tictac_aae:verify_data(HeadNode, TestKVs2),

    lager:info("Forcing read repair - first set"),
    {ok, NumKeys0} = 
        ClientMod:aae_range_repairkeys(ClientHead,
                                        ?BUCKET,
                                        all,
                                        convert_to_modified_range(TS0, TS1)),
    ?assertEqual(?NUM_KEYS, NumKeys0),

    verify_tictac_aae:verify_data(HeadNode, TestKVs0),
    lager:info("Count read repairs after first repair query"),
    get_read_repair_total(Nodes),

    lager:info("Forcing read repair - second set"),
    {ok, NumKeys1} = 
        ClientMod:aae_range_repairkeys(ClientHead,
                                        ?BUCKET,
                                        all,
                                        convert_to_modified_range(TS1, TS2)),
    ?assertEqual(?NUM_KEYS, NumKeys1),

    verify_tictac_aae:verify_data(HeadNode, TestKVs1),

    rpc:call(TailNode, riak_client, reset_node_for_coverage, []),
    rt:wait_until_ring_converged(Nodes),
    
    TC2 = object_count(ClientMod, ClientHead, "after repair - particpating"),
    ?assertMatch(TC2, 3 * ?NUM_KEYS),

    RR1 = get_read_repair_total(Nodes),
    lager:info("Read repairs ~w", [RR1 - RR0]),

    lager:info("Read repair total should be about ~w/~w of ~w",
                [?N_VAL, ?NUM_NODES, 2 * ?NUM_KEYS]),
    
    true = (RR1 - RR0) > ?NUM_KEYS,
    true = (RR1 - RR0) < 2 * ?NUM_KEYS,

    ok.



to_key(N) ->
    list_to_binary(io_lib:format("K~8..0B", [N])).

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
    timer:sleep(5000),
    ok.

wait_until_root_stable(Mod, Client) ->
    {ok, {root, RH0}} = Mod:aae_merge_root(Client, ?N_VAL),
    timer:sleep(2000),
    {ok, {root, RH1}} = Mod:aae_merge_root(Client, ?N_VAL),
    case aae_exchange:compare_roots(RH0, RH1) of
        [] ->
            lager:info("Root appears stable matched");
        [L] ->
            Pre = L * 4,
            <<_B0:Pre/binary, V0:32/integer, _Post0/binary>> = RH0,
            <<_B1:Pre/binary, V1:32/integer, _Post1/binary>> = RH1,
            lager:info("Root not stable: branch ~w compares ~w with ~w",
                        [L, V0, V1]),
            wait_until_root_stable(Mod, Client)
    end.

convert_to_modified_range({StartMega, StartS, _}, {EndMega, EndS, _}) ->
    {StartMega * 1000000 + StartS, EndMega * 1000000 + EndS}.

base_dir_for_backend(leveled) ->
    "leveled";
base_dir_for_backend(bitcask) ->
    "bitcask";
base_dir_for_backend(eleveldb) ->
    "leveldb".

object_count(ClientMod, ClientHead, LogInfo) ->
    timer:sleep(1000),
    {ok, {stats, StatList}} =
        ClientMod:aae_object_stats(ClientHead, ?BUCKET, all, all),
    lager:info("Stats ~p ~s", [StatList, LogInfo]),
    {<<"total_count">>, TC} = lists:keyfind(<<"total_count">>, 1, StatList),
    TC.


get_read_repair_total(Nodes) ->
    FoldFun =
        fun(Node, Acc) ->
            Stats = get_stats(Node),
            {<<"read_repairs_total">>, RR} =
                lists:keyfind(<<"read_repairs_total">>, 1, Stats),
            lager:info("Repairs on ~p ~w", [Node, RR]),
            Acc + RR
        end,
    RRT = lists:foldl(FoldFun, 0, Nodes),
    lager:info("Read repair total ~w", [RRT]),
    RRT.

get_stats(Node) ->
    lager:info("Retrieving stats from node ~s", [Node]),
    StatsCommand = io_lib:format("curl -s -S ~s/stats", [rt:http_url(Node)]),
    StatString = os:cmd(StatsCommand),
    {struct, Stats} = mochijson2:decode(StatString),
    Stats.