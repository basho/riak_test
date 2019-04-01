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
-module(loaded_upgrade).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-export([kv_valgen/1, bucket/1, erlang_mr/0, int_to_key/1]).

-define(TIME_BETWEEN_UPGRADES, 120). %% Seconds!

confirm() ->

    case whereis(loaded_upgrade) of
        undefined -> meh;
        _ -> unregister(loaded_upgrade)
    end,
    register(loaded_upgrade, self()),
    %% Build Cluster
    TestMetaData = riak_test_runner:metadata(),
    %% Only run 2i for level
    Backend = proplists:get_value(backend, TestMetaData),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    Config = [{riak_pipe, [{worker_limit, 200}]}],
    NumNodes = 4,
    Vsns = [{OldVsn, Config} || _ <- lists:seq(1,NumNodes)],
    Nodes = rt:build_cluster(Vsns),

    seed_cluster(Nodes),

    %% Now we have a cluster!
    %% Let's spawn workers against it.
    Concurrent = rt_config:get(load_workers, 10),

    Sups = [{rt_worker_sup:start_link([{concurrent, Concurrent},
                                       {node, Node},
                                       {backend, Backend},
                                       {version, OldVsn},
                                       {report_pid, self()}]), Node} || Node <- Nodes],

    upgrade_recv_loop(),

    [begin
         exit(Sup, normal),
         lager:info("Upgrading ~p", [Node]),
         rt:upgrade(Node, current),
         rt:wait_for_service(Node, [riak_kv, riak_pipe]),
         {ok, NewSup} = rt_worker_sup:start_link([{concurrent, Concurrent},
                                                  {node, Node},
                                                  {backend, Backend},
                                                  {version, current},
                                                  {report_pid, self()}]),
         _NodeMon = init_node_monitor(Node, NewSup, self()),
         upgrade_recv_loop()
     end || {{ok, Sup}, Node} <- Sups],
    pass.

upgrade_recv_loop() ->
    {SMega, SSec, SMicro} = os:timestamp(),
    EndSecs = SSec + ?TIME_BETWEEN_UPGRADES,
    EndTime = case EndSecs > 1000000 of
                  true ->
                      {SMega + 1, EndSecs - 1000000, SMicro};
                  _ ->
                      {SMega, EndSecs, SMicro}
              end,
    upgrade_recv_loop(EndTime).

%% TODO: Collect error message counts in ets table
upgrade_recv_loop(EndTime) ->
    Now = os:timestamp(),
    case Now > EndTime of
        true ->
            lager:info("Done waiting 'cause ~p > ~p", [Now, EndTime]);
        _ ->
            receive
                {mapred, Node, bad_result} ->
                    ?assert(false, {mapred, Node, bad_result});
                {kv, Node, not_equal} ->
                    ?assert(false, {kv, Node, bad_result});
                {kv, Node, {notfound, Key}} ->
                    ?assert(false, {kv, Node, {notfound, Key}});
                {listkeys, Node, not_equal} ->
                    ?assert(false, {listkeys, Node, not_equal});
                Msg ->
                    lager:debug("Received Mesg ~p", [Msg]),
                    upgrade_recv_loop(EndTime)
            after timer:now_diff(EndTime, Now) div 1000 ->
                    lager:info("Done waiting 'cause ~p is up", [?TIME_BETWEEN_UPGRADES])
            end
    end.

seed_cluster(_Nodes=[Node1|_]) ->
    lager:info("Seeding Cluster"),

    %% For List Keys
    lager:info("Writing 100 keys to ~p", [Node1]),
    rt:systest_write(Node1, 100, 3),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),

    seed(Node1, 0, 100, fun(Key) ->
                                Bin = iolist_to_binary(io_lib:format("~p", [Key])),
                                riakc_obj:new(<<"objects">>, Bin, Bin)
                        end),

    %% For KV
    kv_seed(Node1),

    %% for 2i
    twoi_seed(Node1),

    %% for mapred
    mr_seed(Node1).

%% Buckets
bucket(kv) -> <<"utest">>;
bucket(twoi) -> <<"2ibuquot">>;
bucket(mapred) -> <<"bryanitbs">>.

kv_seed(Node) ->
    ValFun = fun(Key) ->
                     riakc_obj:new(bucket(kv), iolist_to_binary(io_lib:format("~p", [Key])), kv_valgen(Key))
             end,
    seed(Node, 0, 7999, ValFun).

kv_valgen(Key) ->
    term_to_binary(lists:seq(0, Key)).

int_to_key(KInt) ->
    list_to_binary(["", integer_to_list(KInt)]).

%% Every 2i seeded object will have indexes
%% int_plusone -> [Key + 1, Key + 10000]
%% bin_plustwo -> [<<"Key + 2">>]
twoi_seed(Node) ->
    ValFun = fun(Key) ->
                     Obj = riakc_obj:new(bucket(twoi), iolist_to_binary(io_lib:format("~p", [Key])), kv_valgen(Key)),
                     MD1 = riakc_obj:get_update_metadata(Obj),
                     MD2 = riakc_obj:set_secondary_index(MD1, [
                                                               {{integer_index, "plusone"}, [Key + 1, Key + 10000]},
                                                               {{binary_index, "plustwo"}, [int_to_key(Key + 2)]}
                                                              ]),
                     riakc_obj:update_metadata(Obj, MD2)
             end,
    seed(Node, 0, 7999, ValFun).

erlang_mr() ->
    [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, false},
     {reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, true}].

mr_seed(Node) ->
    %% to be used along with sequential_int keygen to populate known
    %% mapreduce set
    ValFun = fun(Key) ->
                     Value = iolist_to_binary(io_lib:format("~p", [Key])),
                     riakc_obj:new(bucket(mapred), Value, Value)
             end,
    seed(Node, 0, 9999, ValFun).

seed(Node, Start, End, ValFun) ->
    PBC = rt:pbc(Node),

    [ begin
          Obj = ValFun(Key),
          riakc_pb_socket:put(PBC, Obj, [{w,3}])
      end || Key <- lists:seq(Start, End)],

    riakc_pb_socket:stop(PBC).

%% ===================================================================
%% Monitor nodes after they upgrade
%% ===================================================================
init_node_monitor(Node, Sup, TestProc) ->
    spawn_link(fun() -> node_monitor(Node, Sup, TestProc) end).

node_monitor(Node, Sup, TestProc) ->
    lager:info("Monitoring node ~p to make sure it stays up.", [Node]),
    erlang:process_flag(trap_exit, true),
    erlang:monitor_node(Node, true),
    node_monitor_loop(Node, Sup, TestProc).

node_monitor_loop(Node, Sup, TestProc) ->
    receive
        {nodedown, Node} ->
            lager:error("Node ~p exited after upgrade!", [Node]),
            exit(Sup, normal),
            ?assertEqual(nodeup, {nodedown, Node});
        {'EXIT', TestProc, _} ->
            erlang:monitor_node(Node, false),
            ok;
        Other ->
            lager:warn("Node monitor for ~p got unknown message ~p", [Node, Other]),
            node_monitor_loop(Node, Sup, TestProc)
    end.
