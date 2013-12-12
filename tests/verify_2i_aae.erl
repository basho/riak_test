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
-module(verify_2i_aae).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

%% Make it multi-backend compatible.
-define(BUCKETS, [<<"eleveldb1">>, <<"memory1">>]).
-define(NUM_ITEMS, 1000).
-define(NUM_DELETES, 100).
-define(SCAN_BATCH_SIZE, 100).
-define(N_VAL, 3).

confirm() ->
    Nodes =
    [Node1] = rt:build_cluster(1,
                               [{riak_kv,
                                 [{anti_entropy_build_limit, {100, 1000}},
                                  {anti_entropy_concurrency, 100},
                                  {anti_entropy_tick, 1000}]}]),
    rt:wait_until_aae_trees_built(Nodes),
    rt_intercept:load_code(Node1),
    rt_intercept:add(Node1,
                     {riak_object,
                      [{{index_specs, 1}, skippable_index_specs},
                       {{diff_index_specs, 2}, skippable_diff_index_specs}]}),
    lager:info("Installed intercepts to corrupt index specs on node ~p", [Node1]),
    PBC = rt:pbc(Node1),
    NumItems = ?NUM_ITEMS,
    %%NumDelItems = NumItems div 10,
    NumDel = ?NUM_DELETES,
    pass = check_lost_objects(Node1, PBC, NumItems, NumDel),
    pass = check_lost_indexes(Node1, PBC, NumItems),
    pass = check_kill_repair(Node1),
    lager:info("Et voila"),
    riakc_pb_socket:stop(PBC),
    pass.

%% Write objects with a 2i index. Modify/delete the objects without updating
%% the 2i index. Test that running 2i repair corrects the 2i indexes.
check_lost_objects(Node1, PBC, NumItems, NumDel) ->
    Index = {integer_index, "i"},
    set_skip_index_specs(Node1, false),
    lager:info("Putting ~p objects with indexes", [NumItems]),
    [put_obj(PBC, Bucket, N, N+1, Index) || N <- lists:seq(1, NumItems),
                                            Bucket <- ?BUCKETS],
    %% Verify they are there.
    ExpectedInitial = [{to_key(N+1), to_key(N)} || N <- lists:seq(1, NumItems)],
    lager:info("Check objects are there as expected"),
    [assert_range_query(PBC, Bucket, ExpectedInitial, Index, 1, NumItems+1)
     || Bucket <- ?BUCKETS],
    lager:info("Now mess index spec code and change values"),
    set_skip_index_specs(Node1, true),
    [put_obj(PBC, Bucket, N, N, Index) || N <- lists:seq(1, NumItems-NumDel),
                                          Bucket <- ?BUCKETS],
    DelRange = lists:seq(NumItems-NumDel+1, NumItems),
    lager:info("Deleting ~b objects without updating indexes", [NumDel]),
    [del_obj(PBC, Bucket, N) || N <- DelRange, Bucket <- ?BUCKETS],
    DelKeys = [to_key(N) || N <- DelRange], 
    [rt:wait_until(fun() -> rt:pbc_really_deleted(PBC, Bucket, DelKeys) end)
     || Bucket <- ?BUCKETS],
    %% Verify they are damaged
    lager:info("Verify change did not take, needs repair"),
    [assert_range_query(PBC, Bucket, ExpectedInitial, Index, 1, NumItems+1)
     || Bucket <- ?BUCKETS],
    set_skip_index_specs(Node1, false),
    run_2i_repair(Node1),
    lager:info("Now verify that previous changes are visible after repair"),
    ExpectedFinal = [{to_key(N), to_key(N)} || N <- lists:seq(1, NumItems-NumDel)],
    [assert_range_query(PBC, Bucket, ExpectedFinal, Index, 1, NumItems+1)
     || Bucket <- ?BUCKETS],
    pass.

%% Write objects without a 2i index. Test that running 2i repair will generate
%% the missing indexes.
check_lost_indexes(Node1, PBC, NumItems) ->
    set_skip_index_specs(Node1, true),
    Index = {integer_index, "ii"},
    lager:info("Writing ~b objects without index", [NumItems]),
    [put_obj(PBC, Bucket, N, N+1, Index) || Bucket <- ?BUCKETS,
                                            N <- lists:seq(1, NumItems)],
    lager:info("Verify that objects cannot be found via index"),
    [assert_range_query(PBC, Bucket, [], Index, 1, NumItems+1)
     || Bucket <- ?BUCKETS],
    run_2i_repair(Node1),
    lager:info("Check that objects can now be found via index"),
    Expected = [{to_key(N+1), to_key(N)} || N <- lists:seq(1, NumItems)],
    [assert_range_query(PBC, Bucket, Expected, Index, 1, NumItems+1)
     || Bucket <- ?BUCKETS],
    pass.

check_kill_repair(Node1) ->
    lager:info("Test that killing 2i repair works as desired"),
    spawn(fun() ->
                  timer:sleep(1500),
                  rt:admin(Node1, ["repair-2i", "kill"])
          end),
    ExitStatus = run_2i_repair(Node1),
    case ExitStatus of
        normal ->
            lager:info("Shucks. Repair finished before we could kill it");
        kill ->
            lager:info("Repair was forcibly killed");
        user_request ->
            lager:info("Repair exited gracefully, we should be able to "
                       "trigger another repair immediately"),
            normal = run_2i_repair(Node1)
    end,
    pass.

run_2i_repair(Node1) ->
    lager:info("Run 2i AAE repair"),
    ?assertMatch({ok, _}, rt:admin(Node1, ["repair-2i"])),
    RepairPid = rpc:call(Node1, erlang, whereis, [riak_kv_2i_aae]),
    lager:info("Wait for repair process to finish"),
    Mon = monitor(process, RepairPid),
    MaxWaitTime = rt_config:get(rt_max_wait_time),
    receive
        {'DOWN', Mon, _, _, Status} ->
            lager:info("Status: ~p", [Status]),
            Status
    after
        MaxWaitTime ->
            lager:error("Timed out (~pms) waiting for 2i AAE repair process", [MaxWaitTime]),
            ?assertEqual(aae_2i_repair_complete, aae_2i_repair_timeout)
    end.

set_skip_index_specs(Node, Val) ->
    ok = rpc:call(Node, application, set_env,
                  [riak_kv, skip_index_specs, Val]).

to_key(N) ->
    list_to_binary(integer_to_list(N)).

put_obj(PBC, Bucket, N, IN, Index) ->
    K = to_key(N),
    Obj =
    case riakc_pb_socket:get(PBC, Bucket, K) of
        {ok, ExistingObj} ->
            ExistingObj;
        _ ->
            riakc_obj:new(Bucket, K, K)
    end,
    MD = riakc_obj:get_metadata(Obj),
    MD2 = riakc_obj:set_secondary_index(MD, {Index, [IN]}),
    Obj2 = riakc_obj:update_metadata(Obj, MD2),
    riakc_pb_socket:put(PBC, Obj2, [{dw, ?N_VAL}]).

del_obj(PBC, Bucket, N) ->
    K = to_key(N),
    case riakc_pb_socket:get(PBC, Bucket, K) of
        {ok, ExistingObj} ->
            ?assertMatch(ok, riakc_pb_socket:delete_obj(PBC, ExistingObj));
        _ ->
            ?assertMatch(ok, riakc_pb_socket:delete(PBC, Bucket, K))
    end.


assert_range_query(Pid, Bucket, Expected0, Index, StartValue, EndValue) ->
    lager:info("Searching Index ~p/~p for ~p-~p", [Bucket, Index, StartValue, EndValue]),
    {ok, ?INDEX_RESULTS{terms=Keys}} = riakc_pb_socket:get_index_range(Pid, Bucket, Index, StartValue, EndValue, [{return_terms, true}]),
    Actual = case Keys of
                 undefined ->
                     [];
                 _ ->
                     lists:sort(Keys)
             end,
    Expected = lists:sort(Expected0),
    ?assertEqual({Bucket, Expected}, {Bucket, Actual}),
    lager:info("Yay! ~b (actual) == ~b (expected)", [length(Actual), length(Expected)]).
