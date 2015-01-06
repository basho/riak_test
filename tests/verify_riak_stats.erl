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
-module(verify_riak_stats).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(CTYPE, <<"counters">>).
-define(STYPE, <<"sets">>).
-define(MTYPE, <<"maps">>).
-define(TYPES, [{?CTYPE, counter},
                {?STYPE, set},
                {?MTYPE, map}]).

%% You should have curl installed locally to do this.
confirm() ->
    Nodes = rt:deploy_nodes(1),
    [Node1] = Nodes,
    verify_dt_converge:create_bucket_types(Nodes, ?TYPES),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),
    Stats1 = get_stats(Node1),
    AdminStats1 = get_console_stats(Node1),
    compare_http_and_console_stats(Stats1, AdminStats1),
    %% make sure a set of stats have valid values
    verify_nz(Stats1,[<<"cpu_nprocs">>,
                      <<"mem_total">>,
                      <<"mem_allocated">>,
                      <<"sys_logical_processors">>,
                      <<"sys_process_count">>,
                      <<"sys_thread_pool_size">>,
                      <<"sys_wordsize">>,
                      <<"ring_num_partitions">>,
                      <<"ring_creation_size">>,
                      <<"memory_total">>,
                      <<"memory_processes">>,
                      <<"memory_processes_used">>,
                      <<"memory_system">>,
                      <<"memory_atom">>,
                      <<"memory_atom_used">>,
                      <<"memory_binary">>,
                      <<"memory_code">>,
                      <<"memory_ets">>]),

    lager:info("perform 5 x  PUT and a GET to increment the stats"),
    lager:info("as the stat system only does calcs for > 5 readings"),

    C = rt:httpc(Node1),
    [rt:httpc_write(C, <<"systest">>, <<X>>, <<"12345">>) || X <- lists:seq(1, 5)],
    [rt:httpc_read(C, <<"systest">>, <<X>>) || X <- lists:seq(1, 5)],

    Stats2 = get_stats(Node1),

    %% make sure the stats that were supposed to increment did
    verify_inc(Stats1, Stats2, [{<<"node_gets">>, 10},
                                {<<"node_puts">>, 5},
                                {<<"node_gets_total">>, 10},
                                {<<"node_puts_total">>, 5},
                                {<<"vnode_gets">>, 30},
                                {<<"vnode_puts">>, 15},
                                {<<"vnode_gets_total">>, 30},
                                {<<"vnode_puts_total">>, 15}]),

    %% verify that fsm times were tallied
    verify_nz(Stats2, [<<"node_get_fsm_time_mean">>,
                       <<"node_get_fsm_time_median">>,
                       <<"node_get_fsm_time_95">>,
                       <<"node_get_fsm_time_99">>,
                       <<"node_get_fsm_time_100">>,
                       <<"node_put_fsm_time_mean">>,
                       <<"node_put_fsm_time_median">>,
                       <<"node_put_fsm_time_95">>,
                       <<"node_put_fsm_time_99">>,
                       <<"node_put_fsm_time_100">>]),


    lager:info("Make PBC Connection"),
    Pid = rt:pbc(Node1),

    Stats3 = get_stats(Node1),

    rt:systest_write(Node1, 1),
    %% make sure the stats that were supposed to increment did
    verify_inc(Stats2, Stats3, [{<<"pbc_connects_total">>, 1},
                                {<<"pbc_connects">>, 1},
                                {<<"pbc_active">>, 1}]),



    lager:info("Force Read Repair"),
    rt:pbc_write(Pid, <<"testbucket">>, <<"1">>, <<"blah!">>),
    rt:pbc_set_bucket_prop(Pid, <<"testbucket">>, [{n_val, 4}]),

    Stats4 = get_stats(Node1),
    verify_inc(Stats3, Stats4, [{<<"read_repairs_total">>, 0},
                                {<<"read_repairs">>, 0}]),

    _Value = rt:pbc_read(Pid, <<"testbucket">>, <<"1">>),

    Stats5 = get_stats(Node1),

    verify_inc(Stats3, Stats5, [{<<"read_repairs_total">>, 1},
                                {<<"read_repairs">>, 1}]),

    _ = do_datatypes(Pid),

    lager:info("Verifying datatype stats are non-zero."),

    Stats6 = get_stats(Node1),
    [
     begin
         lager:info("~s: ~p (expected non-zero)", [S, proplists:get_value(S, Stats6)]),
         verify_nz(Stats6, [S])
     end || S <- datatype_stats() ],
    pass.

verify_inc(Prev, Props, Keys) ->
    [begin
         Old = proplists:get_value(Key, Prev, 0),
         New = proplists:get_value(Key, Props, 0),
         lager:info("~s: ~p -> ~p (expected ~p)", [Key, Old, New, Old + Inc]),
         ?assertEqual(New, (Old + Inc))
     end || {Key, Inc} <- Keys].

verify_nz(Props, Keys) ->
    [?assertNotEqual(proplists:get_value(Key,Props,0), 0) || Key <- Keys].

get_stats(Node) ->
    timer:sleep(10000),
    StatString = os:cmd(io_lib:format("curl -s -S ~s/stats", [rt:http_url(Node)])),
    {struct, Stats} = mochijson2:decode(StatString),
    %%lager:debug(StatString),
    Stats.

get_console_stats(Node) ->
    %% Problem: rt:admin(Node, Cmd) seems to drop parts of the output when
    %% used for "riak-admin status" in 'rtdev'.
    %% Temporary workaround: use os:cmd/1 when in 'rtdev' (needs some cheats
    %% in order to find the right path etc.)
    try
	Stats =
	    case rt_config:get(rt_harness) of
		rtdev ->
		    N = rtdev:node_id(Node),
		    Path = rtdev:relpath(rtdev:node_version(N)),
		    Cmd = rtdev:riak_admin_cmd(Path, N, ["status"]),
		    lager:info("Cmd = ~p~n", [Cmd]),
		    os:cmd(Cmd);
		_ ->
		    rt:admin(Node, "status")
	    end,
	[S || {_,_} = S <-
		  [list_to_tuple(re:split(L, " : ", []))
		   || L <- tl(tl(string:tokens(Stats, "\n")))]]
    catch
	error:Reason ->
	    lager:info("riak-admin status ERROR: ~p~n~p~n",
		       [Reason, erlang:get_stacktrace()]),
	    []
    end.

compare_http_and_console_stats(Stats1, Stats2) ->
    OnlyInHttp = [S || {K,_} = S <- Stats1,
		       not lists:keymember(K, 1, Stats2)],
    OnlyInAdmin = [S || {K,_} = S <- Stats2,
			not lists:keymember(K, 1, Stats1)],
    lager:info("OnlyInHttp = ~p~n"
	       "OnlyInAdmin = ~p~n", [OnlyInHttp, OnlyInAdmin]),
    ?assertEqual([], OnlyInHttp),
    ?assertEqual([], OnlyInAdmin),
    ok.

datatype_stats() ->
    %% Merge stats are excluded because we likely never merge disjoint
    %% copies on a single node after a single write each.
    [ list_to_binary(Stat) || 
        Stat <- [
                 %%  "object_counter_merge"
                 %% ,"object_counter_merge_total"
                 %% ,"object_counter_merge_time_mean"
                 %% ,"object_counter_merge_time_median"
                 %% ,"object_counter_merge_time_95"
                 %% ,"object_counter_merge_time_99"
                 %% ,"object_counter_merge_time_100"
                 %% ,
                 "vnode_counter_update"
                ,"vnode_counter_update_total"
                ,"vnode_counter_update_time_mean"
                ,"vnode_counter_update_time_median"
                ,"vnode_counter_update_time_95"
                ,"vnode_counter_update_time_99"
                ,"vnode_counter_update_time_100"
                 %% ,"object_set_merge"
                 %% ,"object_set_merge_total"
                 %% ,"object_set_merge_time_mean"
                 %% ,"object_set_merge_time_median"
                 %% ,"object_set_merge_time_95"
                 %% ,"object_set_merge_time_99"
                 %% ,"object_set_merge_time_100"
                ,"vnode_set_update"
                ,"vnode_set_update_total"
                ,"vnode_set_update_time_mean"
                ,"vnode_set_update_time_median"
                ,"vnode_set_update_time_95"
                ,"vnode_set_update_time_99"
                ,"vnode_set_update_time_100"
                 %% ,"object_map_merge"
                 %% ,"object_map_merge_total"
                 %% ,"object_map_merge_time_mean"
                 %% ,"object_map_merge_time_median"
                 %% ,"object_map_merge_time_95"
                 %% ,"object_map_merge_time_99"
                 %% ,"object_map_merge_time_100"
                ,"vnode_map_update"
                ,"vnode_map_update_total"
                ,"vnode_map_update_time_mean"
                ,"vnode_map_update_time_median"
                ,"vnode_map_update_time_95"
                ,"vnode_map_update_time_99"
                ,"vnode_map_update_time_100"
                ,"node_gets_counter"
                ,"node_gets_counter_total"
                ,"node_get_fsm_counter_siblings_mean"
                ,"node_get_fsm_counter_siblings_median"
                ,"node_get_fsm_counter_siblings_95"
                ,"node_get_fsm_counter_siblings_99"
                ,"node_get_fsm_counter_siblings_100"
                ,"node_get_fsm_counter_objsize_mean"
                ,"node_get_fsm_counter_objsize_median"
                ,"node_get_fsm_counter_objsize_95"
                ,"node_get_fsm_counter_objsize_99"
                ,"node_get_fsm_counter_objsize_100"
                ,"node_get_fsm_counter_time_mean"
                ,"node_get_fsm_counter_time_median"
                ,"node_get_fsm_counter_time_95"
                ,"node_get_fsm_counter_time_99"
                ,"node_get_fsm_counter_time_100"
                ,"node_gets_set"
                ,"node_gets_set_total"
                ,"node_get_fsm_set_siblings_mean"
                ,"node_get_fsm_set_siblings_median"
                ,"node_get_fsm_set_siblings_95"
                ,"node_get_fsm_set_siblings_99"
                ,"node_get_fsm_set_siblings_100"
                ,"node_get_fsm_set_objsize_mean"
                ,"node_get_fsm_set_objsize_median"
                ,"node_get_fsm_set_objsize_95"
                ,"node_get_fsm_set_objsize_99"
                ,"node_get_fsm_set_objsize_100"
                ,"node_get_fsm_set_time_mean"
                ,"node_get_fsm_set_time_median"
                ,"node_get_fsm_set_time_95"
                ,"node_get_fsm_set_time_99"
                ,"node_get_fsm_set_time_100"
                ,"node_gets_map"
                ,"node_gets_map_total"
                ,"node_get_fsm_map_siblings_mean"
                ,"node_get_fsm_map_siblings_median"
                ,"node_get_fsm_map_siblings_95"
                ,"node_get_fsm_map_siblings_99"
                ,"node_get_fsm_map_siblings_100"
                ,"node_get_fsm_map_objsize_mean"
                ,"node_get_fsm_map_objsize_median"
                ,"node_get_fsm_map_objsize_95"
                ,"node_get_fsm_map_objsize_99"
                ,"node_get_fsm_map_objsize_100"
                ,"node_get_fsm_map_time_mean"
                ,"node_get_fsm_map_time_median"
                ,"node_get_fsm_map_time_95"
                ,"node_get_fsm_map_time_99"
                ,"node_get_fsm_map_time_100"
                ,"node_puts_counter"
                ,"node_puts_counter_total"
                ,"node_put_fsm_counter_time_mean"
                ,"node_put_fsm_counter_time_median"
                ,"node_put_fsm_counter_time_95"
                ,"node_put_fsm_counter_time_99"
                ,"node_put_fsm_counter_time_100"
                ,"node_puts_set"
                ,"node_puts_set_total"
                ,"node_put_fsm_set_time_mean"
                ,"node_put_fsm_set_time_median"
                ,"node_put_fsm_set_time_95"
                ,"node_put_fsm_set_time_99"
                ,"node_put_fsm_set_time_100"
                ,"node_puts_map"
                ,"node_puts_map_total"
                ,"node_put_fsm_map_time_mean"
                ,"node_put_fsm_map_time_median"
                ,"node_put_fsm_map_time_95"
                ,"node_put_fsm_map_time_99"
                ,"node_put_fsm_map_time_100"
                ,"counter_actor_counts_mean"
                ,"counter_actor_counts_median"
                ,"counter_actor_counts_95"
                ,"counter_actor_counts_99"
                ,"counter_actor_counts_100"
                ,"set_actor_counts_mean"
                ,"set_actor_counts_median"
                ,"set_actor_counts_95"
                ,"set_actor_counts_99"
                ,"set_actor_counts_100"
                ,"map_actor_counts_mean"
                ,"map_actor_counts_median"
                ,"map_actor_counts_95"
                ,"map_actor_counts_99"
                ,"map_actor_counts_100"
                ]
    ].

do_datatypes(Pid) ->
    _ = [ get_and_update(Pid, Type) || Type <- [counter, set, map]].

get_and_update(Pid, counter) ->

    _ = [ riakc_pb_socket:update_type(Pid, {?CTYPE, <<"pb">>}, <<I>>,
                                      {counter, {increment, 5},
                                       undefined})
          || I <- lists:seq(1, 10) ],

    _ = [ riakc_pb_socket:fetch_type(Pid, {?CTYPE, <<"pb">>}, <<I>>)
          || I <- lists:seq(1, 10) ];

get_and_update(Pid, set) ->

    _ = [ riakc_pb_socket:update_type(Pid, {?STYPE, <<"pb">>}, <<I>>,
                                      {set, {add_all, [<<"a">>, <<"b">>]}, undefined})
          || I <- lists:seq(1, 10) ],

    _ = [ riakc_pb_socket:fetch_type(Pid, {?STYPE, <<"pb">>}, <<I>>)
          || I <- lists:seq(1, 10) ];

get_and_update(Pid, map) ->

    _ = [ riakc_pb_socket:update_type(Pid, {?MTYPE, <<"pb">>}, <<I>>,
                                      {map,
                                       {update,[
                                                {update, {<<"a">>, counter}, {increment, 5}}
                                               ]},
                                       undefined})
          || I <- lists:seq(1, 10) ],

    _ = [ riakc_pb_socket:fetch_type(Pid, {?MTYPE, <<"pb">>}, <<I>>)
          || I <- lists:seq(1, 10) ].
