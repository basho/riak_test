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
-export([confirm/0, get_stats/1]).
-include_lib("eunit/include/eunit.hrl").

-define(CTYPE, <<"counters">>).
-define(STYPE, <<"sets">>).
-define(MTYPE, <<"maps">>).
-define(TYPES, [{?CTYPE, counter},
                {?STYPE, set},
                {?MTYPE, map}]).
-define(CONF, [
        {yokozuna,
            [{enabled, true}]
        }]).

%% You should have curl installed locally to do this.
confirm() ->
    Nodes = rt:deploy_nodes(1, ?CONF),
    [Node1] = Nodes,
    verify_dt_converge:create_bucket_types(Nodes, ?TYPES),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),
    Stats1 = get_stats(Node1),
    TestMetaData = riak_test_runner:metadata(),
    KVBackend = proplists:get_value(backend, TestMetaData),
    HeadSupport = has_head_support(KVBackend),

    lager:info("Verifying that all expected stats keys are present from the HTTP endpoint"),
    ok = verify_stats_keys_complete(Node1, Stats1),

    AdminStats1 = get_console_stats(Node1),
    lager:info("Verifying that the stats keys in riak-admin status and HTTP match"),
    ok = compare_http_and_console_stats(Stats1, AdminStats1),

    %% make sure a set of stats have valid values
    lager:info("Verifying that the system and ring stats have valid values"),
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

    ExpectedNodeStats = 
        case HeadSupport of
            true ->
                [{<<"node_gets">>, 10},
                    {<<"node_puts">>, 5},
                    {<<"node_gets_total">>, 10},
                    {<<"node_puts_total">>, 5},
                    {<<"vnode_gets">>, 5}, 
                        % The five PUTS will require only HEADs
                    {<<"vnode_heads">>, 30},
                        % There is no reduction in the count of HEADs
                        % as HEADS before GETs
                    {<<"vnode_puts">>, 15},
                    {<<"vnode_gets_total">>, 5},
                    {<<"vnode_heads_total">>, 30},
                    {<<"vnode_puts_total">>, 15}];
            false ->
                [{<<"node_gets">>, 10},
                    {<<"node_puts">>, 5},
                    {<<"node_gets_total">>, 10},
                    {<<"node_puts_total">>, 5},
                    {<<"vnode_gets">>, 30},
                    {<<"vnode_heads">>, 0},
                    {<<"vnode_puts">>, 15},
                    {<<"vnode_gets_total">>, 30},
                    {<<"vnode_heads_total">>, 0},
                    {<<"vnode_puts_total">>, 15}]
        end,

    %% make sure the stats that were supposed to increment did
    verify_inc(Stats1, Stats2, ExpectedNodeStats),

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

    _ = do_pools(Node1),

    Stats7 = get_stats(Node1),
    lager:info("Verifying pool stats are incremented"),

    verify_inc(Stats6, Stats7, inc_by_one(dscp_stats())),

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

has_head_support(leveled) ->
    true;
has_head_support(_Backend) ->
    false.

get_stats(Node) ->
    timer:sleep(10000),
    lager:info("Retrieving stats from node ~s", [Node]),
    StatsCommand = io_lib:format("curl -s -S ~s/stats", [rt:http_url(Node)]),
    lager:debug("Retrieving stats using command ~s", [StatsCommand]),
    StatString = os:cmd(StatsCommand),
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
    maybe_log_stats_keys(OnlyInHttp, "Keys missing from riak-admin"),
    maybe_log_stats_keys(OnlyInAdmin, "Keys missing from HTTP"),
    ?assertEqual([], OnlyInHttp),
    ?assertEqual([], OnlyInAdmin),
    ok.

verify_stats_keys_complete(Node, Stats) ->
    ActualKeys = proplists:get_keys(Stats),
    ExpectedKeys = all_stats(Node),
    MissingStatsKeys = diff_lists(ActualKeys, ExpectedKeys),
    AdditionalStatsKeys = diff_lists(ExpectedKeys, ActualKeys),
    maybe_log_stats_keys(MissingStatsKeys, "missing stats keys"),
    maybe_log_stats_keys(AdditionalStatsKeys, "additional stats"),
    ?assertEqual({[],[]}, {MissingStatsKeys, AdditionalStatsKeys}),
    ok.

diff_lists(List, ThatList) ->
    lists:filter(fun(Element) -> not lists:member(Element, List) end, ThatList).

-spec maybe_log_stats_keys([binary()], string()) -> ok.
maybe_log_stats_keys(StatsKeys, _Description) when length(StatsKeys) == 0 ->
    ok;
maybe_log_stats_keys(StatsKeys, Description) ->
    lager:info("~s: ~s", [Description, pretty_print_stats_keys(StatsKeys)]).

-spec pretty_print_stats_keys([binary()]) -> string().
pretty_print_stats_keys(StatsKeys) ->
    ConvertedStatsKeys = lists:map(fun(StatsKey) -> binary_to_list(StatsKey) end, StatsKeys),
    string:join(ConvertedStatsKeys, ", ").

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

all_stats(Node) ->
    common_stats() ++ product_stats(rt:product(Node)).

common_stats() ->
    [
        <<"asn1_version">>,
        <<"basho_stats_version">>,
        <<"bitcask_version">>,
        <<"clique_version">>,
        <<"cluster_info_version">>,
        <<"clusteraae_fsm_create">>,
        <<"clusteraae_fsm_create_error">>,
        <<"clusteraae_fsm_active">>,
        <<"compiler_version">>,
        <<"connected_nodes">>,
        <<"consistent_get_objsize_100">>,
        <<"consistent_get_objsize_95">>,
        <<"consistent_get_objsize_99">>,
        <<"consistent_get_objsize_mean">>,
        <<"consistent_get_objsize_median">>,
        <<"consistent_get_time_100">>,
        <<"consistent_get_time_95">>,
        <<"consistent_get_time_99">>,
        <<"consistent_get_time_mean">>,
        <<"consistent_get_time_median">>,
        <<"consistent_gets">>,
        <<"consistent_gets_total">>,
        <<"consistent_put_objsize_100">>,
        <<"consistent_put_objsize_95">>,
        <<"consistent_put_objsize_99">>,
        <<"consistent_put_objsize_mean">>,
        <<"consistent_put_objsize_median">>,
        <<"consistent_put_time_100">>,
        <<"consistent_put_time_95">>,
        <<"consistent_put_time_99">>,
        <<"consistent_put_time_mean">>,
        <<"consistent_put_time_median">>,
        <<"consistent_puts">>,
        <<"consistent_puts_total">>,
        <<"converge_delay_last">>,
        <<"converge_delay_max">>,
        <<"converge_delay_mean">>,
        <<"converge_delay_min">>,
        <<"coord_redirs_total">>,
        <<"coord_redir_least_loaded_total">>,
        <<"coord_local_unloaded_total">>,
        <<"coord_local_soft_loaded_total">>,
        <<"vnode_mbox_check_timeout_total">>,
        <<"coord_redir_unloaded_total">>,
        <<"coord_redir_loaded_local_total">>,
        <<"soft_loaded_vnode_mbox_total">>,
        <<"counter_actor_counts_100">>,
        <<"counter_actor_counts_95">>,
        <<"counter_actor_counts_99">>,
        <<"counter_actor_counts_mean">>,
        <<"counter_actor_counts_median">>,
        <<"cpu_avg1">>,
        <<"cpu_avg15">>,
        <<"cpu_avg5">>,
        <<"cpu_nprocs">>,
        <<"crypto_version">>,
        <<"disk">>,
        <<"dropped_vnode_requests_total">>,
        <<"ebloom_version">>,
        <<"eleveldb_version">>,
        <<"erlang_js_version">>,
        <<"erlydtl_version">>,
        <<"executing_mappers">>,
        <<"exometer_core_version">>,
        <<"fuse_version">>,
        <<"goldrush_version">>,
        <<"gossip_received">>,
        <<"handoff_timeouts">>,
        <<"hll_bytes">>,
        <<"hll_bytes_mean">>,
        <<"hll_bytes_100">>,
        <<"hll_bytes_95">>,
        <<"hll_bytes_99">>,
        <<"hll_bytes_median">>,
        <<"hll_bytes_total">>,
        <<"ibrowse_version">>,
        <<"ignored_gossip_total">>,
        <<"index_fsm_active">>,
        <<"index_fsm_create">>,
        <<"index_fsm_create_error">>,
        <<"inets_version">>,
        <<"kernel_version">>,
        <<"lager_version">>,
        <<"late_put_fsm_coordinator_ack">>,
        <<"leveldb_read_block_error">>,
        <<"list_fsm_active">>,
        <<"list_fsm_create">>,
        <<"list_fsm_create_error">>,
        <<"list_fsm_create_error_total">>,
        <<"list_fsm_create_total">>,
        <<"map_actor_counts_100">>,
        <<"map_actor_counts_95">>,
        <<"map_actor_counts_99">>,
        <<"map_actor_counts_mean">>,
        <<"map_actor_counts_median">>,
        <<"mem_allocated">>,
        <<"mem_total">>,
        <<"memory_atom">>,
        <<"memory_atom_used">>,
        <<"memory_binary">>,
        <<"memory_code">>,
        <<"memory_ets">>,
        <<"memory_processes">>,
        <<"memory_processes_used">>,
        <<"memory_system">>,
        <<"memory_total">>,
        <<"mochiweb_version">>,
        <<"ngrfetch_nofetch">>,
        <<"ngrfetch_nofetch_total">>,
        <<"ngrfetch_prefetch">>,
        <<"ngrfetch_prefetch_total">>,
        <<"ngrfetch_tofetch">>,
        <<"ngrfetch_tofetch_total">>,
        <<"ngrrepl_object">>,
        <<"ngrrepl_object_total">>,
        <<"ngrrepl_empty">>,
        <<"ngrrepl_empty_total">>,
        <<"ngrrepl_error">>,
        <<"ngrrepl_error_total">>,
        <<"node_get_fsm_active">>,
        <<"node_get_fsm_active_60s">>,
        <<"node_get_fsm_counter_objsize_100">>,
        <<"node_get_fsm_counter_objsize_95">>,
        <<"node_get_fsm_counter_objsize_99">>,
        <<"node_get_fsm_counter_objsize_mean">>,
        <<"node_get_fsm_counter_objsize_median">>,
        <<"node_get_fsm_counter_siblings_100">>,
        <<"node_get_fsm_counter_siblings_95">>,
        <<"node_get_fsm_counter_siblings_99">>,
        <<"node_get_fsm_counter_siblings_mean">>,
        <<"node_get_fsm_counter_siblings_median">>,
        <<"node_get_fsm_counter_time_100">>,
        <<"node_get_fsm_counter_time_95">>,
        <<"node_get_fsm_counter_time_99">>,
        <<"node_get_fsm_counter_time_mean">>,
        <<"node_get_fsm_counter_time_median">>,
        <<"node_get_fsm_errors">>,
        <<"node_get_fsm_errors_total">>,
        <<"node_get_fsm_hll_objsize_100">>,
        <<"node_get_fsm_hll_objsize_95">>,
        <<"node_get_fsm_hll_objsize_99">>,
        <<"node_get_fsm_hll_objsize_mean">>,
        <<"node_get_fsm_hll_objsize_median">>,
        <<"node_get_fsm_hll_siblings_100">>,
        <<"node_get_fsm_hll_siblings_95">>,
        <<"node_get_fsm_hll_siblings_99">>,
        <<"node_get_fsm_hll_siblings_mean">>,
        <<"node_get_fsm_hll_siblings_median">>,
        <<"node_get_fsm_hll_time_100">>,
        <<"node_get_fsm_hll_time_95">>,
        <<"node_get_fsm_hll_time_99">>,
        <<"node_get_fsm_hll_time_mean">>,
        <<"node_get_fsm_hll_time_median">>,
        <<"node_get_fsm_in_rate">>,
        <<"node_get_fsm_map_objsize_100">>,
        <<"node_get_fsm_map_objsize_95">>,
        <<"node_get_fsm_map_objsize_99">>,
        <<"node_get_fsm_map_objsize_mean">>,
        <<"node_get_fsm_map_objsize_median">>,
        <<"node_get_fsm_map_siblings_100">>,
        <<"node_get_fsm_map_siblings_95">>,
        <<"node_get_fsm_map_siblings_99">>,
        <<"node_get_fsm_map_siblings_mean">>,
        <<"node_get_fsm_map_siblings_median">>,
        <<"node_get_fsm_map_time_100">>,
        <<"node_get_fsm_map_time_95">>,
        <<"node_get_fsm_map_time_99">>,
        <<"node_get_fsm_map_time_mean">>,
        <<"node_get_fsm_map_time_median">>,
        <<"node_get_fsm_objsize_100">>,
        <<"node_get_fsm_objsize_95">>,
        <<"node_get_fsm_objsize_99">>,
        <<"node_get_fsm_objsize_mean">>,
        <<"node_get_fsm_objsize_median">>,
        <<"node_get_fsm_out_rate">>,
        <<"node_get_fsm_rejected">>,
        <<"node_get_fsm_rejected_60s">>,
        <<"node_get_fsm_rejected_total">>,
        <<"node_get_fsm_set_objsize_100">>,
        <<"node_get_fsm_set_objsize_95">>,
        <<"node_get_fsm_set_objsize_99">>,
        <<"node_get_fsm_set_objsize_mean">>,
        <<"node_get_fsm_set_objsize_median">>,
        <<"node_get_fsm_set_siblings_100">>,
        <<"node_get_fsm_set_siblings_95">>,
        <<"node_get_fsm_set_siblings_99">>,
        <<"node_get_fsm_set_siblings_mean">>,
        <<"node_get_fsm_set_siblings_median">>,
        <<"node_get_fsm_set_time_100">>,
        <<"node_get_fsm_set_time_95">>,
        <<"node_get_fsm_set_time_99">>,
        <<"node_get_fsm_set_time_mean">>,
        <<"node_get_fsm_set_time_median">>,
        <<"node_get_fsm_siblings_100">>,
        <<"node_get_fsm_siblings_95">>,
        <<"node_get_fsm_siblings_99">>,
        <<"node_get_fsm_siblings_mean">>,
        <<"node_get_fsm_siblings_median">>,
        <<"node_get_fsm_time_100">>,
        <<"node_get_fsm_time_95">>,
        <<"node_get_fsm_time_99">>,
        <<"node_get_fsm_time_mean">>,
        <<"node_get_fsm_time_median">>,
        <<"node_gets">>,
        <<"node_gets_counter">>,
        <<"node_gets_counter_total">>,
        <<"node_gets_hll">>,
        <<"node_gets_hll_total">>,
        <<"node_gets_map">>,
        <<"node_gets_map_total">>,
        <<"node_gets_set">>,
        <<"node_gets_set_total">>,
        <<"node_gets_total">>,
        <<"node_put_fsm_active">>,
        <<"node_put_fsm_active_60s">>,
        <<"node_put_fsm_counter_time_100">>,
        <<"node_put_fsm_counter_time_95">>,
        <<"node_put_fsm_counter_time_99">>,
        <<"node_put_fsm_counter_time_mean">>,
        <<"node_put_fsm_counter_time_median">>,
        <<"node_put_fsm_hll_time_100">>,
        <<"node_put_fsm_hll_time_95">>,
        <<"node_put_fsm_hll_time_99">>,
        <<"node_put_fsm_hll_time_mean">>,
        <<"node_put_fsm_hll_time_median">>,
        <<"node_put_fsm_in_rate">>,
        <<"node_put_fsm_map_time_100">>,
        <<"node_put_fsm_map_time_95">>,
        <<"node_put_fsm_map_time_99">>,
        <<"node_put_fsm_map_time_mean">>,
        <<"node_put_fsm_map_time_median">>,
        <<"node_put_fsm_out_rate">>,
        <<"node_put_fsm_rejected">>,
        <<"node_put_fsm_rejected_60s">>,
        <<"node_put_fsm_rejected_total">>,
        <<"node_put_fsm_set_time_100">>,
        <<"node_put_fsm_set_time_95">>,
        <<"node_put_fsm_set_time_99">>,
        <<"node_put_fsm_set_time_mean">>,
        <<"node_put_fsm_set_time_median">>,
        <<"node_put_fsm_time_100">>,
        <<"node_put_fsm_time_95">>,
        <<"node_put_fsm_time_99">>,
        <<"node_put_fsm_time_mean">>,
        <<"node_put_fsm_time_median">>,
        <<"node_puts">>,
        <<"node_puts_counter">>,
        <<"node_puts_counter_total">>,
        <<"node_puts_hll">>,
        <<"node_puts_hll_total">>,
        <<"node_puts_map">>,
        <<"node_puts_map_total">>,
        <<"node_puts_set">>,
        <<"node_puts_set_total">>,
        <<"node_puts_total">>,
        <<"nodename">>,
        <<"object_counter_merge">>,
        <<"object_counter_merge_time_100">>,
        <<"object_counter_merge_time_95">>,
        <<"object_counter_merge_time_99">>,
        <<"object_counter_merge_time_mean">>,
        <<"object_counter_merge_time_median">>,
        <<"object_counter_merge_total">>,
        <<"object_hll_merge">>,
        <<"object_hll_merge_time_100">>,
        <<"object_hll_merge_time_95">>,
        <<"object_hll_merge_time_99">>,
        <<"object_hll_merge_time_mean">>,
        <<"object_hll_merge_time_median">>,
        <<"object_hll_merge_total">>,
        <<"object_map_merge">>,
        <<"object_map_merge_time_100">>,
        <<"object_map_merge_time_95">>,
        <<"object_map_merge_time_99">>,
        <<"object_map_merge_time_mean">>,
        <<"object_map_merge_time_median">>,
        <<"object_map_merge_total">>,
        <<"object_merge">>,
        <<"object_merge_time_100">>,
        <<"object_merge_time_95">>,
        <<"object_merge_time_99">>,
        <<"object_merge_time_mean">>,
        <<"object_merge_time_median">>,
        <<"object_merge_total">>,
        <<"object_set_merge">>,
        <<"object_set_merge_time_100">>,
        <<"object_set_merge_time_95">>,
        <<"object_set_merge_time_99">>,
        <<"object_set_merge_time_mean">>,
        <<"object_set_merge_time_median">>,
        <<"object_set_merge_total">>,
        <<"os_mon_version">>,
        <<"pbc_active">>,
        <<"pbc_connects">>,
        <<"pbc_connects_total">>,
        <<"pbkdf2_version">>,
        <<"pipeline_active">>,
        <<"pipeline_create_count">>,
        <<"pipeline_create_error_count">>,
        <<"pipeline_create_error_one">>,
        <<"pipeline_create_one">>,
        <<"poolboy_version">>,
        <<"postcommit_fail">>,
        <<"precommit_fail">>,
        <<"protobuffs_version">>,
        <<"public_key_version">>,
        <<"ranch_version">>,
        <<"read_repairs">>,
        <<"read_repairs_counter">>,
        <<"read_repairs_counter_total">>,
        <<"read_repairs_fallback_notfound_count">>,
        <<"read_repairs_fallback_notfound_one">>,
        <<"read_repairs_fallback_outofdate_count">>,
        <<"read_repairs_fallback_outofdate_one">>,
        <<"read_repairs_hll">>,
        <<"read_repairs_hll_total">>,
        <<"read_repairs_map">>,
        <<"read_repairs_map_total">>,
        <<"read_repairs_primary_notfound_count">>,
        <<"read_repairs_primary_notfound_one">>,
        <<"read_repairs_primary_outofdate_count">>,
        <<"read_repairs_primary_outofdate_one">>,
        <<"read_repairs_set">>,
        <<"read_repairs_set_total">>,
        <<"read_repairs_total">>,
        <<"rebalance_delay_last">>,
        <<"rebalance_delay_max">>,
        <<"rebalance_delay_mean">>,
        <<"rebalance_delay_min">>,
        <<"rejected_handoffs">>,
        <<"riak_api_version">>,
        <<"riak_auth_mods_version">>,
        <<"riak_control_version">>,
        <<"riak_core_version">>,
        <<"riak_dt_version">>,
        <<"riak_kv_version">>,
        <<"riak_kv_vnodeq_max">>,
        <<"riak_kv_vnodeq_mean">>,
        <<"riak_kv_vnodeq_median">>,
        <<"riak_kv_vnodeq_min">>,
        <<"riak_kv_vnodeq_total">>,
        <<"riak_kv_vnodes_running">>,
        <<"riak_pb_version">>,
        <<"riak_pipe_version">>,
        <<"riak_pipe_vnodeq_max">>,
        <<"riak_pipe_vnodeq_mean">>,
        <<"riak_pipe_vnodeq_median">>,
        <<"riak_pipe_vnodeq_min">>,
        <<"riak_pipe_vnodeq_total">>,
        <<"riak_pipe_vnodes_running">>,
        <<"riak_repl_version">>,
        <<"riak_sysmon_version">>,
        <<"ring_creation_size">>,
        <<"ring_members">>,
        <<"ring_num_partitions">>,
        <<"ring_ownership">>,
        <<"rings_reconciled">>,
        <<"rings_reconciled_total">>,
        <<"runtime_tools_version">>,
        <<"sasl_version">>,
        <<"search_blockedvnode_count">>,
        <<"search_blockedvnode_one">>,
        <<"search_detected_repairs_count">>,
        <<"search_index_bad_entry_count">>,
        <<"search_index_bad_entry_one">>,
        <<"search_index_error_threshold_blown_count">>,
        <<"search_index_error_threshold_blown_one">>,
        <<"search_index_error_threshold_failure_count">>,
        <<"search_index_error_threshold_failure_one">>,
        <<"search_index_error_threshold_ok_count">>,
        <<"search_index_error_threshold_ok_one">>,
        <<"search_index_error_threshold_recovered_count">>,
        <<"search_index_error_threshold_recovered_one">>,
        <<"search_index_extract_fail_count">>,
        <<"search_index_extract_fail_one">>,
        <<"search_index_fail_count">>,
        <<"search_index_fail_one">>,
        <<"search_index_latency_95">>,
        <<"search_index_latency_99">>,
        <<"search_index_latency_999">>,
        <<"search_index_latency_max">>,
        <<"search_index_latency_mean">>,
        <<"search_index_latency_median">>,
        <<"search_index_latency_min">>,
        <<"search_index_throughput_count">>,
        <<"search_index_throughput_one">>,
        <<"search_query_fail_count">>,
        <<"search_query_fail_one">>,
        <<"search_query_latency_95">>,
        <<"search_query_latency_99">>,
        <<"search_query_latency_999">>,
        <<"search_query_latency_max">>,
        <<"search_query_latency_mean">>,
        <<"search_query_latency_median">>,
        <<"search_query_latency_min">>,
        <<"search_query_throughput_count">>,
        <<"search_query_throughput_one">>,
        <<"search_queue_batch_latency_95">>,
        <<"search_queue_batch_latency_99">>,
        <<"search_queue_batch_latency_999">>,
        <<"search_queue_batch_latency_max">>,
        <<"search_queue_batch_latency_mean">>,
        <<"search_queue_batch_latency_median">>,
        <<"search_queue_batch_latency_min">>,
        <<"search_queue_batch_throughput_count">>,
        <<"search_queue_batch_throughput_one">>,
        <<"search_queue_batchsize_max">>,
        <<"search_queue_batchsize_mean">>,
        <<"search_queue_batchsize_median">>,
        <<"search_queue_batchsize_min">>,
        <<"search_queue_drain_cancel_timeout_count">>,
        <<"search_queue_drain_cancel_timeout_one">>,
        <<"search_queue_drain_count">>,
        <<"search_queue_drain_fail_count">>,
        <<"search_queue_drain_fail_one">>,
        <<"search_queue_drain_latency_95">>,
        <<"search_queue_drain_latency_99">>,
        <<"search_queue_drain_latency_999">>,
        <<"search_queue_drain_latency_max">>,
        <<"search_queue_drain_latency_mean">>,
        <<"search_queue_drain_latency_median">>,
        <<"search_queue_drain_latency_min">>,
        <<"search_queue_drain_one">>,
        <<"search_queue_drain_timeout_count">>,
        <<"search_queue_drain_timeout_one">>,
        <<"search_queue_hwm_purged_count">>,
        <<"search_queue_hwm_purged_one">>,
        <<"search_queue_total_length">>,
        <<"set_actor_counts_100">>,
        <<"set_actor_counts_95">>,
        <<"set_actor_counts_99">>,
        <<"set_actor_counts_mean">>,
        <<"set_actor_counts_median">>,
        <<"sidejob_version">>,
        <<"skipped_read_repairs">>,
        <<"skipped_read_repairs_total">>,
        <<"ssl_version">>,
        <<"stdlib_version">>,
        <<"storage_backend">>,
        <<"syntax_tools_version">>,
        <<"sys_driver_version">>,
        <<"sys_global_heaps_size">>,
        <<"sys_heap_type">>,
        <<"sys_logical_processors">>,
        <<"sys_monitor_count">>,
        <<"sys_otp_release">>,
        <<"sys_port_count">>,
        <<"sys_process_count">>,
        <<"sys_smp_support">>,
        <<"sys_system_architecture">>,
        <<"sys_system_version">>,
        <<"sys_thread_pool_size">>,
        <<"sys_threads_enabled">>,
        <<"sys_wordsize">>,
        <<"tictacaae_queue_microsec__max">>,
        <<"tictacaae_queue_microsec_mean">>,
        <<"vnode_counter_update">>,
        <<"vnode_counter_update_time_100">>,
        <<"vnode_counter_update_time_95">>,
        <<"vnode_counter_update_time_99">>,
        <<"vnode_counter_update_time_mean">>,
        <<"vnode_counter_update_time_median">>,
        <<"vnode_counter_update_total">>,
        <<"vnode_get_fsm_time_100">>,
        <<"vnode_get_fsm_time_95">>,
        <<"vnode_get_fsm_time_99">>,
        <<"vnode_get_fsm_time_mean">>,
        <<"vnode_get_fsm_time_median">>,
        <<"vnode_gets">>,
        <<"vnode_gets_total">>,
        <<"vnode_head_fsm_time_100">>,
        <<"vnode_head_fsm_time_95">>,
        <<"vnode_head_fsm_time_99">>,
        <<"vnode_head_fsm_time_mean">>,
        <<"vnode_head_fsm_time_median">>,
        <<"vnode_heads">>,
        <<"vnode_heads_total">>,
        <<"vnode_hll_update">>,
        <<"vnode_hll_update_time_100">>,
        <<"vnode_hll_update_time_95">>,
        <<"vnode_hll_update_time_99">>,
        <<"vnode_hll_update_time_mean">>,
        <<"vnode_hll_update_time_median">>,
        <<"vnode_hll_update_total">>,
        <<"vnode_index_deletes">>,
        <<"vnode_index_deletes_postings">>,
        <<"vnode_index_deletes_postings_total">>,
        <<"vnode_index_deletes_total">>,
        <<"vnode_index_reads">>,
        <<"vnode_index_reads_total">>,
        <<"vnode_index_refreshes">>,
        <<"vnode_index_refreshes_total">>,
        <<"vnode_index_writes">>,
        <<"vnode_index_writes_postings">>,
        <<"vnode_index_writes_postings_total">>,
        <<"vnode_index_writes_total">>,
        <<"vnode_map_update">>,
        <<"vnode_map_update_time_100">>,
        <<"vnode_map_update_time_95">>,
        <<"vnode_map_update_time_99">>,
        <<"vnode_map_update_time_mean">>,
        <<"vnode_map_update_time_median">>,
        <<"vnode_map_update_total">>,
        <<"vnode_put_fsm_time_100">>,
        <<"vnode_put_fsm_time_95">>,
        <<"vnode_put_fsm_time_99">>,
        <<"vnode_put_fsm_time_mean">>,
        <<"vnode_put_fsm_time_median">>,
        <<"vnode_puts">>,
        <<"vnode_puts_total">>,
        <<"vnode_set_update">>,
        <<"vnode_set_update_time_100">>,
        <<"vnode_set_update_time_95">>,
        <<"vnode_set_update_time_99">>,
        <<"vnode_set_update_time_mean">>,
        <<"vnode_set_update_time_median">>,
        <<"vnode_set_update_total">>,
        <<"webmachine_version">>,
        <<"write_once_merge">>,
        <<"write_once_put_objsize_100">>,
        <<"write_once_put_objsize_95">>,
        <<"write_once_put_objsize_99">>,
        <<"write_once_put_objsize_mean">>,
        <<"write_once_put_objsize_median">>,
        <<"write_once_put_time_100">>,
        <<"write_once_put_time_95">>,
        <<"write_once_put_time_99">>,
        <<"write_once_put_time_mean">>,
        <<"write_once_put_time_median">>,
        <<"write_once_puts">>,
        <<"write_once_puts_total">>,
        <<"xmerl_version">>,
        <<"yokozuna_version">>
    ] ++ pool_stats().

product_stats(riak_ee) ->
    [
        <<"ebloom_version">>,
        <<"mnesia_version">>,
        <<"ranch_version">>,
        <<"riak_jmx_version">>,
        <<"riak_repl_version">>,
        <<"riak_snmp_version">>,
        <<"snmp_version">>
    ];
product_stats(riak) ->
    [].

pool_stats() ->
    dscp_stats() ++
        [<<"node_worker_pool_node_worker_pool_total">>,
         <<"node_worker_pool_unregistered_total">>,
         <<"vnode_worker_pool_total">>].

dscp_stats() ->
    [<<"node_worker_pool_af1_pool_total">>,
     <<"node_worker_pool_af2_pool_total">>,
     <<"node_worker_pool_af3_pool_total">>,
     <<"node_worker_pool_af4_pool_total">>,
     <<"node_worker_pool_be_pool_total">>].

do_pools(Node) ->
    do_pools(Node, rpc:call(Node, riak_core_node_worker_pool, dscp_pools, [])).

do_pools(_Node, []) ->
    ok;
do_pools(Node, [Pool | Pools]) ->
    do_pool(Node, Pool),
    do_pools(Node, Pools).

do_pool(Node, Pool) ->
    WorkFun = fun() -> ok end,
    FinishFun = fun(ok) -> ok end,
    Work = {fold, WorkFun, FinishFun},
    Res = rpc:call(Node, riak_core_node_worker_pool, handle_work, [Pool, Work, undefined]),
    lager:info("Pool ~p returned ~p", [Pool, Res]).

inc_by_one(StatNames) ->
    inc_by(StatNames, 1).

inc_by(StatNames, Amt) ->
    [{StatName, Amt} || StatName <- StatNames].
