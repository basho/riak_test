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
-behavior(riak_test).
-export([confirm/0]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-define(SPAM_BUCKET, <<"scotts_spam">>).
-define(MAX_LIST_KEYS_ATTEMPTS, 4).
-define(MAX_CLIENT_RECONNECT_ATTEMPTS, 100).
-define(CLIENT_RECONNECT_INTERVAL, 100).

%% @doc This test requires additional setup, here's how to do it.
%% 1. Clone and build basho_bench
%% 2. Set an environment variable "BASHO_BENCH" to the path you cloned to.
%% 3. Get this file: search-corpus/spam.0-1.tar.gz
%% 4. Unzip it somewhere.
%% 5. Set an environment variable "SPAM_DIR" to the path you unzipped, including the "spam.0" dir

confirm() ->
    rt:config_or_os_env(basho_bench),
    rt:config_or_os_env(spam_dir),
    verify_upgrade(),
    lager:info("Test ~p passed", [?MODULE]),
    pass.

verify_upgrade() ->

    TestMetaData = riak_test_runner:metadata(),
    %% Only run 2i for level
    Backend = proplists:get_value(backend, TestMetaData),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    Config = [{riak_search, [{enabled, true}]}],
    %% Uncomment to use settings more prone to cause races
    %% Config = [{riak_core, [{handoff_concurrency, 1024},
    %%                        {vnode_inactivity_timeout, 1000},
    %%                        {vnode_rolling_start, 128},
    %%                        {vnode_management_timer, 1000},
    %%                        {gossip_limit, {10000, 1000}}]},
    %%           {riak_search, [{enabled, true}]}],
    NumNodes = 4,
    Vsns = [{OldVsn, Config} || _ <- lists:seq(2,NumNodes)],
    Nodes = rt:build_cluster([{current, Config} | Vsns]),
    [Node1|OldNodes] = Nodes,
    lager:info("Writing 100 keys to ~p", [Node1]),
    rt:systest_write(Node1, 100, 3),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),

    lager:info("Checking list_keys count periodically throughout this test."),
    spawn_link(?MODULE, check_list_keys, [rt:pbc(Node1)]),

    Conns = rt:connection_info(Nodes),
    NodeConn = proplists:get_value(Node1, Conns),
    lager:info("NodeConn: ~p", [NodeConn]),

    KV1 = init_kv_tester(NodeConn),
    MR1 = init_mapred_tester(NodeConn),
    Search1 = init_search_tester(Nodes, Conns),
    
    TwoI1 = case Backend of
        eleveldb -> init_2i_tester(NodeConn);
        _ -> undefined
    end,
    [begin
         KV2 = spawn_kv_tester(KV1),
         MR2 = spawn_mapred_tester(MR1),
         TwoI2 = case TwoI1 of 
            undefined -> undefined;
            _ -> spawn_2i_tester(TwoI1)
         end,
         Search2 = spawn_search_tester(Search1),
         lager:info("Upgrading ~p", [Node]),
         rt:upgrade(Node, current),
         %% rt:slow_upgrade(Node, current, Nodes),
         _KV3 = check_kv_tester(KV2),
         _MR3 = check_mapred_tester(MR2),
         _TwoI3 = case TwoI1 of
            undefined -> undefined;
            _ -> check_2i_tester(TwoI2)
         end,
         _Search3 = check_search_tester(Search2, false),
         lager:info("Ensuring keys still exist"),
         rt:wait_for_cluster_service(Nodes, riak_kv),
         ?assertEqual([], rt:systest_read(Node1, 100, 1))
     end || Node <- OldNodes],
    lager:info("Upgrade complete, ensure search now passes"),
    check_search_tester(spawn_search_tester(Search1), true),
    ok.

%% ===================================================================
%% List Keys Tester
%% ===================================================================

check_list_keys(Pid) ->
    check_list_keys(Pid, 0).
check_list_keys(Pid, Attempt) ->
    case Attempt rem 20 of
        0 -> lager:debug("Performing list_keys check #~p", [Attempt]);
        _ -> nothing
    end,
    {ok, Keys} = list_keys(Pid, <<"systest">>),
    ?assertEqual(100, length(Keys)),
    timer:sleep(3000),
    check_list_keys(Pid, Attempt + 1).

%% List keys with time out recovery.
list_keys(Pid, Bucket) ->
    list_keys(Pid, Bucket, ?MAX_LIST_KEYS_ATTEMPTS,
              riakc_pb_socket:default_timeout(list_keys_timeout)).

list_keys(_, _, 0, _) ->
    {error, "list_keys timed out too many times"};
list_keys(Pid, Bucket, Attempts, TimeOut) ->
    Res = riakc_pb_socket:list_keys(Pid, Bucket, TimeOut),
    case Res of
        {error, Err} when Err =:= timeout; is_tuple(Err), element(1, Err) == timeout ->
            ?assertMatch(ok, wait_for_reconnect(Pid)),
            NewAttempts = Attempts - 1,
            NewTimeOut = TimeOut * 2,
            lager:info("List keys timed out, trying ~p more times, new time out = ~p",
                       [NewAttempts, NewTimeOut]),
            list_keys(Pid, Bucket, NewAttempts, NewTimeOut);
        _ -> Res
    end.


wait_for_reconnect(Pid) ->
    wait_for_reconnect(Pid, ?MAX_CLIENT_RECONNECT_ATTEMPTS, ?CLIENT_RECONNECT_INTERVAL).

wait_for_reconnect(Pid, 0, _) ->
    lager:error("Could not reconnect client ~p to Riak after timed out list keys", [Pid]),
    {error, pbc_client_reconnect_timed_out};
wait_for_reconnect(Pid, Attempts, Delay) ->
    timer:sleep(Delay),
    case riakc_pb_socket:is_connected(Pid) of
        true -> ok;
        _ -> wait_for_reconnect(Pid, Attempts-1, Delay)
    end.

%% ===================================================================
%% K/V Tester
%% ===================================================================

-record(kv, {buckets, runs}).

init_kv_tester(Conn) ->
    {Host, Port} = proplists:get_value(http, Conn),
    Buckets = [{<<"utest">>, []}],
    {BucketNames, _} = lists:unzip(Buckets),
    generate_kv_scripts(BucketNames, Host, Port),
    [kv_populate(Bucket) || Bucket <- BucketNames],
    #kv{buckets=BucketNames, runs=[]}.

spawn_kv_tester(KV=#kv{buckets=Buckets}) ->
    Count = 3,
    Runs = [{Bucket, kv_spawn_verify(Bucket)} || Bucket <- Buckets,
                                                 _ <- lists:seq(1,Count)],
    KV#kv{runs=Runs}.

check_kv_tester(KV=#kv{runs=Runs}) ->
    Failed = [Bucket || {Bucket, Run} <- Runs,
                        ok /= kv_check_verify(Bucket, Run, [])],
    [begin
         lager:info("Failed k/v test for: ~p", [Bucket]),
         lager:info("Re-running until test passes to check for data loss"),
         Result =
             rt:wait_until(node(),
                           fun(_) ->
                                   Rerun = kv_spawn_verify(Bucket),
                                   ok == kv_check_verify(Bucket, Rerun, [])
                           end),
         ?assertEqual(ok, Result),
         lager:info("k/v test finally passed"),
         ok
     end || Bucket <- Failed],
    KV#kv{runs=[]}.

kv_populate(Bucket) when is_binary(Bucket) ->
    kv_populate(binary_to_list(Bucket));
kv_populate(Bucket) ->
    Config = "bb-populate-" ++ Bucket ++ ".config",
    lager:info("Populating bucket ~s", [Bucket]),
    Cmd = "$BASHO_BENCH/basho_bench " ++ Config,
    ?assertMatch({0,_}, rt:cmd(Cmd, [{cd, rt:config(rt_scratch_dir)}, {env, [
        {"BASHO_BENCH", rt:config(basho_bench)},
        {"SPAM_DIR", rt:config(spam_dir)}
    ]}])),
    ok.

kv_spawn_verify(Bucket) when is_binary(Bucket) ->
    kv_spawn_verify(binary_to_list(Bucket));
kv_spawn_verify(Bucket) ->
    Config = "bb-verify-" ++ Bucket ++ ".config",
    lager:info("Spawning k/v test against: ~s", [Bucket]),
    Cmd = "$BASHO_BENCH/basho_bench " ++ Config,
    rt:spawn_cmd(Cmd, [{cd, rt:config(rt_scratch_dir)}, {env, [
        {"BASHO_BENCH", rt:config(basho_bench)},
        {"SPAM_DIR", rt:config(spam_dir)}
    ]}]).

kv_check_verify(Bucket, Port, Opts) ->
    lager:info("Checking k/v test against: ~p", [Bucket]),
    {Status,_} = rt:wait_for_cmd(Port),
    Repair = ordsets:is_element(repair, Opts),
    case {Repair, Status} of
        %% {true, 1} ->
        %%     lager:info("Allowing repair: ~p", [Bucket]),
        %%     kv_verify_repair(Bucket);
        {_, 0} ->
            ok;
        {_, _} ->
            fail
    end.

kv_spawn_repair(Bucket) when is_binary(Bucket) ->
    kv_spawn_repair(binary_to_list(Bucket));
kv_spawn_repair(Bucket) ->
    Config = "bb-repair-" ++ Bucket ++ ".config",
    lager:info("Read-repairing bucket ~s", [Bucket]),
    Cmd = "$BASHO_BENCH/basho_bench " ++ Config,
    rt:spawn_cmd(Cmd, [{cd, rt:config(rt_scratch_dir)}, {env, [
        {"BASHO_BENCH", rt:config(basho_bench)},
        {"SPAM_DIR", rt:config(spam_dir)}
    ]}]).

%% ===================================================================
%% map/reduce Tester
%% ===================================================================

-record(mr, {runs}).

init_mapred_tester(Conn) ->
    {Host, Port} = proplists:get_value(pb, Conn),
    generate_mapred_scripts(Host, Port),
    mapred_populate(),
    #mr{runs=[]}.

spawn_mapred_tester(MR) ->
    Count = 3,
    Runs = [mapred_spawn_verify() || _ <- lists:seq(1,Count)],
    MR#mr{runs=Runs}.

check_mapred_tester(MR=#mr{runs=Runs}) ->
    Failed = [failed || Run <- Runs,
                        ok /= mapred_check_verify(Run)],
    [begin
         lager:info("Failed mapred test"),
         lager:info("Re-running until test passes to check for data loss"),
         Result =
             rt:wait_until(node(),
                           fun(_) ->
                                   Rerun = mapred_spawn_verify(),
                                   ok == mapred_check_verify(Rerun)
                           end),
         ?assertEqual(ok, Result),
         lager:info("map/reduce test finally passed"),
         ok
     end || _ <- Failed],
    MR#mr{runs=[]}.

mapred_populate() ->
    Config = "bb-populate-mapred.config",
    lager:info("Populating map-reduce bucket"),
    Cmd = "$BASHO_BENCH/basho_bench " ++ Config,
    ?assertMatch({0,_}, rt:cmd(Cmd, [{cd, rt:config(rt_scratch_dir)}, {env, [
        {"BASHO_BENCH", rt:config(basho_bench)},
        {"SPAM_DIR", rt:config(spam_dir)}
    ]}])),
    ok.

mapred_spawn_verify() ->
    Config = "bb-verify-mapred.config",
    lager:info("Spawning map/reduce test"),
    rt:spawn_cmd("$BASHO_BENCH/basho_bench " ++ Config, [{cd, rt:config(rt_scratch_dir)}, {env, [
        {"BASHO_BENCH", rt:config(basho_bench)},
        {"SPAM_DIR", rt:config(spam_dir)}
    ]}]).

mapred_check_verify(Port) ->
    lager:info("Checking map/reduce test"),
    case rt:wait_for_cmd(Port) of
        {0,_} ->
            ok;
        _ ->
            fail
    end.

%% ===================================================================
%% Search tester
%% ===================================================================

-record(search, {buckets, runs}).

init_search_tester(Nodes, Conns) ->
    SpamDir = rt:config(spam_dir),
    IPs = [proplists:get_value(http, I) || {_, I} <- Conns],
    Buckets = [?SPAM_BUCKET],
    rt:enable_search_hook(hd(Nodes), ?SPAM_BUCKET),
    generate_search_scripts(Buckets, IPs, SpamDir),
    [search_populate(Bucket) || Bucket <- Buckets],
    %% Check search queries actually work as expected
    [check_search(Bucket, Nodes) || Bucket <- Buckets],
    #search{buckets=Buckets, runs=[]}.

check_search(?SPAM_BUCKET, Nodes) ->
    SearchResults = [{"mx.example.net", 187},
                     {"ZiaSun", 1},
                     {"headaches", 4},
                     {"YALSP", 3},
                     {"mister", 0},
                     {"prohibiting", 5}],
    Results = [{Term,Count} || {Term, Count} <- SearchResults,
                               Node <- Nodes,
                               {Count2,_} <- [rpc:call(Node, search, search, [?SPAM_BUCKET, Term])],
                               Count2 == Count],
    Expected = lists:usort(SearchResults),
    Actual = lists:usort(Results),
    ?assertEqual(Expected, Actual),
    ok.

spawn_search_tester(Search=#search{buckets=Buckets}) ->
    Count = 3,
    Runs = [{Bucket, search_spawn_verify(Bucket)} || Bucket <- Buckets,
                                                     _ <- lists:seq(1, Count)],
    Search#search{runs=Runs}.

check_search_tester(Search=#search{runs=Runs}, Retest) ->
    Failed = [Bucket || {Bucket, Run} <- Runs,
                        ok /= search_check_verify(Bucket, Run, [])],
    [begin
         lager:info("Failed search test for: ~p", [Bucket]),
         maybe_retest_search(Retest, Bucket),
         ok
     end || Bucket <- Failed],
    Search#search{runs=[]}.

maybe_retest_search(false, _) ->
    ok;
maybe_retest_search(true, Bucket) ->
    lager:info("Re-running until test passes to check for data loss"),
    Result =
        rt:wait_until(node(),
                      fun(_) ->
                              Rerun = search_spawn_verify(Bucket),
                              ok == search_check_verify(Bucket, Rerun, [])
                      end),
    ?assertEqual(ok, Result),
    lager:info("search test finally passed"),
    ok.

search_populate(Bucket) when is_binary(Bucket) ->
    search_populate(binary_to_list(Bucket));
search_populate(Bucket) ->
    Config = "bb-populate-" ++ Bucket ++ ".config",
    lager:info("Populating search bucket: ~s", [Bucket]),
    Cmd = "$BASHO_BENCH/basho_bench " ++ Config,
    rt:cmd(Cmd, [{cd, rt:config(rt_scratch_dir)}, {env, [
        {"BASHO_BENCH", rt:config(basho_bench)},
        {"SPAM_DIR", rt:config(spam_dir)}
    ]}]).

search_spawn_verify(Bucket) when is_binary(Bucket) ->
    search_spawn_verify(binary_to_list(Bucket));
search_spawn_verify(Bucket) when is_list(Bucket) ->
    Config = "bb-verify-" ++ Bucket ++ ".config",
    lager:info("Spawning search test against: ~s", [Bucket]),
    Cmd = "$BASHO_BENCH/basho_bench " ++ Config,
    rt:spawn_cmd(Cmd, [{cd, rt:config(rt_scratch_dir)}, {env, [
        {"BASHO_BENCH", rt:config(basho_bench)},
        {"SPAM_DIR", rt:config(spam_dir)}
    ]}]).

search_check_verify(Bucket, Port, Opts) ->
    lager:info("Checking search test against: ~p", [Bucket]),
    {Status,_} = rt:wait_for_cmd(Port),
    Repair = ordsets:is_element(repair, Opts),
    case {Repair, Status} of
        %% {true, 1} ->
        %%     lager:info("Allowing repair: ~p", [Bucket]),
        %%     search_verify_repair(Bucket);
        {_, 0} ->
            ok;
        {_, _} ->
            fail
    end.

%% ===================================================================
%% 2i Tester
%% ===================================================================

-record(twoi, {runs}).

init_2i_tester(Conn) ->
    {PBHost, PBPort} = proplists:get_value(pb, Conn),
    {HTTPHost, HTTPPort} = proplists:get_value(http, Conn),
    generate_2i_scripts(<<"2ibuquot">>, [{PBHost, PBPort}], [{HTTPHost, HTTPPort}]),
    twoi_populate(),
    #twoi{runs=[]}.

spawn_2i_tester(TwoI) ->
    Count = 3,
    Runs = [twoi_spawn_verify() || _ <- lists:seq(1,Count)],
    TwoI#twoi{runs=Runs}.

check_2i_tester(TwoI=#twoi{runs=Runs}) ->
    Failed = [failed || Run <- Runs,
                        ok /= twoi_check_verify(Run)],
    [begin
         lager:info("Failed 2i test"),
         lager:info("Re-running until test passes to check for data loss"),
         Result =
             rt:wait_until(node(),
                           fun(_) ->
                                   Rerun = twoi_spawn_verify(),
                                   ok == twoi_check_verify(Rerun)
                           end),
         ?assertEqual(ok, Result),
         lager:info("2i test finally passed"),
         ok
     end || _ <- Failed],
    TwoI#twoi{runs=[]}.

twoi_populate() ->
    Config = "bb-populate-2i.config",
    lager:info("Populating 2i bucket"),
    Cmd = "$BASHO_BENCH/basho_bench " ++ Config,
    ?assertMatch({0,_}, rt:cmd(Cmd, [{cd, rt:config(rt_scratch_dir)}, {env, [
        {"BASHO_BENCH", rt:config(basho_bench)}
    ]}])),
    ok.

twoi_spawn_verify() ->
    Config = "bb-verify-2i.config",
    lager:info("Spawning 2i test"),
    rt:spawn_cmd("$BASHO_BENCH/basho_bench " ++ Config, [{cd, rt:config(rt_scratch_dir)}, {env, [
        {"BASHO_BENCH", rt:config(basho_bench)}
    ]}]).

twoi_check_verify(Port) ->
    lager:info("Checking 2i test"),
    case rt:wait_for_cmd(Port) of
        {0,_} ->
            ok;
        _ ->
            fail
    end.


%% ===================================================================
%% basho_bench K/V scripts
%% ===================================================================

generate_kv_scripts(Buckets, Host, Port) ->
    [begin
         Bucket = binary_to_list(BucketBin),
         kv_populate_script(Bucket, Host, Port),
         kv_verify_script(Bucket, Host, Port),
         kv_repair_script(Bucket, Host, Port)
     end || BucketBin <- Buckets],
    ok.

kv_populate_script(Bucket, Host, Port) ->
    Cfg = [{mode, max},
           {duration, infinity},
           {concurrent, 16},
           {driver, basho_bench_driver_http_raw},
           {key_generator, {partitioned_sequential_int, 0, 8000}},
           {value_generator, {uniform_bin,100,1000}},
           {operations, [{update, 1}]},
           {http_raw_ips, [Host]},
           {http_raw_port, Port},
           {http_raw_path, "/riak/" ++ Bucket}],
    Config = filename:join([rt:config(rt_scratch_dir), io_lib:format("bb-populate-~s.config", [Bucket])]),
    write_terms(Config, Cfg),
    ok.

kv_verify_script(Bucket, Host, Port) ->
    Cfg = [{mode, {rate, 50}},
           %%{duration, infinity},
           {duration, 1},
           {concurrent, 10},
           {driver, basho_bench_driver_http_raw},
           {key_generator, {uniform_int, 7999}},
           {value_generator, {uniform_bin,100,1000}},
           {operations, [{update, 1},{get_existing, 1}]},
           {http_raw_ips, [Host]},
           {http_raw_port, Port},
           {http_raw_path, "/riak/" ++ Bucket},
           {shutdown_on_error, true}],
    Config = filename:join([rt:config(rt_scratch_dir), io_lib:format("bb-verify-~s.config", [Bucket])]),
    write_terms(Config, Cfg),
    ok.

kv_repair_script(Bucket, Host, Port) ->
    Cfg = [{mode, {rate, 50}},
           {duration, infinity},
           %%{duration, 1},
           {concurrent, 10},
           {driver, basho_bench_driver_http_raw},
           %%{key_generator, {uniform_int, 8000}},
           {key_generator, {partitioned_sequential_int, 0, 8000}},
           {value_generator, {uniform_bin,100,1000}},
           {operations, [{get, 1}]},
           {http_raw_ips, [Host]},
           {http_raw_port, Port},
           {http_raw_path, "/riak/" ++ Bucket}],
    Config = filename:join([rt:config(rt_scratch_dir), io_lib:format("bb-repair-~s.config", [Bucket])]),
    write_terms(Config, Cfg),
    ok.

%% ===================================================================
%% basho_bench map/reduce scripts
%% ===================================================================

generate_mapred_scripts(Host, Port) ->
    mapred_populate_script(Host, Port),
    mapred_verify_script(Host, Port),
    ok.

mapred_populate_script(Host, Port) ->
    Cfg = [{driver, basho_bench_driver_riakc_pb},
           {riakc_pb_ips, [{Host, Port}]},
           {riakc_pb_replies, 1},
           {riakc_pb_bucket, <<"bryanitbs">>},
           {mode, max},
           {duration, 10000},
           {concurrent, 1},
           {operations, [{put, 1}]},
           {key_generator, {int_to_str, {sequential_int, 10000}}},
           {value_generator,
            {function, basho_bench_driver_riakc_pb, mapred_ordered_valgen, []}}],
    Config = filename:join([rt:config(rt_scratch_dir), "bb-populate-mapred.config"]),
    write_terms(Config, Cfg),
    ok.

mapred_verify_script(Host, Port) ->
    Cfg = [{driver, basho_bench_driver_riakc_pb},
           {riakc_pb_ips, [{Host, Port}]},
           {riakc_pb_replies, 1},
           {riakc_pb_bucket, <<"bryanitbs">>},
           {riakc_pb_preloaded_keys, 10000},
           {mode, max},
           {duration, 1},
           {concurrent, 1},
           {operations, [{mr_bucket_erlang, 1}]},
           {key_generator, {int_to_str, {uniform_int, 9999}}},
           {value_generator, {fixed_bin, 1}},
           {riakc_pb_keylist_length, 1000},
           {shutdown_on_error, true}],
    Config = filename:join([rt:config(rt_scratch_dir), "bb-verify-mapred.config"]),
    write_terms(Config, Cfg),
    ok.

%% ===================================================================
%% basho_bench Search scritps
%% ===================================================================

generate_search_scripts(Buckets, IPs, SpamDir) ->
    [begin
         Bucket = binary_to_list(BucketBin),
         search_populate_script(Bucket, IPs, SpamDir),
         search_verify_script(Bucket, IPs)
     end || BucketBin <- Buckets],
    ok.

search_populate_script(Bucket, IPs, SpamDir) ->
    Cfg = [{mode, max},
           {duration, 1},
           {concurrent, 10},
           {driver, basho_bench_driver_http_raw},
           {file_dir, SpamDir},
           {operations, [{put_file,1}]},
           {http_raw_ips, IPs},
           {http_raw_path, "/riak/" ++ Bucket},
           {shutdown_on_error, true}],
    Config = filename:join([rt:config(rt_scratch_dir), io_lib:format("bb-populate-~s.config", [Bucket])]),
    write_terms(Config, Cfg).

search_verify_script(Bucket, IPs) ->
    Expect = [{"mx.example.net", 187},
              {"ZiaSun", 1},
              {"headaches", 4},
              {"YALSP", 3},
              {"mister", 0},
              {"prohibiting", 5}],
    Operations = [{{search,E},1} || E <- Expect],
    Cfg = [{mode, max},
           {duration, 1},
           {concurrent, 10},
           {driver, basho_bench_driver_http_raw},
           {operations, Operations},
           {http_raw_ips, IPs},
           {http_solr_path, "/solr/" ++ Bucket},
           {http_raw_path, "/riak/" ++ Bucket},
           {shutdown_on_error, true}],
    Config = filename:join([rt:config(rt_scratch_dir), io_lib:format("bb-verify-~s.config", [Bucket])]),
    write_terms(Config, Cfg).

%% ===================================================================
%% basho_bench 2i scritps
%% ===================================================================
generate_2i_scripts(Bucket, PBIPs, HTTPIPs) ->
    twoi_populate_script(Bucket, PBIPs, HTTPIPs),
    twoi_verify_script(Bucket, PBIPs, HTTPIPs),
    ok.

twoi_populate_script(Bucket, PBIPs, HTTPIPs) ->
    Cfg = [ {driver, basho_bench_driver_2i},
            {operations, [{{put_pb, 5}, 1}]},
            {mode, max},
            {duration, 10000},
            {concurrent, 1},
            {key_generator, {sequential_int, 10000}},
            {value_generator, {fixed_bin, 10000}},
            {riakc_pb_bucket, Bucket},
            {pb_ips, PBIPs}, 
            {pb_replies, 1},
            {http_ips, HTTPIPs}],
    Config = filename:join([rt:config(rt_scratch_dir), "bb-populate-2i.config"]),
    write_terms(Config, Cfg),
    ok.

twoi_verify_script(Bucket, PBIPs, HTTPIPs) ->
    Cfg = [ {driver, basho_bench_driver_2i},
            {operations, [
                {{query_http, 10}, 1},
                {{query_mr,   10}, 1},
                {{query_pb,   10}, 1}
            ]},
            {mode, max},
            {duration, 1},
            {concurrent, 3},
            {key_generator, {uniform_int, 10000}},
            {value_generator, {fixed_bin, 10000}},
            {riakc_pb_bucket, Bucket},
            {pb_ips, PBIPs},
            {pb_replies, 1},
            {http_ips, HTTPIPs},
            {enforce_keyrange, 10000}],
    Config = filename:join([rt:config(rt_scratch_dir), "bb-verify-2i.config"]),
    write_terms(Config, Cfg),
    ok.

write_terms(File, Terms) ->
    {ok, IO} = file:open(File, [write]),
    [io:fwrite(IO, "~p.~n", [T]) || T <- Terms],
    file:close(IO).
