-module(loaded_upgrade).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-define(SPAM_BUCKET, <<"scotts_spam">>).

loaded_upgrade() ->
    _ = rt:get_os_env("BASHO_BENCH"),
    %% OldVsns = ["1.0.3", "1.1.4"],
    OldVsns = ["1.1.4"],
    [verify_upgrade(OldVsn) || OldVsn <- OldVsns],
    lager:info("Test ~p passed", [?MODULE]),
    ok.

verify_upgrade(OldVsn) ->
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

    Conns = rt:connection_info(Nodes),
    NodeConn = proplists:get_value(Node1, Conns),

    KV1 = init_kv_tester(NodeConn),
    MR1 = init_mapred_tester(NodeConn),
    Search1 = init_search_tester(Nodes, Conns),

    [begin
         KV2 = spawn_kv_tester(KV1),
         MR2 = spawn_mapred_tester(MR1),
         Search2 = spawn_search_tester(Search1),
         lager:info("Upgrading ~p", [Node]),
         rt:upgrade(Node, current),
         %% rt:slow_upgrade(Node, current, Nodes),
         _KV3 = check_kv_tester(KV2),
         _MR3 = check_mapred_tester(MR2),
         _Search3 = check_search_tester(Search2, false),
         lager:info("Ensuring keys still exist"),
         rt:wait_for_cluster_service(Nodes, riak_kv),
         ?assertEqual([], rt:systest_read(Node1, 100, 1))
     end || Node <- OldNodes],
    lager:info("Upgrade complete, ensure search now passes"),
    check_search_tester(spawn_search_tester(Search1), true),
    ok.

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
    ?assertMatch({0,_}, rt:cmd("$BASHO_BENCH/basho_bench " ++ Config)),
    ok.

kv_spawn_verify(Bucket) when is_binary(Bucket) ->
    kv_spawn_verify(binary_to_list(Bucket));
kv_spawn_verify(Bucket) ->
    Config = "bb-verify-" ++ Bucket ++ ".config",
    lager:info("Spawning k/v test against: ~s", [Bucket]),
    rt:spawn_cmd("$BASHO_BENCH/basho_bench " ++ Config).

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
    rt:spawn_cmd("$BASHO_BENCH/basho_bench " ++ Config).

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
    ?assertMatch({0,_}, rt:cmd("$BASHO_BENCH/basho_bench " ++ Config)),
    ok.

mapred_spawn_verify() ->
    Config = "bb-verify-mapred.config",
    lager:info("Spawning map/reduce test"),
    rt:spawn_cmd("$BASHO_BENCH/basho_bench " ++ Config).

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
    SpamDir = rt:get_os_env("SPAM_DIR"),
    IPs = [proplists:get_value(http, I) || {_, I} <- Conns],
    Buckets = [?SPAM_BUCKET],
    rt:enable_search_hook(hd(Nodes), ?SPAM_BUCKET),
    generate_search_scripts(Buckets, IPs, SpamDir),
    [search_populate(Bucket) || Bucket <- Buckets],
    %% Check search queries actually work as expected
    [check_search(Bucket, Nodes) || Bucket <- Buckets],
    #search{buckets=Buckets, runs=[]}.

check_search(?SPAM_BUCKET, Nodes) ->
    SearchResults = [{"postoffice.mr.net", 194},
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
    rt:cmd("$BASHO_BENCH/basho_bench " ++ Config).

search_spawn_verify(Bucket) when is_binary(Bucket) ->
    search_spawn_verify(binary_to_list(Bucket));
search_spawn_verify(Bucket) when is_list(Bucket) ->
    Config = "bb-verify-" ++ Bucket ++ ".config",
    lager:info("Spawning search test against: ~s", [Bucket]),
    rt:spawn_cmd("$BASHO_BENCH/basho_bench " ++ Config).

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
    Config = "bb-populate-" ++ Bucket ++ ".config",
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
    Config = "bb-verify-" ++ Bucket ++ ".config",
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
    Config = "bb-repair-" ++ Bucket ++ ".config",
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

    Config = "bb-populate-mapred.config",
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
    Config = "bb-verify-mapred.config",
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

    Config = "bb-populate-" ++ Bucket ++ ".config",
    write_terms(Config, Cfg).

search_verify_script(Bucket, IPs) ->
    Expect = [{"postoffice.mr.net", 194},
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

    Config = "bb-verify-" ++ Bucket ++ ".config",
    write_terms(Config, Cfg).

write_terms(File, Terms) ->
    {ok, IO} = file:open(File, [write]),
    [io:fwrite(IO, "~p.~n", [T]) || T <- Terms],
    file:close(IO).
