-module(loaded_upgrade).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

loaded_upgrade() ->
    (os:getenv("BASHO_BENCH") /= false) orelse
        throw("Missing path to BASHO_BENCH enviroment variable"),
    %% OldVsns = ["1.0.3", "1.1.4"],
    OldVsns = ["1.1.4"],
    [verify_upgrade(OldVsn) || OldVsn <- OldVsns],
    lager:info("Test ~p passed", [?MODULE]),
    ok.

verify_upgrade(OldVsn) ->
    NumNodes = 4,
    Vsns = [OldVsn || _ <- lists:seq(2,NumNodes)],
    Nodes = rt:build_cluster([current | Vsns]),
    [Node1|OldNodes] = Nodes,
    lager:info("Writing 100 keys to ~p", [Node1]),
    rt:systest_write(Node1, 100, 3),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),

    Conns = rt:connection_info(Nodes),
    NodeConn = proplists:get_value(Node1, Conns),

    KV1 = init_kv_tester(NodeConn),
    MR1 = init_mapred_tester(NodeConn),

    [begin
         KV2 = spawn_kv_tester(KV1),
         MR2 = spawn_mapred_tester(MR1),
         lager:info("Upgrading ~p", [Node]),
         rt:upgrade(Node, current),
         _KV3 = check_kv_tester(KV2),
         _MR3 = check_mapred_tester(MR2),
         lager:info("Ensuring keys still exist"),
         rt:wait_for_cluster_service(Nodes, riak_kv),
         ?assertEqual([], rt:systest_read(Node1, 100, 1))
     end || Node <- OldNodes],
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
    Out = io_lib:format("
{mode, max}.
{duration, infinity}.
{concurrent, 16}.
{driver, basho_bench_driver_http_raw}.
{key_generator, {partitioned_sequential_int, 0, 8000}}.
{value_generator, {uniform_bin,100,1000}}.
{operations, [{update, 1}]}.
{http_raw_ips, [\"~s\"]}.
{http_raw_port, ~b}.
{http_raw_path, \"/riak/~s\"}.", [Host, Port, Bucket]),
    Config = "bb-populate-" ++ Bucket ++ ".config",
    file:write_file(Config, Out),
    ok.

kv_verify_script(Bucket, Host, Port) ->
    Out = io_lib:format("
{mode, {rate, 50}}.
%{duration, infinity}.
{duration, 1}.
{concurrent, 10}.
{driver, basho_bench_driver_http_raw}.
{key_generator, {uniform_int, 7999}}.
{value_generator, {uniform_bin,100,1000}}.
{operations, [{update, 1},{get_existing, 1}]}.
{http_raw_ips, [\"~s\"]}.
{http_raw_port, ~b}.
{http_raw_path, \"/riak/~s\"}.
{shutdown_on_error, true}.", [Host, Port, Bucket]),
    Config = "bb-verify-" ++ Bucket ++ ".config",
    file:write_file(Config, Out),
    ok.

kv_repair_script(Bucket, Host, Port) ->
    Out = io_lib:format("
{mode, {rate, 50}}.
{duration, infinity}.
%{duration, 1}.
{concurrent, 10}.
{driver, basho_bench_driver_http_raw}.
%{key_generator, {uniform_int, 8000}}.
{key_generator, {partitioned_sequential_int, 0, 8000}}.
{value_generator, {uniform_bin,100,1000}}.
{operations, [{get, 1}]}.
{http_raw_ips, [\"~s\"]}.
{http_raw_port, ~b}.
{http_raw_path, \"/riak/~s\"}.", [Host, Port, Bucket]),
    Config = "bb-repair-" ++ Bucket ++ ".config",
    file:write_file(Config, Out),
    ok.

%% ===================================================================
%% basho_bench map/reduce scripts
%% ===================================================================

generate_mapred_scripts(Host, Port) ->
    mapred_populate_script(Host, Port),
    mapred_verify_script(Host, Port),
    ok.

mapred_populate_script(Host, Port) ->
    Out = io_lib:format("
{driver, basho_bench_driver_riakc_pb}.
%{code_paths, [\"deps/stats\",
%              \"deps/riakc\",
%              \"deps/protobuffs\"]}.
{riakc_pb_ips, [{~p, ~b}]}.
{riakc_pb_replies, 1}.
{riakc_pb_bucket, <<\"bryanitbs\">>}.
%% load
{mode, max}.
{duration, 10000}.
{concurrent, 1}.
{operations, [{put, 1}]}.
{key_generator, {int_to_str, {sequential_int, 10000}}}.
{value_generator,
 {function, basho_bench_driver_riakc_pb, mapred_ordered_valgen, []}}.",
    [Host, Port]),
    Config = "bb-populate-mapred.config",
    file:write_file(Config, Out),
    ok.

mapred_verify_script(Host, Port) ->
    Out = io_lib:format("
{driver, basho_bench_driver_riakc_pb}.
%{code_paths, [\"deps/stats\",
%              \"deps/riakc\",
%              \"deps/protobuffs\"]}.
{riakc_pb_ips, [{~p, ~b}]}.
{riakc_pb_replies, 1}.
{riakc_pb_bucket, <<\"bryanitbs\">>}.
%% test
%% for computing expected bucket sum
{riakc_pb_preloaded_keys, 10000}.
{mode, max}.
{duration, 1}.
{concurrent, 1}.
{operations, [{mr_bucket_erlang, 1}]}.
{key_generator, {int_to_str, {uniform_int, 9999}}}.
{value_generator, {fixed_bin, 1}}.
{riakc_pb_keylist_length, 1000}.
{shutdown_on_error, true}.", [Host, Port]),
    Config = "bb-verify-mapred.config",
    file:write_file(Config, Out),
    ok.
