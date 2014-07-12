-module(rtperf).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

update_app_config(Node, Config) ->
    rtssh:update_app_config(Node, Config).

get_version() ->
    unknown.

get_deps() ->
    case rt_config:get(rt_deps, undefined) of
        undefined ->
            throw("Unable to determine Riak library path, rt_deps.");
        _ ->
            ok
    end,
    "deps".

harness_opts() ->

    %% Option Name, Short Code, Long Code, Argument Spec, Help Message
    [
     {test_name, undefined, "name", {string, "ad-hoc"},
      "name for this test"},
     {bin_size, undefined, "bin-size", {integer, 4096},
      "size of fixed binaries (median for non-fixed)"},
     {bin_type, undefined, "bin-type", {atom, fixed},
      "fixed | exponential"},
     {version, undefined, "version", {string, "develop"},
      "version to test"},
     {prepop, undefined, "prepop", {boolean, false},
      "prepopulate cluster"},
     {restart, undefined, "restart", {boolean, false},
      "stop running riak cluster and start new"},
     {cuttle, undefined, "cuttle", {boolean, true},
      "use cuttlefish config system"},
     {duration, undefined, "run-time", {integer, 1},
      "how long to run the test for"},
     {target_pct, undefined, "target-pct", {integer, 75},
      "target block cache to dataset size ratio"},
     {ram_size, undefined, "ram-size", {integer, 1024},
      "ram size of individual test nodes"}
    ].

setup_harness(Test, Args) ->
    lager:info("Harness setup with args: ~p", [Args]),
    case getopt:parse(harness_opts(), Args) of
    {ok, {Parsed, []}} ->
        _ = [rt_config:set(prefix(K), V)
         || {K, V} <- Parsed];
    _Huh ->
        getopt:usage(harness_opts(), escript:script_name()), halt(0)
    end,

    rtssh:setup_harness(Test, Args),
    ok.

prefix(Atom) ->
    list_to_atom("perf_"++atom_to_list(Atom)).

set_backend(Backend) ->
    rt_config:set(rt_backend, Backend).

get_backends() ->
    [riak_kv_bitcask_backend,
     riak_kv_eleveldb_backend,
     riak_kv_memory_backend].

run_test(Nodes, TestBenchConfig, BaseBenchConfig) ->
    Collectors = start_data_collectors(Nodes),

    TestName = test_name(),

    Base = maybe_start_base_load(BaseBenchConfig, Nodes, TestName),

    rt_bench:bench(TestBenchConfig, Nodes, TestName,
                   length(rt_config:get(perf_loadgens, [1]))),

    maybe_stop_base_load(Base),

    ok = stop_data_collectors(Collectors),

    ok = collect_test_data(Nodes, TestName).

teardown() ->
    ok.

cmd(Cmd) ->
    rtssh:cmd(Cmd).

stop_all(_Hosts) ->
    lager:info("called stop all, ignoring?").

start_data_collectors(Nodes) ->
    OSPid = os:getpid(),
    PrepDir = "/tmp/perf-"++OSPid,
    file:make_dir(PrepDir),
    {ok, Hostname} = inet:gethostname(),
    P = observer:watch(Nodes, {Hostname, 65001, PrepDir}),
    lager:info("started data collector: ~p", [P]),
    P.

stop_data_collectors(Collector) ->
    Collector ! stop,
    ok.

maybe_start_base_load([], _, _) ->
    none;
maybe_start_base_load(Config, Nodes, TestName) ->
    spawn(fun() ->
                rt_bench:bench(Config, Nodes, TestName++"_base",
                            length(rt_config:get(perf_loadgens, [1])))
    end).

maybe_stop_base_load(_) -> %% should be none, but benches aren't stoppable rn.
    ok.

%% need more sensible test names.
test_name() ->
    Vsn = rt_config:get(perf_version),
    BinSize = rt_config:get(perf_bin_size),
    rt_config:get(perf_test_name)++"-"++Vsn++"-"++
    integer_to_list(rt_config:get(perf_target_pct))++"pct-"++
        atom_to_list(rt_config:get(perf_bin_type))++"-"++
        integer_to_list(BinSize)++"b-"++date_string().

collect_test_data(Nodes, TestName) ->
    %% collect the files
    OSPid = os:getpid(),
    PrepDir = "/tmp/perf-"++OSPid,

    %% collect loadgen logs
    ok = rt_bench:collect_bench_data(TestName, PrepDir),

    %% collect node logs
    [begin
            rtssh:scp_from(rtssh:node_to_host(Node),
                           rtssh:node_path(Node) ++ "/log",
                           PrepDir++"/"++rtssh:node_to_host(Node))
     end
     || Node <- Nodes],

    %% no need to collect stats output, it's already in the prepdir
    rt:cmd("mv "++PrepDir++" results/"++TestName),

    %% really, really need to compress the results so they don't take
    %% up os damn much space
    ok.

maybe_prepop(Hosts, BinSize, SetSize) ->
   Vsn = rt_config:get(perf_version),
    case rt_config:get(perf_prepop) of
        true ->
            PPids = start_data_collectors(Hosts),
            PrepopName = rt_config:get(perf_test_name)++"-"++Vsn++
                "-prepop"++integer_to_list(BinSize)++"b-"++date_string(),

            lager:info("Target size = ~p", [SetSize]),

            PrepopConfig =
                        rt_bench:config(
                          max,
                          infinity,
                          Hosts,
                          {int_to_bin_bigendian, {partitioned_sequential_int, SetSize}},
                          rt_bench:valgen(rt_config:get(perf_bin_type), BinSize),
                          [{put,1}]),

            %% drop the cache
            rt_bench:bench(PrepopConfig, Hosts, PrepopName, 1, true),

            stop_data_collectors(PPids),
            collect_test_data(Hosts, PrepopName),
            timer:sleep(timer:minutes(1)+timer:seconds(30));
        false ->
            ok
    end.

date_string() ->
    {Mega, Sec, _Micro} = os:timestamp(),
    integer_to_list((Mega * 1000000) + Sec).


standard_config(NodeCount) ->
    standard_config(NodeCount, on).

standard_config(NodeCount, AAE) ->
    Backend = rt_config:get(rt_backend, undefined),
    Fish = rt_config:get(perf_cuttle, true),
    RingSize = rt:nearest_ringsize(NodeCount),
    mk_std_conf(Backend, Fish, RingSize, AAE).

mk_std_conf(riak_kv_memory_backend, false, Ring, AAE) ->
    [{riak_core,
      [{ring_creation_size, Ring*2},
       {handoff_concurrency, 16}]},
     {riak_kv, [{storage_backend, riak_kv_memory_backend},
                {anti_entropy,{AAE, []}},
                {memory_backend, []},
                {fsm_limit, 50000}
               ]}
    ];
mk_std_conf(riak_kv_memory_backend, true, Ring, AAE0) ->
    AAE = aae_cuttle(AAE0),
    {cuttlefish,
     [{ring_size, Ring*2},
      {transfer_limit, 16},
      {"erlang.distribution_buffer_size", "128MB"},
      {storage_backend, memory},
      {anti_entropy, AAE}
     ]};
mk_std_conf(riak_kv_eleveldb_backend, false, Ring, AAE) ->
    [{riak_core,
      [{handoff_concurrency, 16},
       {ring_creation_size, Ring}]},
     {riak_kv,
      [{storage_backend, riak_kv_eleveldb_backend},
       {anti_entropy,{AAE,[]}},
       {fsm_limit, undefined}]},
     {eleveldb,
      [{max_open_files, 500}]}
    ];
mk_std_conf(riak_kv_eleveldb_backend, true, Ring, AAE0) ->
    AAE = aae_cuttle(AAE0),
    {cuttlefish,
     [{ring_size, Ring},
      {transfer_limit, 16},
      {"erlang.distribution_buffer_size", "128MB"},
      {storage_backend, leveldb},
      {anti_entropy, AAE}
     ]};
mk_std_conf(_, false, Ring, AAE) ->
    [{riak_core,
      [{handoff_concurrency, 16},
       {ring_creation_size, Ring}]},
     {riak_kv,
      [{anti_entropy,{AAE, []}}]}
    ];
mk_std_conf(_, true, Ring, AAE0) ->
    AAE = aae_cuttle(AAE0),
    {cuttlefish,
     [{ring_size, Ring},
      {transfer_limit, 16},
      {"storage_backend", "bitcask"},
      {"erlang.distribution_buffer_size", "128MB"},
      {"bitcask.io_mode", nif},
      {anti_entropy, AAE}]}.

aae_cuttle(off) ->
    passive;
aae_cuttle(on) ->
    active.

target_size(Percentage, BinSize, RamSize, NodeCount) ->
    TotalRam = (RamSize * 1024 * 1024 * 1024) * NodeCount,
    CacheTarget = trunc((Percentage/100)*TotalRam),
    BinPlus = (BinSize + 300) * 3,
    %% hacky way of rounding up to the nearest 10k
    trunc((CacheTarget/(BinPlus*10000))+1)*10000.

deploy_clusters(ClusterConfigs) ->
    Clusters = rt_config:get(rtssh_clusters, []),
    NumConfig = length(ClusterConfigs),
    case length(Clusters) < NumConfig of
        true ->
            erlang:error("Requested more clusters than available");
        false ->
            Both = lists:zip(lists:sublist(Clusters, NumConfig), ClusterConfigs),
            Deploy =
                [begin
                     NumNodes = length(NodeConfig),
                     NumHosts = length(Hosts),
                     case NumNodes > NumHosts of
                         true ->
                             erlang:error("Not enough hosts available to deploy nodes",
                                          [NumNodes, NumHosts]);
                         false ->
                             Hosts2 = lists:sublist(Hosts, NumNodes),
                             {Hosts2, NodeConfig}
                     end
                 end || {{_,Hosts}, NodeConfig} <- Both],
            [deploy_nodes(NodeConfig, Hosts) || {Hosts, NodeConfig} <- Deploy]
    end.

deploy_nodes(NodeConfig) ->
    Hosts = rt_config:get(rtssh_hosts),
    NumNodes = length(NodeConfig),
    NumHosts = length(Hosts),
    case NumNodes > NumHosts of
        true ->
            erlang:error("Not enough hosts available to deploy nodes",
                         [NumNodes, NumHosts]);
        false ->
            Hosts2 = lists:sublist(Hosts, NumNodes),
            deploy_nodes(NodeConfig, Hosts2)
    end.

deploy_nodes(NodeConfig, Hosts) ->
    Path = rtssh:relpath(root),
    lager:info("Riak path: ~p", [Path]),
    Nodes = [rtssh:host_to_node(Host) || Host <- Hosts],
    HostMap = lists:zip(Nodes, Hosts),

    {Versions, Configs} = lists:unzip(NodeConfig),
    VersionMap = lists:zip(Nodes, Versions),

    rt_config:set(rt_hosts,
        orddict:from_list(
            orddict:to_list(rt_config:get(rt_hosts, orddict:new())) ++ HostMap)),
    rt_config:set(rt_versions,
        orddict:from_list(
            orddict:to_list(rt_config:get(rt_versions, orddict:new())) ++ VersionMap)),

    rt:pmap(fun({Node, _}) ->
                {ok,
                 {_, _, _, _, _, [IP0|_]}} = inet:gethostbyname(
                        rtssh:node_to_host(Node)),
                IP = inet:ntoa(IP0),
                Config = [{"listener.protobuf.internal",
                            IP++":10017"},
                           {"listener.http.internal",
                            IP++":10018"}],
                rtssh:set_conf(Node, Config)
        end, lists:zip(Nodes, Configs)),
    timer:sleep(500),

    rt:pmap(fun({_, default}) ->
                lager:info("Default configuration detected!"),
                ok;
               ({Node, {cuttlefish, Config}}) ->
                lager:info("Cuttlefish configuration detected!"),
                rtssh:set_conf(Node, Config);
               ({Node, Config}) ->
                lager:info("Legacy configuration detected!"),
                rtssh:update_app_config(Node, Config)
            end,
            lists:zip(Nodes, Configs)),
    timer:sleep(500),

    case rt_config:get(cuttle, true) of
        false ->
            rt:pmap(fun(Node) ->
                            Host = rtssh:get_host(Node),
                            Config = [{riak_api,
                                       [{pb, fun([{_, Port}]) ->
                                                     [{Host, Port}]
                                             end},
                                        {pb_ip, fun(_) ->
                                                        Host
                                                end}]},
                                      {riak_core,
                                       [{http, fun([{_, Port}]) ->
                                                       [{Host, Port}]
                                               end}]}],
                            rtssh:update_app_config(Node, Config)
                    end, Nodes),

            timer:sleep(500),

            rt:pmap(fun(Node) ->
                            rtssh:update_vm_args(Node,
                                                [{"-name", Node},
                                                 {"-zddbl", "65535"},
                                                 {"-P", "256000"}])
                    end, Nodes),

            timer:sleep(500);
        true -> ok
    end,

    rtssh:create_dirs(Nodes),

    rt:pmap(fun start/1, Nodes),

    %% Ensure nodes started
    [ok = rt:wait_until_pingable(N) || N <- Nodes],

    %% %% Enable debug logging
    %% [rpc:call(N, lager, set_loglevel, [lager_console_backend, debug]) || N <- Nodes],

    %% We have to make sure that riak_core_ring_manager is running before we can go on.
    [ok = rt:wait_until_registered(N, riak_core_ring_manager) || N <- Nodes],

    %% Ensure nodes are singleton clusters
    [ok = rt:check_singleton_node(N) || {N, Version} <- VersionMap,
                                        Version /= "0.14.2"],

    Nodes.

start(Node) ->
    rtssh:start(Node).
