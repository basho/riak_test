-module(rtperf).
-compile(export_all).

run_test(HostList, TestBenchConfig, BaseBenchConfig) ->
    Collectors = start_data_collectors(HostList),
    
    TestName = test_name(),
        
    Base = maybe_start_base_load(BaseBenchConfig),

    rt_bench:bench(TestBenchConfig, HostList, TestName, 
                   rt_config:get(perf_loadgens)),

    maybe_stop_base_load(Base),
    
    ok = stop_data_collectors(Collectors),
    
    ok = collect_test_data(HostList, TestName).

build_cluster(Config) ->
    Vsn = rt_config:get(perf_version),
    HostList = rt_config:get(rt_hostnames),
    Count = length(HostList),
    
    %% make sure that all of the remote nodes have a clean build at
    %% the remote location
    Force = rt_config:get(perf_force_build, false),
    case rt_config:get(perf_restart, meh) of
        true ->
            case rtssh:ensure_remote_build(HostList, Vsn, Force) of
                ok -> ok;
                Else ->
                    lager:error("Got unexpected return ~p from deploy, stopping",
                                [Else]),
                    error(deploy_error)
            end;
        _ -> ok
    end,

    Nodes =
        case rt_config:get(perf_restart) of
            true ->
                rt:build_cluster(Count,
                                 lists:duplicate(Count, {Vsn, Config}),
                                 whatever);
            false ->
                [list_to_atom("riak@" ++ Host) || Host <- HostList]
        end,

    Me = self(),
    spawn(fun() -> 
                  ok = rt:wait_until_nodes_ready(Nodes),
                  ok = rt:wait_until_ring_converged(Nodes),
                  ok = rt:wait_until_transfers_complete(Nodes),
                  Me ! done
          end),
    receive
        done -> ok
    after timer:minutes(10) ->
            lager:error("Cluster setup is taking too long, stopping"),
            error(cluster_setup_timeout)
    end.

start_data_collectors(Hosts) ->
    %% should probably only start this once?
    inets:start(),

    OSPid = os:getpid(),
    PrepDir = "/tmp/perf-"++OSPid,
    file:make_dir(PrepDir),

    Cmd = "python ./bin/dstat -cdngyimrs --vm --fs --socket --tcp --disk-util "++
        "--output "++"/tmp/dstat-"++os:getpid(),

    file:write(PrepDir++"/START", io_lib:format("~w.~n", [calendar:local_time()])),

    [spawn(rtssh, ssh_cmd, [Host, Cmd]) || Host <- Hosts] ++
    [spawn(?MODULE, poll_stats, [Host]) || Host <- Hosts].

poll_stats(Host) ->

    case httpc:request("http://"++Host++":8098/stats/") of
        {ok, {{_Version, 200, _ReasonPhrase}, _Headers, Body}} ->

            Stats = mochijson2:decode(Body),

            OSPid = os:getpid(),
            PrepDir = "/tmp/perf-"++OSPid,

            {ok, Fd} = file:open(PrepDir++"/rstats-"++Host, [append]),
            file:write(Fd, io_lib:format("~w.~n", [calendar:local_time()])),
            file:write(Fd, io_lib:format("~p.~n", [Stats])),
            file:close(Fd),

            timer:sleep(60000);
        _Else ->
            %% good to know, but turns out that this is just annoying
            %%lager:error("Web stat collector failed with: ~p", [Else]),
            timer:sleep(100)
    end,
    poll_stats(Host).    

stop_data_collectors(Collectors) ->
    [C ! stop || C <- Collectors].
    
maybe_start_base_load([]) ->
    none.

maybe_stop_base_load(none) ->
    ok.

%% need more sensible test names.
test_name() ->
    Vsn = rt_config:get(perf_version),
    BinSize = rt_config:get(perf_binsize),
    rt_config:get(perf_test_name)++"-"++Vsn++"-"++
        atom_to_list(rt_config:get(perf_test_type))++"-"++
        atom_to_list(rt_config:get(perf_bin_type))++"-"++
        integer_to_list(BinSize)++"b-"++date_string().

collect_test_data(Hosts, TestName) ->
    %% stop the dstat watching processes
    [rtssh:ssh_cmd(Host, "killall python") %% potentially unsafe
     || Host <- Hosts],

    %% collect the files
    OSPid = os:getpid(),
    PrepDir = "/tmp/perf-"++OSPid,

    file:write_file(PrepDir++"/END",
                    io_lib:format("~w~n", [calendar:local_time()])),

    %% get rid of this hateful crap
    [begin
         rtssh:cmd("scp -q "++Host++":/tmp/dstat-"++OSPid++" "
                   ++PrepDir++"/dstat-"++Host),
         rtssh:ssh_cmd(Host, "rm /tmp/dstat-"++OSPid)
     end || Host <- Hosts],

    
    ok = rt_bench:collect_bench_data(PrepDir),

    %% grab all the benchmark stuff. need L to make real files because
    %% it's a soft link
    BBDir = rt_config:get(basho_bench),
    rtssh:cmd("cp -aL "++BBDir++"/"++TestName++"/current_. "++PrepDir),

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

            PrepopConfig = 
                rt_bench:config(
                  max, 
                  rt_config:get(perf_runtime),
                  Hosts,
                  {int_to_bin_bigendian, {uniform_int, SetSize}},
                  rt_bench:valgen(rt_config:get(perf_bin_type), BinSize),
                  [{put,1}]),            

            rt_bench:bench(PrepopConfig, Hosts, PrepopName, 
                           rt_config:get(perf_loadgens)),

            timer:sleep(timer:minutes(1)+timer:seconds(30)),
            [exit(P, kill) || P <- PPids],
            collect_test_data(Hosts, PrepopName);
        false ->
            ok
    end.

date_string() ->
    {Mega, Sec, _Micro} = os:timestamp(),
    integer_to_list((Mega * 1000000) + Sec).


%% in the end, it'd be nice to automatically generate some of the
%% other config stuff as well, i.e. give a node count, some
%% information (RAM, fast or slow disks, etc.) and generate a config
%% that should more or less hit the same performance contours
%% regardless of what machines are being used.  I suspect that
%% data-set sizing here is what's important, the ratio of disk cache
%% to data set size.

%% this actually suggests an entirely different line of testing than
%% what's been pursued so far, running a test at say, 50% ram usage,
%% 150%, 200% etc.  Then we could skip the time-consuming and not
%% terribly enlightening up-from-cold period.

standard_config(NodeCount) ->
    standard_config(NodeCount, off).

standard_config(NodeCount, AAE) ->
    Backend = rt_config:get(rt_backend, undefined),
    Fish = rt_config:get(cuttle, true),
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
      {handoff_concurrency, 16},
      {"erlang.distribution_buffer_size", "128MB"},
      {storage_backend, memory},
      {anti_entropy, AAE}
     ]};
mk_std_conf(riak_kv_eleveldb_backend, false, Ring, AAE) ->
    [{riak_core,
      [{ring_creation_size, Ring}]},
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
      {"erlang.distribution_buffer_size", "128MB"},
      {storage_backend, leveldb},
      {anti_entropy, AAE}
     ]};
mk_std_conf(_, false, Ring, AAE) ->
    [{riak_core,
      [{ring_creation_size, Ring}]},
     {riak_kv,
      [{anti_entropy,{AAE, []}}]}
    ];
mk_std_conf(_, true, Ring, AAE0) ->
    AAE = aae_cuttle(AAE0),
    {cuttlefish,
     [{ring_size, Ring},
      {"storage_backend", "bitcask"},
      {"erlang.distribution_buffer_size", "128MB"},
      {"bitcask.io_mode", nif},
      {anti_entropy, AAE}]}.

aae_cuttle(off) ->
    passive;
aae_cuttle(on) ->
    active.

target_size(Percentage, BinSize, RamSize, NodeCount) ->
    TotalRam = RamSize * NodeCount,
    CacheTarget = trunc((Percentage/100)*TotalRam),
    BinPlus = (BinSize + 300) * 3,
    %% hacky way of rounding up to the nearest 10k
    trunc((CacheTarget/(BinPlus*10000))+1)*10000.
