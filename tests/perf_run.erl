-module(perf_run).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt:config(rt_harness))).

confirm() ->
    Vsn = rt:config(perf_version),
    HostList = rt:config(rt_hostnames),
    BinSize = rt:config(perf_binsize),

    %% need this for http requests
    inets:start(),

    Count = length(HostList),
    Config = 
    case rt:config(rt_backend, undefined) of 
        riak_kv_memory_backend ->
            [{riak_core, 
              [{ring_creation_size, 64},
               {handoff_concurrency, 16}]},
             {riak_kv, [{storage_backend, riak_kv_memory_backend},
                        {anti_entropy,{off,[]}},
                        {memory_backend, []}, %%{max_memory, 256}]},
                        {fsm_limit, 50000}
                       ]}];
        riak_kv_eleveldb_backend -> 
            [{riak_core, 
              [{ring_creation_size, rt:nearest_ringsize(Count)}]},
             {riak_kv, 
              [{storage_backend, riak_kv_eleveldb_backend},
               {anti_entropy,{off,[]}},
               {fsm_limit, undefined}]},
             {eleveldb, 
              [{max_open_files, 500}]}
            ];
        _ ->
            [{riak_core, 
              [{ring_creation_size, rt:nearest_ringsize(Count)}]},
             {riak_kv, [{storage_backend, riak_kv_bitcask_backend},
                        {anti_entropy,{on,[]}},
                        {fsm_limit, 50000}
                       ]}]
        end,
    %% make sure that all of the remote nodes have a clean build at
    %% the remote location
    case rt:config(perf_restart, meh) of
        true ->
            ?assertMatch(ok, rtssh:ensure_remote_build(HostList, Vsn));
        _ -> ok
    end,
            
    Nodes = 
        case rt:config(perf_restart) of
            true -> 
                rt:build_cluster(Count, 
                                 lists:duplicate(Count, {Vsn, Config}), 
                                 whatever);
            false ->  
                [list_to_atom("riak@" ++ Host) || Host <- HostList]
        end,
                                                    
    rt:wait_until_nodes_ready(Nodes),
    rt:wait_until_ring_converged(Nodes),
    rt:wait_until_transfers_complete(Nodes),

    case rt:config(perf_prepop) of
        true -> 
            PPids = start_data_collectors(HostList),
            PrepopName = rt:config(perf_test_name)++"-"++Vsn++
                "-prepop-binsize"++integer_to_list(BinSize)++"-"++date_string(),
            
            do_prepop(HostList, BinSize, PrepopName),

            [exit(P, kill) || P <- PPids],
            collect_test_data(HostList, PrepopName);
        false -> 
            ok
    end,

    Test = 
        case rt:config(perf_config, undefined) of 
            undefined ->
                case rt:config(perf_test_type, undefined) of 
                    undefined ->
                        error("no run config or runtype defined");
                    pareto ->
                        do_pareto;
                    uniform ->
                        do_uniform
                end
        end,
    Pids = start_data_collectors(HostList),

    TestName = rt:config(perf_test_name)++"-"++Vsn++"-"++
        atom_to_list(rt:config(perf_test_type))++"-"++
        atom_to_list(rt:config(perf_bin_type))++"-"++
        atom_to_list(rt:config(perf_load_type))++"-"++
        "binsize"++integer_to_list(BinSize)++"-"++date_string(),

    ?MODULE:Test(HostList, BinSize, TestName),

    [exit(P, kill) || P <- Pids],
    collect_test_data(HostList, TestName).

start_data_collectors(Hosts) ->
    OSPid = os:getpid(),
    PrepDir = "/tmp/perf-"++OSPid,
    file:make_dir(PrepDir),

    Cmd = "python ./bin/dstat -cdngyimrs --vm --fs --socket --tcp --disk-util "++
        "--output "++"/tmp/dstat-"++os:getpid(), %%++" "++
        %%"--graphite-host r2s11.bos1 --graphite-port 2004",

%%    Cmd2 = "python ./bin/graphite-report.py",
    
    file:write(PrepDir++"/START", io_lib:format("~w.~n", [calendar:local_time()])),

    [spawn(rtssh, ssh_cmd, [Host, Cmd]) || Host <- Hosts],
%%    [spawn(rtssh, ssh_cmd, [Host, Cmd2]) || Host <- Hosts],
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
        Else -> 
            lager:error("Web stat collector failed with: ~p", [Else]),
            timer:sleep(100)
    end,
    poll_stats(Host).

collect_test_data(Hosts, TestName) ->
    %% stop the dstat watching processes
    [rtssh:ssh_cmd(Host, "killall python") %% potentially unsafe
     || Host <- Hosts],
    
    %% collect the files
    OSPid = os:getpid(),
    PrepDir = "/tmp/perf-"++OSPid,

    file:write_file(PrepDir++"/END", 
                    io_lib:format("~w~n", [calendar:local_time()])),

    [begin
         rtssh:cmd("scp "++Host++":/tmp/dstat-"++OSPid++" "
                   ++PrepDir++"/dstat-"++Host),
         rtssh:ssh_cmd(Host, "rm /tmp/dstat-"++OSPid)
     end || Host <- Hosts],

    %% grab all the benchmark stuff
    %% need L to make real files because it's a soft link
    BBDir = rt:config(basho_bench),
    rtssh:cmd("cp -aL "++BBDir++"/"++TestName++"/current "++PrepDir),

    %% TODO: stuff things into graphite

    %% TODO: upload to to the artifacts server?

    rt:cmd("mv "++PrepDir++" results/"++TestName),
    ok.

do_prepop(NodeList, BinSize, TestName) ->
    PrepopCount = rt:config(perf_prepop_size),
    Config = prepop_config(BinSize, NodeList, PrepopCount),
    rt_bench:clear_seq_state_dir(),
    lager:info("Config ~p", [Config]),
    rt_bench:bench(Config, NodeList, TestName),
    lager:info("Prepop complete").

prepop_config(BinSize, NodeList, BinCount0) ->
    Count = length(NodeList),
    {Mode, _Duration} = get_md(BinSize),

    BinCount = adjust_count(BinCount0, 30*Count), 
    
                
    lager:info("Starting prepop run for ~p binaries of size ~p bytes",
               [BinCount, BinSize]),

    [Mode,
     %% run to completion
     {duration, infinity},
     {concurrent, 30*Count},

     {riakc_pb_bucket, <<"b1">>},
     {key_generator, {int_to_str, {partitioned_sequential_int, 0, BinCount}}},
     {value_generator, valgen(BinSize)},
     {operations, operations(prepop)},
     {sequential_int_state_dir, rt_bench:seq_state_dir()},

     %% should add asis when it's supported by the driver.
     {riakc_pb_ips, NodeList},
     {riakc_pb_replies, default},
     {driver, basho_bench_driver_riakc_pb},
     {code_paths, ["evan/basho_bench/deps/riakc",
                   "evan/basho_bench/deps/protobuffs"]}].

adjust_count(Count, Concurrency) ->
    case Count rem Concurrency of
        0 -> Count;
        N -> Count + (Concurrency - N)
    end.

do_pareto(NodeList, BinSize, TestName) ->
    Config = pareto_config(BinSize, NodeList),
    rt_bench:bench(Config, NodeList, TestName).

pareto_config(BinSize, NodeList) ->
    Count = length(NodeList),
    {Mode, Duration} = get_md(BinSize),

    [Mode,
     {duration, Duration},
     {concurrent, 30*Count},

     {riakc_pb_bucket, <<"b1">>}, 
     {key_generator, {int_to_str, {truncated_pareto_int, 10000000}}},
     {value_generator, valgen(BinSize)},
     {operations, operations(pareto)}, %% update - 50% get, 50% put
     
     %% should add asis when it's supported by the driver.
     {riakc_pb_ips, NodeList},
     {riakc_pb_replies, default},
     {driver, basho_bench_driver_riakc_pb},
     {code_paths, ["deps/riakc",
                   "deps/protobuffs"]}].

do_uniform(NodeList, BinSize, TestName) ->
    Config = uniform_config(BinSize, NodeList),
    rt_bench:bench(Config, NodeList, TestName, 2).

uniform_config(BinSize, NodeList) ->
    Count = length(NodeList),
    {Mode, Duration} = get_md(BinSize),

    Numkeys = 
	case rt:config(perf_prepop_size) of
	    Count when is_integer(Count) -> 
		Count;
	    _ -> 10000000
	end,

    [Mode,
     {duration, Duration},
     {concurrent, 40*Count},

     {riakc_pb_bucket, <<"b1">>}, 
     {key_generator, {int_to_str, {uniform_int, Numkeys}}},
     {value_generator, valgen(BinSize)},
     {operations, operations(uniform)},
     
     %% should add asis when it's supported by the driver.
     {riakc_pb_ips, NodeList},
     {riakc_pb_replies, default},
     {driver, basho_bench_driver_riakc_pb},
     {code_paths, ["evan/basho_bench/deps/riakc",
                   "evan/basho_bench/deps/protobuffs"]}].



get_md(_BinSize) ->
    case rt:config(rt_backend, undefined) of 
        %% hueristically determined nonsense, need a real model
        %% riak_kv_eleveldb_backend ->
        %%     lager:info("leveldb"),
        %%     Rate = 
        %%         case BinSize >= 10000 of
        %%             true -> 50;
        %%             false -> 75
        %%         end,
        %%     {{mode, {rate, Rate}}, 150};
        _ ->
            %%fixme yo
            lager:info("unset or bitcask"),
            {{mode, max}, 60}
    end.

date_string() ->
    {YrMoDay, HrMinSec} = calendar:local_time(),
    string:join(lists:map(fun erlang:integer_to_list/1, 
                          tuple_to_list(YrMoDay)++tuple_to_list(HrMinSec)), 
                "-").

valgen(BinSize) ->    
    Type = rt:config(perf_bin_type),
    case Type of
        fixed ->
            {fixed_bin, BinSize};
        exponential ->
            Quarter = BinSize div 4,
            {exponential_bin, Quarter, Quarter*3}
    end.

operations(Type) ->
    LoadType = rt:config(perf_load_type), 
    N = 
        case LoadType of 
            read_heavy -> 4;
            write_heavy -> 1
        end,
            
    case Type of 
        prepop ->
            [{put, 1}];
        pareto -> 
            [{get, N*3}, {put, 3}, {update, 1}];
        uniform ->
             [{get, N*3}, {update, 1}]
    end.
            
          
