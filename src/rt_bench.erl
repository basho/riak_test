-module(rt_bench).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(ESCRIPT, rt_config:get(basho_bench_escript)).

seq_state_dir() ->
    rt_config:get(basho_bench_statedir).

bench(Config, NodeList, TestName) ->
    bench(Config, NodeList, TestName, 1).

bench(Config, NodeList, TestName, Runners) ->
    bench(Config, NodeList, TestName, Runners, false).

bench(Config, NodeList, TestName, Runners, Drop) ->
    lager:info("Starting basho_bench run"),

    LoadGens = rt_config:get(perf_loadgens, ["localhost"]),

    case Drop of
        true ->
            Fun = fun(Node) ->
                        R = rtssh:ssh_cmd(Node, "sudo ~/bin/drop_caches.sh"),
                        lager:info("Dropped cache for node: ~p ret: ~p",
                                   [Node, R])
                end,
            rt:pmap(Fun, NodeList);
        _ -> ok
    end,

    %% make a local config file, to be copied to a remote
    %% loadgen. They're named separately because for simplicity, we
    %% use network operations even for local load generation

    %% remote tempdir name
    BBTmp = "/tmp/basho_bench_tmp"++os:getpid(),
    %% local staging version
    BBTmpStage = BBTmp ++ "_stage/",
    ok = filelib:ensure_dir(BBTmpStage),

    Filename = TestName++"-rt.config",
    ConfigPath = BBTmpStage++"/"++Filename,
    RemotePath = BBTmp++"/"++Filename,
    [file:write_file(ConfigPath,io_lib:fwrite("~p.\n",[C]),
                     [append])
     || C <- Config],
    BBDir = rt_config:get(basho_bench),
    GenList =
    [begin
         G = lists:nth(C, LoadGens),
         {G, C}
     end
     || C <- lists:seq(1, Runners)],

    F = fun({LG, N}, Owner) ->
                try
                    Num = integer_to_list(N),

                    {0, _} = rtssh:ssh_cmd(LG, "mkdir -p "++BBTmp),
                    %% don't care much if we fail here
                    rtssh:ssh_cmd(LG, "rm -r " ++ seq_state_dir()),
                    {0, _} = rtssh:scp_to(LG, ConfigPath, BBTmp),
                    %% run basho bench on the remote loadgen,
                    %% specifying the remote testdir and the newly
                    %% copied remote config location
                    Cmd = ?ESCRIPT++" "++
                        BBDir++"/"++"basho_bench -d "++
                        BBDir++"/"++TestName++"_"++Num++" "++RemotePath,
                    lager:info("Spawning remote basho_bench w/ ~p on ~p",
                               [Cmd, LG]),
                    {0, R} = rtssh:ssh_cmd(LG, Cmd, false),
                    lager:info("bench run finished, on ~p returned ~p",
                   [LG, R]),
                    {0, _} = rtssh:ssh_cmd(LG, "rm -r "++BBTmp++"/"),
            Owner ! {done, ok}
                catch
                    Class:Error ->
                        lager:error("basho_bench died with error ~p:~p",
                                    [Class, Error]),
            Owner ! {done, error}
                after
                    lager:info("finished bb run")
                end
        end,
    S = self(),
    [spawn(fun() -> F(R, S) end)|| R <- GenList],
    [ok] = lists:usort([receive {done, R} -> R end
            || _ <- GenList]),
    lager:debug("removing stage dir"),
    {ok, FL} = file:list_dir(BBTmpStage),
    [file:delete(BBTmpStage++File) || File <- FL],
    ok = file:del_dir(BBTmpStage).

collect_bench_data(TestName, Dir) ->
    %% grab all the benchmark stuff. need L to make real files because
    %% it's a soft link
    BBDir = rt_config:get(basho_bench),
    Gens = rt_config:get(perf_loadgens, ["localhost"]),
    Len = length(Gens),
    [begin
     N = integer_to_list(N0),
     rtssh:scp_from(Gen, BBDir++"/"++TestName++"_"++N++"/current", Dir ++ "/" ++ Gen)
     end
     || {Gen, N0} <- lists:zip(Gens, lists:seq(1, Len))],
    ok.

-define(CONCURRENCY_FACTOR, rt_config:get(basho_bench_concurrency, 30)).

config(Rate, Duration, NodeList, KeyGen,
       ValGen, Operations) ->
    config(Rate, Duration, NodeList, KeyGen,
           ValGen, Operations,
           <<"testbucket">>, riakc_pb).

config(Rate, Duration, NodeList, KeyGen,
       ValGen, Operations, Bucket, Driver0) ->
    {Driver, DriverB} = 
	case Driver0 of
	    '2i' -> {pb, riakc_pb};
	    _ -> {Driver0, Driver0}
	end,
    DriverBucket = append_atoms(DriverB, '_bucket'),
    DriverIps = append_atoms(Driver, '_ips'),
    DriverReplies = append_atoms(DriverB, '_replies'),
    DriverName = append_atoms(basho_bench_driver_, Driver0),
    [
     {mode, {rate, Rate}},
     {duration, Duration},
     {concurrent, ?CONCURRENCY_FACTOR * length(NodeList)},
     {rng_seed, now},

     {DriverBucket, Bucket},
     {key_generator, KeyGen},
     {value_generator, ValGen},
     {operations, Operations},
     %% just leave this in in case we need it, it's harmless when not
     %% using the sequential generator
     {sequential_int_state_dir, seq_state_dir()},

     {DriverIps, NodeList},
     {DriverReplies, default},
     {driver, DriverName}
     %%{code_paths, rt_config:get(basho_bench_code_paths)}
    ].

append_atoms(L, R) ->
    list_to_atom(atom_to_list(L)++atom_to_list(R)).

valgen(Type, BinSize) ->
    case Type of
        fixed ->
            {fixed_bin, BinSize};
        exponential ->
            Quarter = BinSize div 4,
            {exponential_bin, Quarter, Quarter*3}
    end.
