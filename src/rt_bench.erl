-module(rt_bench).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").


clear_seq_state_dir() ->
    StateDir = rt_config:get(basho_bench_statedir),
    case file:list_dir(StateDir) of
        {ok, FL} ->
            [file:delete(StateDir++F) || F <- FL],
            file:del_dir(StateDir);
        {error, enoent} -> ok;
        {error, Reason} -> error(Reason)
    end.

seq_state_dir() ->
    rt_config:get(basho_bench_statedir).

bench(Config, NodeList, TestName) ->
    bench(Config, NodeList, TestName, 1).

bench(Config, NodeList, TestName, Runners) ->
    lager:info("Starting basho_bench run"),

    LoadGens =
    case rt_config:get(perf_loadgens) of
        undefined -> ["localhost"];
        LG -> LG
    end,

    case rt_config:get(perf_drop_cache, true) of
        true ->
            [begin
                 R = rtssh:ssh_cmd(Node, "sudo ~/bin/drop_caches.sh"),
                 lager:info("Dropped cache for node: ~p ~p",
                            [Node, R])
             end
             || Node <- NodeList];
        _ ->
            ok
    end,

    BBTmp = "/tmp/basho_bench_tmp"++os:getpid(),
    BBTmpStage = BBTmp ++ "_stage",
    file:make_dir(BBTmpStage),  %% removed error checking here
    Path = BBTmpStage++"/"++TestName++"-rt.config",
    RemPath = BBTmp++"/"++TestName++"-rt.config",
    [file:write_file(Path,io_lib:fwrite("~p.\n",[C]),
             [append])
     || C <- Config],
    BBDir = rt_config:get(basho_bench),
    GenList =
    [begin
         G = lists:nth((C rem 2) + 1, LoadGens),
         {G, C}
     end
     || C <- lists:seq(1, Runners)],

    F =
    fun({LG, N}, Owner) ->
        try
            Num = integer_to_list(N),

            {0, _} = rtssh:ssh_cmd(LG, "mkdir -p "++BBTmp),
            %% don't care if we fail here
            rtssh:ssh_cmd(LG, "rm -r /tmp/bb_seqstate/"),
            rtssh:scp(LG, Path, BBTmp),
            Cmd = "/usr/local/basho/erlang/R16B02/bin/escript "++
            BBDir++"basho_bench -d "++
            BBDir++"/"++TestName++"_"++Num++" "++RemPath,
            lager:info("Spawning remote basho_bench w/ ~p on ~p",
                   [Cmd, LG]),
            {0, R} = rtssh:ssh_cmd(LG, Cmd, false),
            lager:info("ssh done returned ~p", [R]),
            {0, _} = rtssh:ssh_cmd(LG, "rm -r "++BBTmp++"/")
        catch
            Class:Error ->
            lager:error("basho_bench died with error ~p:~p",
                    [Class, Error])
        after
            lager:info("finishing bb run")
        end,
        Owner ! done
    end,
    S = self(),
    [begin
     spawn(fun() -> F(R, S) end)
     end
     || R <- GenList],
    [receive done -> ok end || _ <- lists:seq(1,Runners)],
    lager:info("removing stage dir"),
    {ok, FL} = file:list_dir(BBTmpStage),
    [file:delete(BBTmpStage++File) || File <- FL],
    file:del_dir(BBTmpStage).

discard_bb_output(Port) ->
    receive
        {Port, {data, _Bytes}} ->
            %%lager:info("bb ~p", [Bytes]),
            discard_bb_output(Port);
        {Port, {exit_status, 0}} ->
            ok;
        {Port, {exit_status, Status}} ->
            throw({bb_error, Status})
    end.
