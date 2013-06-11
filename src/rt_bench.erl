-module(rt_bench).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").


clear_seq_state_dir() ->
    StateDir = rt:config(basho_bench_statedir),
    case file:list_dir(StateDir) of
        {ok, FL} ->
            [file:delete(StateDir++F) || F <- FL],    
            file:del_dir(StateDir);
        {error, enoent} -> ok;
        {error, Reason} -> error(Reason)
    end.

seq_state_dir() ->
    rt:config(basho_bench_statedir).

bench(Config, NodeList, TestName) ->
    lager:info("Starting basho_bench run"),

    %% will need to start dstat here
    [begin
         R = rtssh:ssh_cmd(Node, "sudo ~/bin/drop_caches.sh"),
         lager:info("Dropped cache for node: ~p ~p", 
                    [Node, R])
     end
     || Node <- NodeList],

    BBTmp = "/tmp/basho_bench_tmp"++os:getpid()++"/",
    ok = file:make_dir(BBTmp),
    Path = BBTmp++TestName++"-rt.config",
    [file:write_file(Path,io_lib:fwrite("~p.\n",[C]),[append]) 
     || C <- Config],
    BBDir = rt:config(basho_bench),
    try
        Cmd = BBDir++"basho_bench -d "++BBDir++"/"++TestName++" "++Path,
        lager:info("Spawning basho_bench w/ ~p", [Cmd]),
        Port = rtssh:spawn_cmd(Cmd),
        lager:debug("bb port ~p", [Port]),
        discard_bb_output(Port)
    catch
        Error ->
            lager:error("basho_bench died with error ~p", [Error])
    after
        {ok, FL} = file:list_dir(BBTmp),
        [file:delete(BBTmp++F) || F <- FL],
        file:del_dir(BBTmp)
    end.

discard_bb_output(Port) ->
    receive
        {Port, {data, _Bytes}} ->
            %%lager:info("bb ~p", [Bytes]),
            discard_bb_output(Port);
        {Port, {exit_status, 0}} ->
            ok;
        {Port, {exit_status, Status}} ->
            {bb_error, Status}
    end.

        
