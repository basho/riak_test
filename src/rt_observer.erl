-module(rt_observer).
-compile(export_all).

-record(history, {network,
                  disk,
                  rate,
                  nodes,
                  lvlref,
                  collector_sock}).

-record(watcher, {nodes,
          acceptor,
                  collector,
                  probes}).

%% See: https://www.kernel.org/doc/Documentation/iostats.txt
-record(disk, {read,
               read_merged,
               read_sectors,
               read_wait_ms,
               write,
               write_merged,
               write_sectors,
               write_wait_ms,
               io_pending,
               io_wait_ms,
               io_wait_weighted_ms}).

watch(Nodes, Collector) ->
    Pid = spawn(?MODULE, watcher, [self(), Nodes, Collector]),
    %%start(self(), 1000, Collector, Nodes, ping),
    Pid.

watcher(Master, Nodes, {_Host, Port, _Dir} = Collector) ->
    case gen_tcp:listen(Port, [{active, false}, binary,
                   {packet, 2}]) of
    {ok, LSock} ->
        Acceptor = spawn(?MODULE, lloop, [self(), LSock]),
        monitor(process, Master),
        Probes = [{Node, undefined} || Node <- Nodes],
        W = #watcher{nodes=Nodes,
             acceptor={Acceptor, LSock},
             collector=Collector,
             probes=Probes},
        watcher_loop(W);
    {error, eaddrinuse} ->
        timer:sleep(100),
        watcher(Master, Nodes, Collector)
    %% case_clause other errors
    end.

lloop(Master, LSock) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            ok = gen_tcp:controlling_process(Sock, Master),
            inet:setopts(Sock, [{active, once}]),
            lloop(Master, LSock);
        _ ->
            ok
    end.

watcher_loop(W=#watcher{probes=Probes,
            acceptor={Acceptor,LSock},
            collector={_,_,Dir}}) ->
    Missing = [Node || {Node, undefined} <- Probes],
    %% io:format("Missing: ~p~n", [Missing]),
    W2 = install_probes(Missing, W),
    Probes2 = W2#watcher.probes,
    receive
        {'DOWN', MRef, process, _, Reason} ->
            case lists:keyfind(MRef, 2, Probes2) of
                false ->
                    %% master died, exit
                    io:format("watcher exiting~n"),
                    ok;
                {Node, MRef} ->
                    io:format("Probe exit. ~p: ~p~n", [Node, Reason]),
                    Probes3 = lists:keyreplace(Node, 1, Probes2, {Node, undefined}),
                    W3 = W2#watcher{probes=Probes3},
                    ?MODULE:watcher_loop(W3)
            end;
    {tcp, Sock, Msg} ->
        inet:setopts(Sock, [{active, once}]),
        case catch binary_to_term(Msg) of
        Stats when is_list(Stats) ->
            case get(Sock) of
            undefined ->
                {ok, {Addr, _Port}} = inet:peername(Sock),
                SAddr = inet:ntoa(Addr),
                %% not sure that we want to just blindly append...
                {ok, FD} = file:open(Dir++"/cstats-"++SAddr, [append]),
                dump_stats(Stats, FD),
                put(Sock, FD);
            FD ->
                dump_stats(Stats, FD)
            end
        end,
        ?MODULE:watcher_loop(W2);
    stop ->
        exit(Acceptor),
        gen_tcp:close(LSock),
        [begin
         file:close(FD),
         gen_tcp:close(Sock)
         end
         || {Sock, FD} <- get()]
    end.


install_probes([], W) ->
    W;
install_probes(Nodes, W=#watcher{collector=Collector, nodes=AllNodes, probes=Probes}) ->
    load_modules_on_nodes([?MODULE], Nodes),
    R = rpc:multicall(Nodes, ?MODULE, start, [self(), 5000, Collector, AllNodes, collect]),
    {Pids, Down} = R,
    Probes2 = lists:foldl(fun({Node, Pid}, Acc) ->
                                  if is_pid(Pid) ->
                                          lists:keystore(Node, 1, Acc, {Node, monitor(process, Pid)});
                                     true ->
                                          Acc
                                  end
                          end, Probes, Pids),
    Probes3 = lists:foldl(fun(Node, Acc) ->
                                  lists:keystore(Node, 1, Acc, {Node, undefined})
                          end, Probes2, Down),
    W#watcher{probes=Probes3}.

start(Master, Rate, Collector, Nodes, Fun) ->
    lager:info("In start: ~p~n", [node()]),
    Pid = spawn(?MODULE, init, [Master, Rate, Collector, Nodes, Fun]),
    {node(), Pid}.

init(Master, Rate, {Host, Port, _Dir}, Nodes, Fun) ->
    lager:info("In init: ~p ~p~n", [node(), Host]),
    {ok, Sock} = gen_tcp:connect(Host, Port,
                                 [binary, {packet, 2},
				 {send_timeout, 500}]),
    case application:get_env(riak_kv, storage_backend) of
        {ok, riak_kv_eleveldb_backend} ->
            LRef = get_leveldb_ref();
        _ ->
            LRef = undefined
    end,
    H = #history{network=undefined,
                 %% disk=undefined,
                 disk=[],
                 rate=Rate div 5000,
                 lvlref=LRef,
                 nodes=Nodes,
                 collector_sock=Sock},
    monitor(process, Master),
    loop(Fun, Rate, H).

loop(Fun, Rate, H) ->
    %% io:format("loop: ~p~n", [node()]),
    NewH = ?MODULE:Fun(H),
    receive
        {'DOWN', _, process, _, _} ->
            %%io:format("shutting: ~p~n", [node()]),
            ok
    after Rate ->
            ?MODULE:loop(Fun, Rate, NewH)
    end.

%% fix this later
%% ping(H=#history{nodes=Nodes}) ->
%%     TS = timestamp(),
%%     XNodes = lists:zip(lists:seq(1, length(Nodes)), Nodes),
%%     pmap(fun({X,Node}) ->
%%                  case net_adm:ping(Node) of
%%                      pang ->
%%                          notify_down(TS, X, Node, H),
%%                          ok;
%%                      pong ->
%%                          case rpc:call(Node, riak_core_node_watcher, services, [Node]) of
%%                              L when is_list(L) ->
%%                                  case lists:member(riak_kv, L) of
%%                                      true ->
%%                                          ok;
%%                                      false ->
%%                                          notify_down(TS, X, Node, H)
%%                                  end;
%%                              _ ->
%%                                  notify_down(TS, X, Node, H)
%%                          end;
%%                      _ ->
%%                          ok
%%                  end
%%          end, XNodes),
%%     H.

%% notify_down(TS, X, Node, H) ->
%%     %% emit_stat(Stat, TS, Value, H) ->
%%     NodeBin = atom_to_binary(Node, utf8),
%%     Metric = <<"offline_nodes/", NodeBin/binary>>,
%%     emit_stat2(Metric, TS, X, H).

collect(H0) ->
    {H, L} = report_leveldb(H0),
    {_, Q} = report_queues(H),
    {_, P} = report_processes(H),
    {H2, N} = report_network(H),

    DiskList =
    case get(disks) of
        undefined ->
        Disks = determine_disks(),
        put(disks, Disks),
        Disks;
        Disks ->
        Disks
    end,

    {H3, D} = report_disk2(DiskList, H2),
    {_, V} = report_vmstat(H3),
    {_, M} = report_memory(H3),
    C = report_stats(riak_core_stat, all),
    R = report_stats(riak_kv_stat, all),
    Stats0 = L ++ Q ++ P ++ N ++ D ++ V ++ M ++ C ++ R,
    Stats = term_to_binary(Stats0),
    %% catch TCP errors here
    case gen_tcp:send(H3#history.collector_sock, Stats) of
	ok -> ok;
	%% die on any error, we'll get restarted soon.
	{error, _} ->
	    gen_tcp:close(H3#history.collector_sock),
	    error(splode)
    end,
    H3.

%% this portion is meant to be run inside a VM instance running riak
determine_disks() ->
    DataDir =
    case application:get_env(riak_kv, storage_backend) of
        {ok, riak_kv_bitcask_backend} ->
        {ok, Dir} = application:get_env(bitcask, data_root),
        Dir;
        {ok, riak_kv_eleveldb_backend} ->
        {ok, Dir} = application:get_env(eleveldb, data_root),
        Dir;
        _ ->
        error(unhandled_backend)
    end,
    Name0 = os:cmd("basename `df "++DataDir++
               " | tail -1 | awk '{print $1}'`"),
    {Name, _} = lists:split(length(Name0)-1, Name0),
    %% keep the old format just in case we need to extend this later.
    [{Name, Name}].


report_queues(H) ->
    Max = lists:max([Len || Pid <- processes(),
                            {message_queue_len, Len} <- [process_info(Pid, message_queue_len)]]),
    {H, [{message_queue_max, Max}]}.

report_processes(H) ->
    Procs = erlang:system_info(process_count),
    {H, [{erlang_processes, Procs}]}.

report_network(H=#history{network=LastStats, rate=Rate}) ->
    {RX, TX} = get_network(),
    Report =
    case LastStats of
        undefined ->
        [];
        {LastRX, LastTX} ->
        RXRate = net_rate(LastRX, RX) div Rate,
        TXRate = net_rate(LastTX, TX) div Rate,
        [{net_rx, RXRate},
         {net_tx, TXRate}]
    end,
    {H#history{network={RX, TX}}, Report}.

report_disk2(Disks, H=#history{disk=DiskStats}) ->
    {NewStats, NewReport} =
        lists:foldl(fun({Name, Dev}, {OrdAcc, LstAcc}) ->
                            LastStats = case orddict:find(Dev, DiskStats) of
                                            error ->
                                                undefined;
                                            {ok, LS} ->
                                                LS
                                        end,
                            {Stats, Report} = report_disk2(Name, Dev, LastStats, H),
                            {orddict:store(Dev, Stats, OrdAcc),
                 LstAcc ++ Report}
                    end, {DiskStats, []}, Disks),
    {H#history{disk=NewStats}, NewReport}.

report_disk2(_Name, Dev, LastStats, #history{rate=Rate}) ->
    Stats = get_disk2(Dev),
    Report =
    case LastStats of
        undefined ->
        [];
        _ ->
        ReadRate = disk_rate(#disk.read_sectors, LastStats, Stats) div Rate,
        WriteRate = disk_rate(#disk.write_sectors, LastStats, Stats) div Rate,
        {AwaitR, AwaitW} = disk_await(LastStats, Stats),
        Svctime = disk_svctime(LastStats, Stats),
        QueueLen = disk_qlength(LastStats, Stats),
        Util = disk_util(LastStats, Stats),
        [{disk_read, ReadRate},
         {disk_write, WriteRate},
         {disk_await_r, AwaitR},
         {disk_await_w, AwaitW},
         {disk_svctime, Svctime},
         {disk_queue_size, QueueLen},
         {disk_utilization, Util}]
    end,
    {Stats, Report}.

append_atoms(Atom, List) ->
    list_to_atom(List ++
             "_" ++ atom_to_list(Atom)).

report_memory(H) ->
    Stats = get_memory(),
    Util = memory_util(Stats),
    Dirty = memory_dirty(Stats),
    Writeback = memory_writeback(Stats),
    {H, [{memory_utilization, Util},
     {memory_page_dirty, Dirty},
     {memory_page_writeback, Writeback}]}.

report_leveldb(H = #history{ lvlref = undefined }) ->
    {H, []};
report_leveldb(H = #history{ lvlref = LRef }) ->
    try case eleveldb:status(LRef, <<"leveldb.ThrottleGauge">>) of
            {ok, Result} ->
                Value = list_to_integer(Result),
                {H, [{leveldb_write_throttle, Value}]};
            _ ->
                {H, []}
        end
    catch
        _:_ ->
            LRef2 = get_leveldb_ref(),
        {H#history{lvlref=LRef2}, []}
    end.

net_rate(Bytes1, Bytes2) ->
    (Bytes2 - Bytes1) div 1024.

disk_rate(I, Stats1, Stats2) ->
    disk_rate(element(I, Stats1), element(I, Stats2)).

disk_rate(Sectors1, Sectors2) ->
    %% 512-byte sectors
    (Sectors2 - Sectors1) div 2.

disk_await(S1, S2) ->
    NumR = erlang:max(S2#disk.read - S1#disk.read, 1),
    NumW = erlang:max(S2#disk.write - S1#disk.write, 1),
    AwaitR = (S2#disk.read_wait_ms - S1#disk.read_wait_ms) div NumR,
    AwaitW = (S2#disk.write_wait_ms - S1#disk.write_wait_ms) div NumW,
    {AwaitR, AwaitW}.

disk_svctime(S1, S2) ->
    NumR = S2#disk.read - S1#disk.read,
    NumW = S2#disk.write - S1#disk.write,
    NumIO = erlang:max(NumR + NumW, 1),
    Wait = S2#disk.io_wait_ms - S1#disk.io_wait_ms,
    Wait div NumIO.

disk_util(S1, S2) ->
    Wait = S2#disk.io_wait_ms - S1#disk.io_wait_ms,
    Wait * 100 div 5000. %% Really should be div Rate

disk_qlength(S1, S2) ->
    (S2#disk.io_wait_weighted_ms - S1#disk.io_wait_weighted_ms) div 5000.

filter(L, Pos, Val) ->
    [T || T <- L,
          element(Pos, T) /= Val].

message_queues([], _Threshold, _VNodeMap, Queues) ->
    lists:reverse(lists:keysort(1, Queues));
message_queues([Pid|Pids], Threshold, VNodeMap, Queues) ->
    case process_info(Pid, [message_queue_len, registered_name]) of
        [{message_queue_len, Len},
         {registered_name, RegName}] when Len > Threshold ->
            Entry = {Len, pid_name(Pid, RegName, VNodeMap)},
            message_queues(Pids, Threshold, VNodeMap, [Entry|Queues]);
        _ ->
            message_queues(Pids, Threshold, VNodeMap, Queues)
    end.

get_network() ->
    {ok, RX} = file:read_file("/sys/class/net/eth0/statistics/rx_bytes"),
    {ok, TX} = file:read_file("/sys/class/net/eth0/statistics/tx_bytes"),
    {to_integer(RX), to_integer(TX)}.

get_disk2() ->
    {ok, Bin} = file:read_file("/sys/block/md127/stat"),
    %% {ok, Bin} = file:read_file("/sys/block/dm-0/stat"),
    Stats = parse_disk_stats(Bin),
    Stats.

get_disk2(Dev) ->
    {ok, Bin} = file:read_file("/sys/block/" ++ Dev ++ "/stat"),
    Stats = parse_disk_stats(Bin),
    Stats.

%% get_disk() ->
%%     {ok, Bin} = file:read_file("/sys/block/md127/stat"),
%%     Stats = parse_disk_stats(Bin),
%%     {Stats#disk.read_sectors, Stats#disk.write_sectors}.

memory_util(Mem) ->
    Stat = fun(Key) ->
                   list_to_integer(element(2, lists:keyfind(Key, 1, Mem)))
           end,
    Total = Stat("MemTotal:"),
    Free  = Stat("MemFree:"),
    Buffers = Stat("Buffers:"),
    Cached  = Stat("Cached:"),
    (Total - Free - Buffers - Cached) * 100 div Total.

memory_dirty(Mem) ->
    {_, Dirty} = lists:keyfind("Dirty:", 1, Mem),
    list_to_integer(Dirty).

memory_writeback(Mem) ->
    {_, Writeback} = lists:keyfind("Writeback:", 1, Mem),
    list_to_integer(Writeback).

get_memory() ->
    S = os:cmd("cat /proc/meminfo"),
    [case string:tokens(L," ") of
         [Key, Value, _] ->
             {Key, Value};
         [Key, Value] ->
             {Key, Value};
         _ ->
             ignore
     end || L <- string:tokens(S, "\n")].

parse_disk_stats(Bin) ->
    [Line|_] = binary:split(Bin, <<"\n">>),
    Fields = string:tokens(binary_to_list(Line), " "),
    Fields2 = [list_to_integer(Field) || Field <- Fields],
    list_to_tuple([disk|Fields2]).

to_integer(Bin) ->
    [Line|_] = binary:split(Bin, <<"\n">>),
    list_to_integer(binary_to_list(Line)).

pid_name(Pid, [], VNodeMap) ->
    case dict:find(Pid, VNodeMap) of
        {ok, VNode} ->
            VNode;
        _ ->
            Pid
    end;
pid_name(_Pid, RegName, _VNodeMap) ->
    RegName.

report_stats(Mod, Keys) ->
    Stats = Mod:get_stats(),
    case Keys of
    all ->
        Stats;
    _ ->
        lists:filter(fun({Key, _Value}) ->
                 lists:member(Key, Keys)
             end, Stats)
    end.

%%%===================================================================
%%% Utility functions
%%%===================================================================
pmap(F, L) ->
    Parent = self(),
    lists:mapfoldl(
      fun(X, N) ->
              Pid = spawn(fun() ->
                                  Parent ! {pmap, N, F(X)}
                          end),
              {Pid, N+1}
      end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    [R || {_, R} <- lists:keysort(1, L2)].

load_modules_on_nodes(Modules, Nodes) ->
    [case code:get_object_code(Module) of
         {Module, Bin, File} ->
             %% rpc:multicall(Nodes, code, purge, [Module]),
             {Ret, []} = rpc:multicall(Nodes, code, 
				       load_binary, [Module, File, Bin]),
	     [{module, observer}] = lists:usort(Ret);
         error ->
             error({no_object_code, Module})
     end || Module <- Modules].

get_leveldb_ref() ->
    VNodes = riak_core_vnode_manager:all_vnodes(riak_kv_vnode),
    {_, _, Pid} = hd(VNodes),
    State = get_state(Pid),
    ModState = element(4, State),
    case element(3,ModState) of
        riak_kv_eleveldb_backend ->
            LvlState = element(4, ModState),
            element(2, LvlState);
        _ ->
            undefined
    end.

get_state(Pid) ->
    {status, Pid, _Mod, Status} = sys:get_status(Pid),
    Status2 = lists:flatten(Status),
    Status3 = [L || {data, L} <- Status2],
    Status4 = lists:flatten(Status3),
    State = proplists:get_value("StateData", Status4),
    State.

dump_stats(Stats, FD) ->
    Out = io_lib:format("~p.~n ~p.~n",
            [os:timestamp(), Stats]),
    ok = file:write(FD, Out),
    %% not sure this is required.
    file:sync(FD).

-record(vmstat, {procs_r,
                 procs_b,
                 mem_swpd,
                 mem_free,
                 mem_buff,
                 mem_cache,
                 swap_si,
                 swap_so,
                 io_bi,
                 io_bo,
                 system_in,
                 system_cs,
                 cpu_us,
                 cpu_sy,
                 cpu_id,
                 cpu_wa}).

report_vmstat(H) ->
    Result = os:cmd("vmstat 1 2"),
    Lines = string:tokens(Result, "\n"),
    Last = hd(lists:reverse(Lines)),
    Report =
    case parse_vmstat(Last) of
        undefined ->
        [];
        VM = #vmstat{} ->
        [{cpu_utilization, 100 - VM#vmstat.cpu_id},
         {cpu_iowait, VM#vmstat.cpu_wa},
         {memory_swap_in, VM#vmstat.swap_si},
         {memory_swap_out, VM#vmstat.swap_so}]
    end,
    {H, Report}.

parse_vmstat(Line) ->
    Values = string:tokens(Line, " "),
    try
        Fields = [list_to_integer(Field) || Field <- Values],
        list_to_tuple([vmstat|Fields])
    catch
        _:_ ->
            undefined
    end.
