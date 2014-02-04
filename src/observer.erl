-module(observer).
-compile(export_all).

-record(history, {network,
                  disk,
                  rate,
                  nodes,
                  lvlref,
                  collector_sock,
                  collector_host,
                  collector_port}).

-record(watcher, {nodes,
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
    %% io:format("Loading on ~p~n", [Nodes]),
    %% load_modules_on_nodes([?MODULE], Nodes),
    %% R = rpc:multicall(Nodes, ?MODULE, start, [self(), 1000, Collector, Nodes, collect]),
    %% io:format("RPC: ~p~n", [R]),
    spawn(?MODULE, watcher, [self(), Nodes, Collector]),
    start(self(), 1000, Collector, Nodes, ping),
    ok.

watcher(Master, Nodes, Collector) ->
    monitor(process, Master),
    Probes = [{Node, undefined} || Node <- Nodes],
    W = #watcher{nodes=Nodes,
                 collector=Collector,
                 probes=Probes},
    watcher_loop(W).

watcher_loop(W=#watcher{probes=Probes}) ->
    Missing = [Node || {Node, undefined} <- Probes],
    %% io:format("Missing: ~p~n", [Missing]),
    W2 = install_probes(Missing, W),
    Probes2 = W2#watcher.probes,
    receive
        {'DOWN', MRef, process, _, _} ->
            case lists:keyfind(MRef, 2, Probes2) of
                false ->
                    %% master died, exit
                    io:format("watcher exiting~n"),
                    ok;
                {Node, MRef} ->
                    io:format("Probe exit: ~p/~p~n", [Node, MRef]),
                    Probes3 = lists:keyreplace(Node, 1, Probes2, {Node, undefined}),
                    W3 = W2#watcher{probes=Probes3},
                    ?MODULE:watcher_loop(W3)
            end
    after 1000 ->
            ?MODULE:watcher_loop(W2)
    end.

install_probes(Nodes, W=#watcher{collector=Collector, nodes=AllNodes, probes=Probes}) ->
    %% io:format("Loading on ~p~n", [Nodes]),
    load_modules_on_nodes([?MODULE], Nodes),
    R = rpc:multicall(Nodes, ?MODULE, start, [self(), 1000, Collector, AllNodes, collect]),
    %% io:format("R: ~p~n", [R]),
    {Pids, Down} = R,
    %% io:format("I: ~p/~p~n", [Pids, Down]),
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
    %% io:format("P3: ~p~n", [Probes3]),
    W#watcher{probes=Probes3}.

start(Master, Rate, Collector, Nodes, Fun) ->
    io:format("In start: ~p~n", [node()]),
    Pid = spawn(?MODULE, init, [Master, Rate, Collector, Nodes, Fun]),
    {node(), Pid}.

init(Master, Rate, {Host, Port}, Nodes, Fun) ->
    io:format("In init: ~p~n", [node()]),
    {ok, Sock} = gen_udp:open(Port),
    case application:get_env(riak_kv, storage_backend) of
        {ok, riak_kv_eleveldb_backend} ->
            LRef = get_leveldb_ref();
        _ ->
            LRef = undefined
    end,
    H = #history{network=undefined,
                 %% disk=undefined,
                 disk=[],
                 rate=Rate div 1000,
                 lvlref=LRef,
                 nodes=Nodes,
                 collector_sock=Sock,
                 collector_host=Host,
                 collector_port=Port},
    %% case Fun of
    %%     collect ->
    %%         vmstat(Master, H);
    %%     _ ->
    %%         ok
    %% end,
    monitor(process, Master),
    loop(Fun, Rate, H).

loop(Fun, Rate, H) ->
    %% io:format("loop: ~p~n", [node()]),
    NewH = ?MODULE:Fun(H),
    receive
        {'DOWN', _, process, _, _} ->
            io:format("shutting: ~p~n", [node()]),
            ok
    after Rate ->
            ?MODULE:loop(Fun, Rate, NewH)
    end.

ping(H=#history{nodes=Nodes}) ->
    TS = timestamp(),
    XNodes = lists:zip(lists:seq(1, length(Nodes)), Nodes),
    pmap(fun({X,Node}) ->
                 case net_adm:ping(Node) of
                     pang ->
                         notify_down(TS, X, Node, H),
                         ok;
                     pong ->
                         case rpc:call(Node, riak_core_node_watcher, services, [Node]) of
                             L when is_list(L) ->
                                 case lists:member(riak_kv, L) of
                                     true ->
                                         ok;
                                     false ->
                                         notify_down(TS, X, Node, H)
                                 end;
                             _ ->
                                 notify_down(TS, X, Node, H)
                         end;
                     _ ->
                         ok
                 end
         end, XNodes),
    H.

notify_down(TS, X, Node, H) ->
    %% emit_stat(Stat, TS, Value, H) ->
    NodeBin = atom_to_binary(Node, utf8),
    Metric = <<"offline_nodes/", NodeBin/binary>>,
    emit_stat2(Metric, TS, X, H).

collect(H0) ->
    H = try report_leveldb(H0) catch _:_ -> H0 end,
    catch report_queues(H),
    catch report_processes(H),
    H2 = try report_network(H) catch _:_ -> H end,
    %% H3 = report_disk2(H2),
    %% H3 = report_disk2([{<<"dm-0">>, "dm-0"},
    %%                    {<<"dm-1">>, "dm-1"}], H2),
    H3 = report_disk2([{<<"xvdb">>, "xvdb"},
                       {<<"xvdc">>, "xvdc"},
                       {<<"raid0">>, "md127"}], H2),
    report_vmstat(H2),
    report_memory(H2),
    %% H3 = try report_disk2(H2) catch _:_ -> H2 end,
    catch report_stats(riak_core_stat, [dropped_vnode_requests_total], H3),
    catch report_stats(riak_kv_stat,
                       [node_gets,
                        node_puts,

                        vnode_gets,
                        vnode_puts,

                        node_get_fsm_active,
                        node_get_fsm_rejected,
                        node_get_fsm_in_rate,
                        node_get_fsm_out_rate,

                        node_put_fsm_active,
                        node_put_fsm_rejected,
                        node_put_fsm_in_rate,
                        node_put_fsm_out_rate
                       ], H3),

    catch report_stats(riak_kv_stat,
                       [riak_kv_stat,
                        node_get_fsm_time_median,
                        node_get_fsm_time_95,
                        node_get_fsm_time_100,

                        node_put_fsm_time_median,
                        node_put_fsm_time_95,
                        node_put_fsm_time_100
                       ], H3, 1000),

    %% catch print_down(Nodes),
    H3.

report_queues(H) ->
    Max = lists:max([Len || Pid <- processes(),
                            {message_queue_len, Len} <- [process_info(Pid, message_queue_len)]]),
    TS = timestamp(),
    emit_stat(<<"message_queue_max">>, TS, Max, H),
    ok.

%% report_queues(Threshold) ->
%%     VNodes = riak_core_vnode_manager:all_vnodes(),
%%     VNodes2 = [{Pid, {Mod,Idx}} || {Mod,Idx,Pid} <- VNodes],
%%     VNodeMap = dict:from_list(VNodes2),
%%     Queues = message_queues(processes(), Threshold, VNodeMap, []),
%%     Queues2 = lists:keysort(1, filter(Queues, 2, [])),
%%     ok.

report_processes(H) ->
    Procs = erlang:system_info(process_count),
    %% Limit = erlang:system_info(process_limit),
    %% Ratio = Procs * 100 div Limit,
    TS = timestamp(),
    emit_stat(<<"erlang_processes">>, TS, Procs, H),
    ok.

%% report_processes(Threshold) ->
%%     Procs = erlang:system_info(process_count),
%%     Limit = erlang:system_info(process_limit),
%%     Ratio = Procs * 100 div Limit,
%%     case Ratio > Threshold of
%%         true ->
%%             {Procs, Ratio};
%%         false ->
%%             none
%%     end.

report_network(H=#history{network=LastStats, rate=Rate}) ->
    {RX, TX} = get_network(),
    case LastStats of
        undefined ->
            ok;
        {LastRX, LastTX} ->
            RXRate = net_rate(LastRX, RX) div Rate,
            TXRate = net_rate(LastTX, TX) div Rate,
            TS = timestamp(),
            emit_stat(<<"net_rx">>, TS, RXRate, H),
            emit_stat(<<"net_tx">>, TS, TXRate, H)
    end,
    H#history{network={RX, TX}}.

report_disk2(Disks, H=#history{disk=DiskStats}) ->
    NewStats =
        lists:foldl(fun({Name, Dev}, Acc) ->
                            LastStats = case orddict:find(Dev, DiskStats) of
                                            error ->
                                                undefined;
                                            {ok, LS} ->
                                                LS
                                        end,
                            Stats = report_disk2(Name, Dev, LastStats, H),
                            orddict:store(Dev, Stats, Acc)
                    end, DiskStats, Disks),
    H#history{disk=NewStats}.

report_disk2(Name, Dev, LastStats, H=#history{rate=Rate}) ->
    Stats = get_disk2(Dev),
    case LastStats of
        undefined ->
            ok;
        _ ->
            ReadRate = disk_rate(#disk.read_sectors, LastStats, Stats) div Rate,
            WriteRate = disk_rate(#disk.write_sectors, LastStats, Stats) div Rate,
            {AwaitR, AwaitW} = disk_await(LastStats, Stats),
            Svctime = disk_svctime(LastStats, Stats),
            QueueLen = disk_qlength(LastStats, Stats),
            Util = disk_util(LastStats, Stats),
            TS = timestamp(),
            emit_stat(<<"disk_read (", Name/binary, ")">>, TS, ReadRate, H),
            emit_stat(<<"disk_write (", Name/binary, ")">>, TS, WriteRate, H),
            emit_stat(<<"disk_await_r (", Name/binary, ")">>, TS, AwaitR, H),
            emit_stat(<<"disk_await_w (", Name/binary, ")">>, TS, AwaitW, H),
            emit_stat(<<"disk_svctime (", Name/binary, ")">>, TS, Svctime, H),
            emit_stat(<<"disk_queue_size (", Name/binary, ")">>, TS, QueueLen, H),
            emit_stat(<<"disk_utilization (", Name/binary, ")">>, TS, Util, H)
    end,
    Stats.

report_disk2(H=#history{disk=LastStats, rate=Rate}) ->
    Stats = get_disk2(),
    case LastStats of
        undefined ->
            ok;
        _ ->
            ReadRate = disk_rate(#disk.read_sectors, LastStats, Stats) div Rate,
            WriteRate = disk_rate(#disk.write_sectors, LastStats, Stats) div Rate,
            {AwaitR, AwaitW} = disk_await(LastStats, Stats),
            Svctime = disk_svctime(LastStats, Stats),
            QueueLen = disk_qlength(LastStats, Stats),
            Util = disk_util(LastStats, Stats),
            TS = timestamp(),
            emit_stat(<<"disk_read">>, TS, ReadRate, H),
            emit_stat(<<"disk_write">>, TS, WriteRate, H),
            emit_stat(<<"disk_await_r">>, TS, AwaitR, H),
            emit_stat(<<"disk_await_w">>, TS, AwaitW, H),
            emit_stat(<<"disk_svctime">>, TS, Svctime, H),
            emit_stat(<<"disk_queue_size">>, TS, QueueLen, H),
            emit_stat(<<"disk_utilization">>, TS, Util, H)
    end,
    H#history{disk=Stats}.

%% report_disk(H=#history{disk=LastStats, rate=Rate}) ->
%%     {Read, Write} = get_disk(),
%%     case LastStats of
%%         undefined ->
%%             ok;
%%         {LastRead, LastWrite} ->
%%             ReadRate = disk_rate(LastRead, Read) div Rate,
%%             WriteRate = disk_rate(LastWrite, Write) div Rate,
%%             TS = timestamp(),
%%             emit_stat(<<"disk_read">>, TS, ReadRate, H),
%%             emit_stat(<<"disk_write">>, TS, WriteRate, H)
%%     end,
%%     H#history{disk={Read, Write}}.

report_memory(H) ->
    Stats = get_memory(),
    Util = memory_util(Stats),
    Dirty = memory_dirty(Stats),
    Writeback = memory_writeback(Stats),
    TS = timestamp(),
    emit_stat(<<"memory_utilization">>, TS, Util, H),
    emit_stat(<<"memory_page_dirty">>, TS, Dirty, H),
    emit_stat(<<"memory_page_writeback">>, TS, Writeback, H),
    ok.

report_leveldb(H=#history{lvlref=undefined}) ->
    H;
report_leveldb(H=#history{lvlref=LRef}) ->
    try case eleveldb:status(LRef, <<"leveldb.ThrottleGauge">>) of
            {ok, Result} ->
                Value = list_to_integer(binary_to_list(Result)),
                TS = timestamp(),
                emit_stat(<<"leveldb_write_throttle">>, TS, Value, H),
                H;
            _ ->
                H
        end
    catch
        _:_ ->
            LRef2 = get_leveldb_ref(),
            H#history{lvlref=LRef2}
    end.

%% print_down(Nodes) ->
%%     Down = [Node || Node <- Nodes,
%%                     net_adm:ping(Node) =:= pang],
%%     case Down of
%%         [] ->
%%             ok;
%%         _ ->
%%             io:format("Offline nodes:~n  ~p~n", [Down])
%%     end.

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
    Wait * 100 div 1000. %% Really should be div Rate
    
disk_qlength(S1, S2) ->
    (S2#disk.io_wait_weighted_ms - S1#disk.io_wait_weighted_ms) div 1000.

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
    %% {ok, RX} = file:read_file("/sys/class/net/eth0/statistics/rx_bytes"),
    %% {ok, TX} = file:read_file("/sys/class/net/eth0/statistics/tx_bytes"),
    {ok, RX} = file:read_file("/sys/class/net/eth1/statistics/rx_bytes"),
    {ok, TX} = file:read_file("/sys/class/net/eth1/statistics/tx_bytes"),
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

report_stats(Mod, Keys, H) ->
    report_stats(Mod, Keys, H, 1).

report_stats(Mod, Keys, H, Scale) ->
    Stats = Mod:get_stats(),
    TS = timestamp(),
    [case lists:keyfind(Key, 1, Stats) of
         false ->
             ok;
         {_, Value} ->
             emit_stat(atom_to_binary(Key, utf8), TS, Value / Scale, H)
     end || Key <- Keys],
    ok.

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
             rpc:multicall(Nodes, code, load_binary, [Module, File, Bin]);
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

%% print_queues(Nodes) ->
%%     pmap(remote(get_queues), Nodes).

%% remote(F) ->
%%     fun(Node) ->
%%             rpc:call(Node, ?MODULE, F, [])
%%     end.

timestamp() ->
    timestamp(os:timestamp()).

timestamp({Mega, Secs, Micro}) ->
    Mega*1000*1000*1000 + Secs * 1000 + (Micro div 1000).

emit_stat(Stat, TS, Value, H) ->
    NodeBin = atom_to_binary(node(), utf8),
    %% Metric = <<NodeBin/binary, ":", Stat/binary>>,
    Metric = <<Stat/binary, "/", NodeBin/binary>>,
    emit_stat2(Metric, TS, Value, H).

emit_stat2(Metric, TS, Value, #history{collector_sock=Sock,
                                       collector_host=Host,
                                       collector_port=Port}) ->
    if is_integer(Value) ->
            Packet = <<"=", TS:64/integer, Value:64/integer, Metric/binary>>,
            %% io:format("Sending: ~p~n", [{TS, Value, Metric}]),
            gen_udp:send(Sock, Host, Port, Packet);
       is_float(Value) ->
            %% IValue = erlang:trunc(Value),
            %% Packet = <<"=", TS:64/integer, IValue:64/integer, Metric/binary>>,
            %% %% io:format("Sending: ~p~n", [{TS, Value, Metric}]),
            Packet = <<"#", (term_to_binary({Value, Metric, TS}))/binary>>,
            gen_udp:send(Sock, Host, Port, Packet);
       true ->
            io:format("NT: ~p~n", [Value])
    end,
    ok.


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
    case parse_vmstat(Last) of
        undefined ->
            ok;
        VM = #vmstat{} ->
            TS = timestamp(),
            emit_stat(<<"cpu_utilization">>, TS, 100 - VM#vmstat.cpu_id, H),
            emit_stat(<<"cpu_iowait">>, TS, VM#vmstat.cpu_wa, H),
            emit_stat(<<"memory_swap_in">>, TS, VM#vmstat.swap_si, H),
            emit_stat(<<"memory_swap_out">>, TS, VM#vmstat.swap_so, H)
    end,
    ok.

vmstat(Master, H) ->
    spawn(fun() ->
                  monitor(process, Master),
                  Port = open_port({spawn, "vmstat 1"}, [{line,4096}, out]),
                  vmstat_loop(Port, H)
          end).

parse_vmstat(Line) ->
    Values = string:tokens(Line, " "),
    try
        Fields = [list_to_integer(Field) || Field <- Values],
        list_to_tuple([vmstat|Fields])
    catch
        _:_ ->
            undefined
    end.

vmstat_loop(Port, H) ->
    receive
        {'DOWN', _, process, _, _} ->
            ok;
        {Port, {data, Line}} ->
            case parse_vmstat(Line) of
                undefined ->
                    ok;
                VM = #vmstat{} ->
                    TS = timestamp(),
                    emit_stat(<<"cpu_utilization">>, TS, 100 - VM#vmstat.cpu_id, H),
                    emit_stat(<<"cpu_iowait">>, TS, VM#vmstat.cpu_wa, H),
                    emit_stat(<<"memory_swap_in">>, TS, VM#vmstat.swap_si, H),
                    emit_stat(<<"memory_swap_out">>, TS, VM#vmstat.swap_so, H)
            end,
            vmstat_loop(Port, H)
    end.
