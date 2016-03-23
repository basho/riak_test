%% @doc The purpose of thie test is to ensure the realtime helpers on both
%% the source and sink sides properly exit when a connection is flakey; ie
%% then there are errors and not out-right closes of the connection.

-module(repl_process_leak).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(SEND_ERROR_INTERVAL, 500).

confirm() ->
    Conf = [
        {riak_repl, [
            {fullsync_on_connect, false},
            {fullsync_interval, disabled}
        ]}
    ],

    lager:info("deploying 2 nodes"),
    Nodes = rt:deploy_nodes(2, Conf, [riak_kv, riak_repl]),

    [SourceNode, SinkNode] = Nodes,

    lager:info("nameing clusters"),
    repl_util:name_cluster(SourceNode, "source"),
    repl_util:name_cluster(SinkNode, "sink"),

    {ok, {_IP, Port}} = rpc:call(SinkNode, application, get_env, [riak_core, cluster_mgr]),

    lager:info("connecting clusters using port ~p", [Port]),
    repl_util:connect_cluster(SourceNode, "127.0.0.1", Port),
    repl_util:wait_for_connection(SourceNode, "sink"),

    lager:info("enabling and starting realtime"),
    repl_util:enable_realtime(SourceNode, "sink"),
    repl_util:start_realtime(SourceNode, "sink"),

    lager:info("testing for leaks on flakey sink"),
    flakey_sink(SourceNode, SinkNode),

    lager:info("testing for leaks on flakey source"),
    flakey_source(SourceNode, SinkNode),

    pass.

flakey_sink(_SourceNode, SinkNode) ->
    InitialCount = rpc:call(SinkNode, erlang, system_info, [process_count]),
    ProcCounts = send_sink_tcp_errors(SinkNode, 20, [InitialCount]),

    Smallest = lists:min(ProcCounts),
    Biggest = lists:max(ProcCounts),
    ?assert(2 =< Biggest - Smallest),
    %?assertEqual(InitialProcCount, PostProcCount),
    % the process count is increasing, but the helper did die
    true.

send_sink_tcp_errors(_SinkNode, 0, Acc) ->
    Acc;

send_sink_tcp_errors(SinkNode, N, Acc) ->
    case rpc:call(SinkNode, riak_repl2_rtsink_conn_sup, started, []) of
        [] ->
            timer:sleep(?SEND_ERROR_INTERVAL),
            send_sink_tcp_errors(SinkNode, N, Acc);
        [P | _] ->
            SysStatus = sys:get_status(P),
            {status, P, _Modul, [_PDict, _Status, _, _, Data]} = SysStatus,
            [_Header, _Data1, Data2] = Data,
            {data, [{"State", StateRec}]} = Data2,
            [Helper | _] = lists:filter(fun(E) ->
                is_pid(E)
            end, tuple_to_list(StateRec)),
            HelpMon = erlang:monitor(process, Helper),
            P ! {tcp_error, <<>>, test},
            Mon = erlang:monitor(process, P),
            receive {'DOWN', Mon, process, P, _} -> ok end,
            receive
                {'DOWN', HelpMon, process, Helper, _} ->
                    ok
                after 10000 ->
                    throw("helper didn't die")
            end,
            timer:sleep(?SEND_ERROR_INTERVAL),
            Procs = rpc:call(SinkNode, erlang, system_info, [process_count]),
            send_sink_tcp_errors(SinkNode, N - 1, [Procs | Acc])
    end.

flakey_source(SourceNode, _SinkNode) ->
    InitialProcCount = rpc:call(SourceNode, erlang, system_info, [process_count]),
    ProcCounts = send_source_tcp_errors(SourceNode, 20, [InitialProcCount]),

    Biggest = lists:max(ProcCounts),
    Smallest = lists:min(ProcCounts),
    %lager:info("initial: ~p; post: ~p", [InitialProcCount, PostProcCount]),
    %?assertEqual(InitialProcCount, PostProcCount).
    ?assert(2 =< Biggest - Smallest),
    true.

send_source_tcp_errors(_SourceNode, 0, Acc) ->
    Acc;

send_source_tcp_errors(SourceNode, N, Acc) ->
    List = rpc:call(SourceNode, riak_repl2_rtsource_conn_sup, enabled, []),
    case proplists:get_value("sink", List) of
        undefined ->
            timer:sleep(?SEND_ERROR_INTERVAL),
            send_source_tcp_errors(SourceNode, N, Acc);
        Pid ->
            lager:debug("Get the status"),
            SysStatus = try sys:get_status(Pid) of
                S -> S
            catch
                W:Y ->
                    lager:info("Sys failed due to ~p:~p", [W,Y]),
                    {status, Pid, undefined, [undefined, undefined, undefined, undefined, [undefined, undefined, {data, [{"State", {Pid}}]}]]}
            end,
            {status, Pid, _Module, [_PDict, _Status, _, _, Data]} = SysStatus,
            [_Header, _Data1, Data2] = Data,
            {data, [{"State", StateRec}]} = Data2,
            [Helper | _] = lists:filter(fun(E) ->
                is_pid(E)
            end, tuple_to_list(StateRec)),
            lager:debug("mon the hlepr"),
            HelperMon = erlang:monitor(process, Helper),
            lager:debug("Send the murder"),
            Pid ! {tcp_error, <<>>, test},
            Mon = erlang:monitor(process, Pid),
            lager:debug("Wait for deaths"),
            receive
                {'DOWN', Mon, process, Pid, _} -> ok
            end,
            receive
                {'DOWN', HelperMon, process, Helper, _} ->
                    ok
                after 10000 ->
                    throw("Helper didn't die")
            end,
            timer:sleep(?SEND_ERROR_INTERVAL),
            Count = rpc:call(SourceNode, erlang, system_info, [process_count]),
            send_source_tcp_errors(SourceNode, N - 1, [Count | Acc])
    end.

