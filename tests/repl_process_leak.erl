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

    flakey_sink(SourceNode, SinkNode),

    flakey_source(SourceNode, SinkNode),

    fail.

flakey_sink(_SourceNode, SinkNode) ->
    InitialProcCount = rpc:call(SinkNode, erlang, system_info, [process_count]),
    send_sink_tcp_errors(SinkNode, 10),

    PostProcCount = rpc:call(SinkNode, erlang, system_info, [process_count]),

    ?assertEqual(InitialProcCount, PostProcCount).

send_sink_tcp_errors(_SinkNode, 0) ->
    ok;

send_sink_tcp_errors(SinkNode, N) ->
    case rpc:call(SinkNode, riak_repl2_rtsink_conn_sup, started, []) of
        [] ->
            timer:sleep(?SEND_ERROR_INTERVAL),
            send_sink_tcp_errors(SinkNode, N);
        [P | _] ->
            P ! {tcp_error, <<>>, test},
            timer:sleep(?SEND_ERROR_INTERVAL),
            send_sink_tcp_errors(SinkNode, N - 1)
    end.

flakey_source(SourceNode, _SinkNode) ->
    InitialProcCount = rpc:call(SourceNode, erlang, system_info, [process_count]),
    send_source_tcp_errors(SourceNode, 10),

    PostProcCount = rpc:call(SourceNode, erlang, system_info, [process_count]),

    lager:info("initial: ~p; post: ~p", [InitialProcCount, PostProcCount]),
    ?assertEqual(InitialProcCount, PostProcCount).

send_source_tcp_errors(_SourceNode, 0) ->
    ok;

send_source_tcp_errors(SourceNode, N) ->
    List = rpc:call(SourceNode, riak_repl2_rtsource_conn_sup, enabled, []),
    case proplists:get_value("sink", List) of
        undefined ->
            timer:sleep(?SEND_ERROR_INTERVAL),
            send_source_tcp_errors(SourceNode, N);
        Pid ->
            Pid ! {tcp_error, <<>>, test},
            timer:sleep(?SEND_ERROR_INTERVAL),
            send_source_tcp_errors(SourceNode, N - 1)
    end.

