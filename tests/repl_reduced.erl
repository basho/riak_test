-module(repl_reduced).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    case eunit:test(?MODULE, [verbose]) of
        ok ->
            pass;
        error ->
            exit(error),
            fail
    end.

toggle_enabled_test_() ->
    {setup, fun() ->
        Nodes = rt:deploy_nodes(3, conf()),
        repl_util:make_cluster(Nodes),
        Nodes
    end,
    fun(Nodes) ->
        rt:clean_cluster(Nodes)
    end,
    fun(Nodes) -> [

        {"check default setting", fun() ->
            [Head | _] = Nodes,
            Got = rpc:call(Head, riak_repl_console, full_objects, [[]]),
            ?assertEqual(never, Got)
        end},

        {"set to always reduce", fun() ->
            [Head | _] = Nodes,
            Got = rpc:call(Head, riak_repl_console, full_objects, [["always"]]),
            ?assertEqual(always, Got),
            Got2 = rpc:call(Head, riak_repl_console, full_objects, [[]]),
            ?assertEqual(always, Got2)
        end},

        {"set to never reduce", fun() ->
            [Head | _] = Nodes,
            Got = rpc:call(Head, riak_repl_console, full_objects, [["never"]]),
            ?assertEqual(never, Got),
            Got2 = rpc:call(Head, riak_repl_console, full_objects, [[]]),
            ?assertEqual(never, Got2)
        end},

        {"set to only keep M full objects", fun() ->
            [Head | _] = Nodes,
            Got = rpc:call(Head, riak_repl_console, full_objects, [["3"]]),
            ?assertEqual(3, Got),
            Got2 = rpc:call(Head, riak_repl_console, full_objects, [[]]),
            ?assertEqual(3, Got2)
        end}

    ] end}.

-record(data_push_test, {
    nodes,
    c123,
    c456
}).

data_push_test_() ->
    {timeout, rt_cascading:timeout(1000000000000000), {setup, fun() ->
        Nodes = rt:deploy_nodes(6, conf()),
        {[N1 | _] = C123, [N4 | _] = C456} = lists:split(3, Nodes),
        repl_util:make_cluster(C123),
        repl_util:name_cluster(N1, "c123"),
        repl_util:make_cluster(C456),
        repl_util:name_cluster(N4, "c456"),
        Port = repl_util:get_cluster_mgr_port(N4),
        lager:info("attempting to connect ~p to c456 on port ~p", [N1, Port]),
        repl_util:connect_rt(N1, Port, "c456"),
        #data_push_test{nodes = Nodes, c123 = C123, c456 = C456}
    end,
    fun(State) ->
        rt:clean_cluster(State#data_push_test.nodes)
    end,
    fun(State) -> [

        {"common case: 1 real with 2 reduced", fun() ->
            #data_push_test{c123 = [N1 | _], c456 = [N4 | _]} = State,
            lager:info("setting full objects to 1"),
            rpc:call(N4, riak_repl_console, full_objects, [["1"]]),
            WaitFun = fun(Node) ->
                case rpc:call(Node, riak_repl_console, full_objects, [[]]) of
                    1 ->
                        true;
                    Uh ->
                        lager:info("~p got ~p", [Node, Uh]),
                        false
                end
            end,
            [rt:wait_until(Node, WaitFun) || Node <- State#data_push_test.c456],
            lager:info("putting an object on ~p", [N1]),
            Client123 = rt:pbc(N1),
            Bin = <<"before repl reduction, this is a binary">>,
            Key = <<"the key">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Key, Bin),
            riakc_pb_socket:put(Client123, Obj, [{w,3}]),
            riakc_pb_socket:stop(Client123),
            lager:info("Checking object on sink cluster"),
            Got = lists:map(fun(Node) ->
                lager:info("maybe eventuall exists on ~p", [Node]),
                rt_cascading:maybe_eventually_exists(Node, Bucket, Key)
            end, State#data_push_test.c456),
            ?assertEqual([Bin, Bin, Bin], Got)
        end}

    ] end}}.

conf() ->
    [{lager, [
        {handlers, [
            {lager_console_backend,info},
            {lager_file_backend, [
                {"./log/error.log",error,10485760,"$D0",5},
                {"./log/console.log",info,10485760,"$D0",5},
                {"./log/debug.log",debug,10485760,"$D0",5}
            ]}
        ]},
        {crash_log,"./log/crash.log"},
        {crash_log_msg_size,65536},
        {crash_log_size,10485760},
        {crash_log_date,"$D0"},
        {crash_log_count,5},
        {error_logger_redirect,true}
    ]},
    {riak_repl, [
        {fullsync_on_connect, false},
        {fullsync_interval, disabled},
        {diff_batch_size, 10}
    ]}].
