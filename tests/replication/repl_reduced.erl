-module(repl_reduced).

-behavior(riak_test).

-compile([export_all]).

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
        Nodes = rt_cluster:deploy_nodes(3, conf()),
        repl_util:make_cluster(Nodes),
        Nodes
    end,
    fun(Nodes) ->
        rt_cluster:clean_cluster(Nodes)
    end,
    fun(Nodes) -> [

        {"check default setting", fun() ->
            [Head | _] = Nodes,
            Got = rpc:call(Head, riak_repl_console, full_objects, [[]]),
            ?assertEqual(always, Got)
        end},

        {"set to always have full objects", fun() ->
            [Head | _] = Nodes,
            Got = rpc:call(Head, riak_repl_console, full_objects, [["always"]]),
            ?assertEqual(always, Got),
            Got2 = rpc:call(Head, riak_repl_console, full_objects, [[]]),
            ?assertEqual(always, Got2)
        end},

        {"set to never have full objects", fun() ->
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

data_push() ->
    Tests = data_push_test_(),
    case eunit:test(Tests, [verbose]) of
        ok ->
            pass;
        error ->
            exit(error),
            fail
    end.

data_push_test_() ->
    {timeout, rt_cascading:timeout(1000000000000000), {setup, fun() ->
        Nodes = rt_cluster:deploy_nodes(6, conf()),
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
        case rt_config:config_or_os_env(skip_teardown, false) of
            "false" ->
                rt_cluster:clean_cluster(State#data_push_test.nodes);
            false ->
                rt_cluster:clean_cluster(State#data_push_test.nodes);
            _ ->
                ok
        end
    end,
    fun(State) -> [

        {"repl works", timeout, rt_cascading:timeout(1000), fun() ->
            #data_push_test{c123 = [N1 | _]} = State,
            Client123 = rt_pb:pbc(N1),
            Bin = <<"data data data">>,
            Key = <<"derkey">>,
            Bucket = <<"kicked">>,
            Obj = riakc_obj:new(Bucket, Key, Bin),
            riakc_pb_socket:put(Client123, Obj, [{w,3}]),
            riakc_pb_socket:stop(Client123),
            Got = [maybe_eventually_exists(Node, Bucket, Key) || Node <- State#data_push_test.c456],
            ?assertMatch([{ok, _Obj}, {ok, _Obj}, {ok, _Obj}], Got)
        end},

        {"common case: 1 real with 2 reduced", timeout, rt_cascading:timeout(1000), fun() ->
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
            Client123 = rt_pb:pbc(N1),
            Bin = <<"before repl reduction, this is a binary">>,
            Key = <<"the key">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Key, Bin),
            riakc_pb_socket:put(Client123, Obj, [{w,3}]),
            riakc_pb_socket:stop(Client123),
            lager:info("Checking object on sink cluster"),
            Got = lists:map(fun(Node) ->
                lager:info("maybe eventualliy exists on ~p", [Node]),
                maybe_eventually_exists(Node, Bucket, Key)
            end, State#data_push_test.c456),
            ?assertMatch([{ok, _}, {ok, _}, {ok, _}], Got),
            lists:map(fun({ok, GotObj}) ->
                Value = riakc_obj:get_value(GotObj),
                ?assertEqual(Bin, Value)
            end, Got)
        end},

        {"only carry reduced objects, no proxy provider though", timeout, rt_cascading:timeout(1000), fun() ->
            #data_push_test{c123 = [N1 | _], c456 = [N4 | _]} = State,
            lager:info("setting full objects to never"),
            rpc:call(N4, riak_repl_console, full_objects, [["never"]]),
            WaitFun = fun(Node) ->
                Got = rpc:call(Node, riak_repl_console, full_objects, [[]]),
                Got =:= never
            end,
            [rt:wait_until(Node, WaitFun) || Node <- State#data_push_test.c456],
            Client123 = rt_pb:pbc(N1),
            Bin = <<"only carry reduced objects">>,
            Key = <<"ocro">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Key, Bin),
            riakc_pb_socket:put(Client123, Obj, [{w,3}]),
            riakc_pb_socket:stop(Client123),
            Got = lists:map(fun(Node) ->
                maybe_eventually_exists(Node, Bucket, Key)
            end, State#data_push_test.c456),
            ?assertMatch([{error, notfound}, {error, notfound}, {error, notfound}], Got)
        end},

        {"only carry reduced objects, has a proxy provider", timeout, rt_cascading:timeout(1000), fun() ->
            #data_push_test{c123 = [N1 | _], c456 = [N4 | _]} = State,
            lager:info("setting full objects to never"),
            rpc:call(N4, riak_repl_console, full_objects, [["never"]]),
            lager:info("setting proxy provider at c123 for c456"),
            rpc:call(N1, riak_repl_console, proxy_get, [["enable", "c456"]]),
            WaitFun = fun(Node) ->
                Got = rpc:call(Node, riak_repl_console, full_objects, [[]]),
                Got =:= never
            end,
            [rt:wait_until(Node, WaitFun) || Node <- State#data_push_test.c456],
            Client123 = rt_pb:pbc(N1),
            Bin = <<"only carry reduced objects">>,
            Key = <<"ocro2">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Key, Bin),
            riakc_pb_socket:put(Client123, Obj, [{w,3}]),
            riakc_pb_socket:stop(Client123),
            Got = lists:map(fun(Node) ->
                maybe_eventually_exists(Node, Bucket, Key)
            end, State#data_push_test.c456),
            ?assertMatch([{ok, _}, {ok, _}, {ok, _}], Got),
            lists:map(fun({ok, GotObj}) ->
                Value = riakc_obj:get_value(GotObj),
                ?assertEqual(Bin, Value)
            end, Got)
        end}



    ] end}}.

read_repair_interaction() ->
    Tests = read_repair_interaction_test_(),
    case eunit:test(Tests, [verbose]) of
        ok -> pass;
        error -> exit(error), fail
    end.

read_repair_interaction_test_() ->
    {timeout, rt_cascading:timeout(100000), {setup, fun() ->
        Nodes = rt_cluster:deploy_nodes(6, conf()),
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
        case rt_config:config_or_os_env(skip_teardown, false) of
            "false" ->
                rt_cluster:clean_cluster(State#data_push_test.nodes);
            false ->
                rt_cluster:clean_cluster(State#data_push_test.nodes);
            _ ->
                ok
        end
    end,
    fun(State) -> [

        {"load up sink cluster", timeout, rt_cascading:timeout(100000), fun() ->
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
            Client123 = rt_pb:pbc(N1),
            Bin = <<"before repl reduction, this is a binary">>,
            Key = <<"rrit">>,
            Bucket = <<"rrit_objects">>,
            Obj = riakc_obj:new(Bucket, Key, Bin),
            riakc_pb_socket:put(Client123, Obj, [{w,3}]),
            riakc_pb_socket:stop(Client123),
            lager:info("Checking object on sink cluster"),
            Got = lists:map(fun(Node) ->
                lager:info("maybe eventualliy exists on ~p", [Node]),
                maybe_eventually_exists(Node, Bucket, Key)
            end, State#data_push_test.c456),
            ?assertMatch([{ok, _}, {ok, _}, {ok, _}], Got),
            lists:map(fun({ok, GotObj}) ->
                Value = riakc_obj:get_value(GotObj),
                ?assertEqual(Bin, Value)
            end, Got)
        end},

        {"read repair in the small", timeout, rt_cascading:timeout(1000000), fun() ->
            Key = <<"rrit">>,
            Bucket = <<"rrit_objects">>,
            Bin = <<"before repl reduction, this is a binary">>,
            riak_repl_reduced_intercepts:register_as_target(),
            lists:map(fun(Node) ->
                rt_intercept:add(Node, {riak_repl_reduced, [
                    {{mutate_get, 1}, report_mutate_get},
                    {{mutate_put, 5}, report_mutate_put}
                ]})
            end, State#data_push_test.c456),
            [N4 | _] = State#data_push_test.c456,
            Client456 = rt_pb:pbc(N4),

            % set the nval higher, which make the below have read repair
            % end up being forced
            riakc_pb_socket:set_bucket(Client456, Bucket, [{n_val, 5}]),

            % getting the vlaud once with a smaller pr should trigger the
            % read repair, which should have puts.
            riakc_pb_socket:get(Client456, Bucket, Key, [{pr, 3}]),

            % the n_val is 5, but there's only 3 actual data's. This means
            % there should be 5 total mutate_gets (1 real, 2 promoise, 
            % 2 from promise getting real)
            MutateGets1 = riak_repl_reduced_intercepts:get_all_reports(),
            ?assertEqual(5, length(MutateGets1)),
            % 3 of those gets are from the primary, so they are exactly
            % the same
            ?assertEqual(3, length(lists:usort(MutateGets1))),
            % and since 3 of those are the real, we should have 3 copies
            % of the full bin if we concat all the values together.
            ExpectedValue1 = <<Bin/binary, Bin/binary, Bin/binary>>,
            GotValue1 = lists:foldl(fun({_Node, _Pid, Object}, Acc) ->
                Value = riak_object:get_value(Object),
                <<Acc/binary, Value/binary>>
            end, <<>>, MutateGets1),
            ?assertEqual(ExpectedValue1, GotValue1),

            % and then 2 mutate puts due t
            MutatePuts1 = riak_repl_reduced_intercepts:put_all_reports(),
            ?assertEqual(2, length(MutatePuts1)),

            % next time a get is done, 5 vnodes will reply with found
            % data, 4 of those being reduced. This means there are 9
            % totoal get requests.
            riakc_pb_socket:get(Client456, Bucket, Key),
            MutateGets2 = riak_repl_reduced_intercepts:get_all_reports(),
            ?assertEqual(9, length(MutateGets2)),
            % five of those (1 from request directly, 4 from promise objs)
            % are get requests to the primary.
            ?assertEqual(5, length(lists:usort(MutateGets2))),
            ExpectedValue2 = lists:foldl(fun(_, Acc) ->
                <<Acc/binary, Bin/binary>>
            end, <<>>, lists:seq(1, 5)),
            GotValue2 = lists:foldl(fun({_Node, _Pid, Object}, Acc) ->
                Value = riak_object:get_value(Object),
                <<Acc/binary, Value/binary>>
            end, <<>>, MutateGets2),
            ?assertEqual(ExpectedValue2, GotValue2)
        end},

        {"read repair interacton in the large", timeout, rt_cascading:timeout(1000000), fun() ->
            Bucket = <<"rrit_objects_lots">>,
            Bin = <<"datadatadata">>,
            % to avoid overloading the message queue here, we're going
            % to reset the intercepts
            lists:map(fun(Node) ->
                rt_intercept:add(Node, {riak_repl_reduced, []})
            end, State#data_push_test.c456),

            FillSize = case rt_config:config_or_os_env(fill_size, 1000) of
                FS when is_integer(FS) ->
                    FS;
                FS when is_list(FS) ->
                    list_to_integer(FS)
            end,
            % Fill the source node and let the sink get it all
            lager:info("filling c123 cluster"),
            write(State#data_push_test.c123, 1, FillSize, Bucket, Bin),
            WaitForRtqEmptyFun = fun(Node) ->
                rpc:call(Node, riak_repl2_rtq, all_queues_empty, [])
            end,
            lists:map(fun(N) ->
                rt:wait_until(N, WaitForRtqEmptyFun)
            end, State#data_push_test.c123),

            % read all the things, ensuring read-repair happens
            % as expected
            lists:map(fun(Node) ->
                rt_intercept:add(Node, {riak_repl_reduced, [
                    {{mutate_get, 1}, report_mutate_get},
                    {{mutate_put, 5}, report_mutate_put}
                ]})
            end, State#data_push_test.c456),
            riak_repl_reduced_intercepts:register_as_target(),
            riak_repl_reduced_intercepts:put_all_reports(),
            riak_repl_reduced_intercepts:get_all_reports(),

            AssertFirstRead = fun
                (Key, {ok, _Obj}) ->
                    MutateGets = riak_repl_reduced_intercepts:get_all_reports(),
                    ?assertEqual(5, length(MutateGets)),
                    ?assertEqual(3, length(lists:usort(MutateGets))),
                    ExpectValue = <<Bin/binary, Bin/binary, Bin/binary>>,
                    GotValue = lists:foldl(fun({_Node, _Pid, Object}, Acc) ->
                        Value = case riak_object:get_values(Object) of
                            [V] -> V;
                            [V | _] = Values ->
                                %lager:warning("key ~p has mutltiple values in the object: ~p", [Key, Object]),
                                lager:warning("(First Read) Double check {~p, ~p} as reduced n mutator got multiple values", [Bucket, Key]),
                                lists:foldl(fun(B,Ba) ->
                                    ?assertEqual(V, B),
                                    <<B/binary, Ba/binary>>
                                end, <<>>, Values)
                        end,
                        <<Acc/binary, Value/binary>>
                    end, <<>>, MutateGets),
                    ?assertEqual(ExpectValue, GotValue),
                    MutatePuts = riak_repl_reduced_intercepts:put_all_reports(),
                    ?assertEqual(2, length(MutatePuts));
                (Key, Error) ->
                    lager:warning("Didn't get ~p as expected: ~p", [Key, Error]),
                    ?assertMatch({ok, _}, Error)
            end,

            AssertSecondRead = fun
                (Key, {ok, _Obj}) ->
                    MutateGets = riak_repl_reduced_intercepts:get_all_reports(),
                    ?assertEqual(9, length(MutateGets)),
                    ?assertEqual(5, length(lists:usort(MutateGets))),
                    ExpectValue = <<Bin/binary, Bin/binary, Bin/binary, Bin/binary, Bin/binary>>,
                    GotValue = lists:foldl(fun({_Node, _Pid, Object}, Acc) ->
                        Value = case riak_object:get_values(Object) of
                            [V] -> V;
                            [V | _] = Values ->
                                %lager:warning("Key ~p has multiple values in the object ~p (second read asserts)", [Key, Object]),
                                lager:warning("(Second Read) Double check {~p, ~p} as reduced n mutator got multiple values", [Bucket, Key]),
                                lists:foldl(fun(B,Ba) ->
                                    ?assertEqual(V, B),
                                    <<B/binary, Ba/binary>>
                                end, <<>>, Values)
                        end,
                        <<Acc/binary, Value/binary>>
                    end, <<>>, MutateGets),
                    ?assertEqual(ExpectValue, GotValue),
                    MutatePuts = riak_repl_reduced_intercepts:put_all_reports(),
                    ?assertEqual(0, length(MutatePuts));
                (Key, Error) ->
                    lager:warning("Didn't get ~p as expected: ~p", [Key, Error]),
                    ?assertMatch({ok, _}, Error)
            end,

            Client456 = rt_pb:pbc(hd(State#data_push_test.c456)),
            riakc_pb_socket:set_bucket(Client456, Bucket, [{n_val, 5}]),
            riakc_pb_socket:stop(Client456),

            lager:info("assert first read group"),
            read(State#data_push_test.c456, 1, FillSize, Bucket, AssertFirstRead),
            lager:info("assert second read group"),
            read(State#data_push_test.c456, 1, FillSize, Bucket, AssertSecondRead)
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

exists(Nodes, Bucket, Key) ->
    exists({error, notfound}, Nodes, Bucket, Key).

exists(Got, [], _Bucket, _Key) ->
    Got;
exists({error, notfound}, [Node | Tail], Bucket, Key) ->
    Pid = rt_pb:pbc(Node),
    Got = riakc_pb_socket:get(Pid, Bucket, Key, [{pr, 1}]),
    riakc_pb_socket:stop(Pid),
    exists(Got, Tail, Bucket, Key);
exists(Got, _Nodes, _Bucket, _Key) ->
    Got.

maybe_eventually_exists(Node, Bucket, Key) ->
    Timeout = rt_cascading:timeout(10),
    WaitTime = rt_config:get(default_wait_time, 1000),
    maybe_eventually_exists(Node, Bucket, Key, Timeout, WaitTime).

%maybe_eventually_exists(Node, Bucket, Key, Timeout) ->
%    WaitTime = rt_config:get(default_wait_time, 1000),
%    maybe_eventually_exists(Node, Bucket, Key, Timeout, WaitTime).

maybe_eventually_exists(Node, Bucket, Key, Timeout, WaitMs) when is_atom(Node) ->
    maybe_eventually_exists([Node], Bucket, Key, Timeout, WaitMs);

maybe_eventually_exists(Nodes, Bucket, Key, Timeout, WaitMs) ->
    Got = exists(Nodes, Bucket, Key),
    maybe_eventually_exists(Got, Nodes, Bucket, Key, Timeout, WaitMs).

maybe_eventually_exists({error, notfound}, Nodes, Bucket, Key, Timeout, WaitMs) when Timeout > 0 ->
    ?debugMsg("not found, waiting again"),
    timer:sleep(WaitMs),
    Got = exists(Nodes, Bucket, Key),
    Timeout2 = case Timeout of
        infinity ->
            infinity;
        _ ->
            Timeout - WaitMs
    end,
    maybe_eventually_exists(Got, Nodes, Bucket, Key, Timeout2, WaitMs);

maybe_eventually_exists({ok, _RiakObj} = Out, _Nodes, _Bucket, _Key, _Timeout, _WaitMs) ->
    Out;

maybe_eventually_exists(Got, _Nodes, _Bucket, _Key, _Timeout, _WaitMs) ->
    Got.

write(NodeList, Start, Stop, Bucket, Data) when is_list(NodeList) ->
    SocketQueue = make_socket_queue(NodeList),
    write(SocketQueue, Start, Stop, Bucket, Data);

write(SocketQueue, N, Stop, _Bucket, _Data) when N > Stop ->
    stop_socket_queue(SocketQueue),
    ok;

write(SocketQueue, N, Stop, Bucket, Data) ->
    {Socket, NextQueue} = next_socket(SocketQueue),
    Key = list_to_binary(integer_to_list(N)),
    Obj = riakc_obj:new(Bucket, Key, Data),
    riakc_pb_socket:put(Socket, Obj, [{w, 3}]),
    NextN = N + 1,
    write(NextQueue, NextN, Stop, Bucket, Data).

read(NodeList, Start, Stop, Bucket, AssertFun) when is_list(NodeList) ->
    SocketQueue = make_socket_queue(NodeList),
    read(SocketQueue, Start, Stop, Bucket, AssertFun);

read(SocketQueue, N, Stop, _Bucket, _AssertFun) when N > Stop ->
    stop_socket_queue(SocketQueue),
    ok;

read(SocketQueue, N, Stop, Bucket, AssertFun) ->
    {Socket, SocketQueue2} = next_socket(SocketQueue),
    Key = list_to_binary(integer_to_list(N)),
    Got = riakc_pb_socket:get(Socket, Bucket, Key, [{pr, 3}]),
    AssertFun(Key, Got),
    NextN = N + 1,
    read(SocketQueue2, NextN, Stop, Bucket, AssertFun).


make_socket_queue(Nodes) ->
    Sockets = lists:map(fun(Node) ->
        rt_pb:pbc(Node)
    end, Nodes),
    queue:from_list(Sockets).

stop_socket_queue(SocketQueue) ->
    Sockets = queue:to_list(SocketQueue),
    lists:map(fun(Socket) ->
        riakc_pb_socket:stop(Socket)
    end, Sockets).

next_socket(SocketQueue) ->
    {{value, Socket}, ShorterQueue} = queue:out(SocketQueue),
    NextQueue = queue:in(Socket, ShorterQueue),
    {Socket, NextQueue}.

