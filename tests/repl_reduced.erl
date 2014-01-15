-module(repl_reduced).

-behavior(riak_test).

-compile([export_all]).

-export([confirm/0]).
% individual tests
-export([toggle_enabled/0, data_push/0, aae/0]).

-define(default_fill, 300).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    eunit(?MODULE).

toggle_enabled() ->
    eunit(toggle_enabled_test_()).

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
    eunit(data_push_test_()).

data_push_test_() ->
    {timeout, rt_cascading:timeout(100), {setup, fun() ->
        Nodes = rt:deploy_nodes(6, conf()),
        {[N1 | _] = C123, [N4 | _] = C456} = lists:split(3, Nodes),
        repl_util:make_cluster(C123),
        repl_util:name_cluster(N1, "c123"),
        repl_util:make_cluster(C456),
        repl_util:name_cluster(N4, "c456"),
        Port = repl_util:get_cluster_mgr_port(N4),
        lager:info("attempting to connect ~p to c456 on port ~p", [N1, Port]),
        connect_rt(N1, Port, "c456"),
        #data_push_test{nodes = Nodes, c123 = C123, c456 = C456}
    end,
    fun(State) ->
        case rt_config:config_or_os_env(skip_teardowns, false) of
            false ->
                rt:clean_cluster(State#data_push_test.nodes);
            _ ->
                ok
        end
    end,
    fun(State) -> [

        {"repl works", timeout, rt_cascading:timeout(1000), fun() ->
            #data_push_test{c123 = [N1 | _]} = State,
            Client123 = rt:pbc(N1),
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
            Client123 = rt:pbc(N1),
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
            Client123 = rt:pbc(N1),
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
            Client123 = rt:pbc(N1),
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

aae_tree_nuke() ->
    eunit(aae_tree_nuke_test_()).

aae_tree_nuke_test_() ->
    {timeout, rt_cascading:timeout(100), {setup, fun() ->
        Conf = conf(),
        Nodes = rt:deploy_nodes(6, Conf),
        {[N1 | _] = C123, [N4 | _] = C456} = lists:split(3, Nodes),
        repl_util:make_cluster(C123),
        repl_util:make_cluster(C456),
        repl_util:name_cluster(N1, "c123"),
        repl_util:name_cluster(N4, "c456"),
        Port = repl_util:get_cluster_mgr_port(N4),
        lager:info("attempting to connect ~p to c456 on port ~p", [N1, Port]),
        %repl_util:connect_rt(N1, Port, "c456"),
        connect_rt(N1, Port, "c456"),
        {Nodes, C123, C456}
    end,
    fun({Nodes, _, _}) ->
        case rt_config:config_or_os_env(skip_teardowns, false) of
            false ->
                rt:clean_cluster(Nodes);
            _ ->
                ok
        end
    end,
    fun({_Nodes, C123, C456}) -> [

        {"loaded sink has trees destroyed, but have reals still", timeout, rt_cascading:timeout(50), fun() ->
            [N1 | _] = C123,
            [N4 | _] = C456,
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
            [rt:wait_until(Node, WaitFun) || Node <- C456],

            First = 1, Last = fill_size(), Bucket = <<"aae_test_bucket">>,
            lager:info("writing ~p keys to aae_test_bucket on c123", [Last]),
            WriteGot = repl_util:do_write(N1, First, Last, Bucket, 3),
            ?assertEqual([], WriteGot),

            lager:info("waiting for reads on c456"),
            ensure_synced(N1, N4, "c456", First, Last, Bucket, 3),
            %ReadGot = until_not_decresing(N4, First, Last, Bucket, 3),
            %?assertEqual(0, ReadGot),

            [rt:stop_and_wait(Node) || Node <- C456],
            rt:clean_data_dir(C456, "anti_entropy"),
            [rt:start_and_wait(Node) || Node <- C456],

            ReadGot2 = repl_util:wait_for_reads(N4, First, Last, Bucket, 1),
            ?assertEqual(0, ReadGot2)
        end}

    ] end}}.

ensure_synced(SourceNode, SinkNode, SinkCluster, First, Last, Bucket, N) ->
    Got = repl_util:wait_for_reads(SinkNode, First, Last, Bucket, N),
    case Got of
        0 ->
            ok;
        _ ->
            repl_util:enable_fullsync(SourceNode, SinkCluster),
            repl_util:start_and_wait_until_fullsync_complete(SourceNode)
    end.

until_not_decresing(Node, First, Last, Bucket, N) ->
    Got = repl_util:wait_for_reads(Node, First, Last, Bucket, N),
    until_not_decreasing(Node, First, Last, Bucket, N, Got).

until_not_decreasing(_,_,_,_,_,0) ->
    0;
until_not_decreasing(Node, First, Last, Bucket, N, PrevGot) ->
    case repl_util:wait_for_reads(Node, First, Last, Bucket, N) of
        PrevGot ->
            PrevGot;
        Got ->
            until_not_decreasing(Node, First, Last, Bucket, N, Got)
    end.

aae() ->
    eunit(aae_test_()).

aae_test_() ->
    {timeout, rt_cascading:timeout(100), {setup, fun() ->
        BaseConf = conf(),
        KvConf = proplists:get_value(riak_kv, BaseConf, []),
        % these settings are all really bad ideas on a production system
        % however, I'd rather not wait several hours for a tree to
        % finish building
        AAESettings = [
            {anti_entropy_concurrency, 5},
            {anti_entropy_build_limit, {5, 5000}},
            {anti_entropy_tick, 1000}
        ],
        KvAAEConf = lists:foldl(fun({K,_} = Prop,Acc) ->
            lists:keystore(K, 1, Acc, Prop)
        end, KvConf, AAESettings),
        Conf = lists:keystore(riak_kv, 1, BaseConf, {riak_kv, KvAAEConf}),
        Nodes = rt:deploy_nodes(6, Conf),
        {[N1 | _] = C123, [N4 | _] = C456} = lists:split(3, Nodes),
        repl_util:make_cluster(C123),
        repl_util:name_cluster(N1, "c123"),
        repl_util:make_cluster(C456),
        repl_util:name_cluster(N4, "c456"),
        Port = repl_util:get_cluster_mgr_port(N4),
        lager:info("attempting to connect ~p to c456 on port ~p", [N1, Port]),
        %repl_util:connect_rt(N1, Port, "c456"),
        %maybe_reconnect_rt(N1, N4, "c456"),
        connect_rt(N1, Port, "c456"),
        {Nodes, C123, C456}
    end,
    fun({Nodes, _C123, _C456}) ->
        case rt_config:config_or_os_env(skip_teardowns, false) of
            false ->
                rt:clean_cluster(Nodes);
            _ ->
                ok
        end
    end,
    fun({_Node, C123, C456}) -> [

        {"aae diff tree rebuilt doesn't hose data", timeout, rt_cascading:timeout(50), fun() ->
            [N4 | _] = C456,
            [N1 | _] = C123,

            lager:info("setting full objects to 1 on c456"),
            rpc:call(N4, riak_repl_console, full_objects, [["1"]]),
            WaitFun = fun(Node) ->
                case rpc:call(Node, riak_repl_console, full_objects, [[]]) of
                    1 ->
                        lager:info("~p is all set", [Node]),
                        true;
                    Uh ->
                        lager:info("~p got ~p", [Node, Uh]),
                        false
                end
            end,
            [rt:wait_until(Node, WaitFun) || Node <- C456],

            First = 1, Last = fill_size(), Bucket = <<"aae_test_bucket">>,
            lager:info("writing ~p keys to aae_test_bucket on c123", [Last]),
            WriteGot = repl_util:do_write(N1, First, Last, Bucket, 3),
            ?assertEqual([], WriteGot),

            lager:info("waiting for reads on c456"),
            ReadGot = repl_util:wait_for_reads(N4, First, Last, Bucket, 3),
            ?assertEqual(0, ReadGot),

            lager:info("waiting for aae to kick"),
            timer:sleep(3000),
            ReadGot2 = repl_util:wait_for_reads(N4, First, Last, Bucket, 1),
            ?assertEqual(0, ReadGot2)
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
        Nodes = rt:deploy_nodes(6, conf()),
        {[N1 | _] = C123, [N4 | _] = C456} = lists:split(3, Nodes),
        repl_util:make_cluster(C123),
        repl_util:name_cluster(N1, "c123"),
        repl_util:make_cluster(C456),
        repl_util:name_cluster(N4, "c456"),
        Port = repl_util:get_cluster_mgr_port(N4),
        lager:info("attempting to connect ~p to c456 on port ~p", [N1, Port]),
        connect_rt(N1, Port, "c456"),
        #data_push_test{nodes = Nodes, c123 = C123, c456 = C456}
    end,
    fun(State) ->
        case rt_config:config_or_os_env(skip_teardowns, false) of
            "false" ->
                rt:clean_cluster(State#data_push_test.nodes);
            false ->
                rt:clean_cluster(State#data_push_test.nodes);
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
            Client123 = rt:pbc(N1),
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
            Client456 = rt:pbc(N4),

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

            lists:map(fun(Node) ->
                rt_intercept:add(Node, {riak_repl_reduced, [
                    {{mutate_get, 1}, tag_mutate_get},
                    {{mutate_put, 5}, tag_mutate_put}
                ]})
            end, State#data_push_test.c456),
            lists:map(fun(Node) ->
                rt_intercept:add(Node, {riak_repl_reduced, [
                    {{mutate_put, 5}, tag_mutate_put}
                ]})
            end, State#data_push_test.c123),

            FillSize = fill_size(),
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
            %riak_repl_reduced_intercepts:register_as_target(),
            %riak_repl_reduced_intercepts:put_all_reports(),
            %riak_repl_reduced_intercepts:get_all_reports(),

            AssertFirstRead = fun
                (_Key, {ok, Obj}) ->
                    %lager:info("Asserting ~p", [Key]),
                    ?assert(riak_repl_reduced_intercepts:is_get_tagged(Obj)),
                    ?assert(riak_repl_reduced_intercepts:is_put_tagged(Obj));
                (Key, {error, timeout}) ->
                    lager:warning("Timeout on ~p, check again on second assert", [Key]),
                    ok;
                (Key, Error) ->
                    lager:warning("Didn't get ~p as expected: ~p", [Key, Error]),
                    ?assertMatch({ok, _}, Error)
            end,

            AssertSecondRead = fun
                (_Key, {ok, Obj}) ->
                    %lager:info("Asserting ~p", [Key]),
                    ?assert(riak_repl_reduced_intercepts:is_get_tagged(Obj)),
                    ?assert(riak_repl_reduced_intercepts:is_put_tagged(Obj));
                (Key, Error) ->
                    lager:warning("Didn't get ~p as expected: ~p", [Key, Error]),
                    ?assertMatch({ok, _}, Error)
            end,

            Client456 = rt:pbc(hd(State#data_push_test.c456)),
            riakc_pb_socket:set_bucket(Client456, Bucket, [{n_val, 5}]),
            riakc_pb_socket:stop(Client456),

            lager:info("assert first read group"),
            read(State#data_push_test.c456, 1, FillSize, Bucket, AssertFirstRead),
            lager:info("assert second read group"),
            read(State#data_push_test.c456, 1, FillSize, Bucket, AssertSecondRead)
        end}

    ] end}}.

upgrade_sink() ->
    eunit(upgrade_sink_test_()).

upgrade_sink_test_() ->
    {timeout, rt_cascading:timeout(100), {setup, fun() ->
        Conf = conf(),
        Versions = [{previous, Conf} || _ <- lists:seq(1, 6)],
        Nodes = rt:deploy_nodes(Versions),
        {[N1 | _] = C123, [N4 | _] = C456} = lists:split(3, Nodes),
        repl_util:make_cluster(C123),
        repl_util:make_cluster(C456),
        repl_util:name_cluster(N1, "c123"),
        repl_util:name_cluster(N4, "c456"),
        Port = repl_util:get_cluster_mgr_port(N4),
        lager:info("attempting to connect ~p to c456 on port ~p", [N1, Port]),
        %repl_util:connect_rt(N1, Port, "c456"),
        connect_rt(N1, Port, "c456"),
        {Nodes, C123, C456}
    end,
    fun({Nodes, _, _}) ->
        case rt_config:config_or_os_env(skip_teardowns, false) of
            false ->
                rt:clean_cluster(Nodes);
            _ ->
                ok
        end
    end,
    fun({_Nodes, C123, C456}) -> [

        {"upgrade sink and set full objects to 0", timeout, rt_cascading:timeout(50), fun() ->
            % the source doesn't have mutator support, so the objects
            % never get tagged with the source as the cluster of record
            % this means you can set the full_objects to whatever you
            % want, but the sink will still store everything as there is
            % no cluster of record entry in an object's metadata.
            [N1 | _] = C123,
            [N4 | _] = C456,
            [rt:upgrade(Node, current) || Node <- C456],

            rpc:call(N4, riak_repl_console, full_objects, [["0"]]),

            maybe_reconnect_rt(N1, N4, "c456"),

            First = 1, Last = fill_size(), Bucket = <<"upgrade">>,
            WriteGot = repl_util:do_write(N1, First, Last, Bucket, 3),
            ?assertEqual([], WriteGot),

            lager:info("waiting for reads on c456"),
            ReadGot = repl_util:wait_for_reads(N4, First, Last, Bucket, 1),
            ?assertEqual(0, ReadGot)
        end},

        {"upgrade source and see objects tagged", timeout, rt_cascading:timeout(50), fun() ->
            [N1 | _] = C123,
            [N4 | _] = C456,
            [rt:upgrade(Node, current) || Node <- C123],
            WaitForMutateCap = fun(Node) ->
                rpc:call(Node, riak_core_capability, get, [{riak_kv, mutators}, false])
            end,
            [rt:wait_until(Node, WaitForMutateCap) || Node <- C123],

            First = fill_size() + 1, Last = First - 1 + fill_size(), Bucket = <<"upgrade">>,
            WriteGot = repl_util:do_write(N1, First, Last, Bucket, 3),
            ?assertEqual([], WriteGot),
            lager:info("waiting for reads on c456"),
            ReadGot = repl_util:wait_for_reads(N4, First, Last, Bucket, 1),
            ?assertEqual(fill_size(), ReadGot)
        end},

        {"enable proxy get and last notfounds become founds", timeout, rt_cascading:timeout(5), fun() ->
            [N1 | _] = C123,
            [N4 | _] = C456,
            rpc:call(N1, riak_repl_console, proxy_get, [["enable", "c456"]]),
            First = fill_size() + 1, Last = First - 1 + fill_size(), Bucket = <<"upgrade">>,
            ReadGot = repl_util:wait_for_reads(N4, First, Last, Bucket, 1),
            ?assertEqual(0, ReadGot)
        end}

    ] end}}.

upgrade_source() ->
    eunit(upgrade_source_test_()).

upgrade_source_test_() ->
    {timeout, rt_cascading:timeout(100), {setup, fun() ->
        Conf = conf(),
        Versions = [{previous, Conf} || _ <- lists:seq(1, 6)],
        Nodes = rt:deploy_nodes(Versions),
        {[N1 | _] = C123, [N4 | _] = C456} = lists:split(3, Nodes),
        repl_util:make_cluster(C123),
        repl_util:make_cluster(C456),
        repl_util:name_cluster(N1, "c123"),
        repl_util:name_cluster(N4, "c456"),
        Port = repl_util:get_cluster_mgr_port(N4),
        lager:info("attempting to connect ~p to c456 on port ~p", [N1, Port]),
        connect_rt(N1, Port, "c456"),
        {Nodes, C123, C456}
    end,
    fun({Nodes, _, _}) ->
        case rt_config:config_or_os_env(skip_teardowns, false) of
            false ->
                rt:clean_cluster(Nodes);
            _ ->
                ok
        end
    end,
    fun({_Nodes, C123, C456}) -> [

        {"upgrade source", timeout, rt_cascading:timeout(50), fun() ->
            [N1 | _] = C123,
            [N4 | _] = C456,
            [rt:upgrade(Node, current) || Node <- C123],

            WaitForMutateCap = fun(Node) ->
                rpc:call(Node, riak_core_capability, get, [{riak_kv, mutators}, false])
            end,
            [rt:wait_until(Node, WaitForMutateCap) || Node <- C123],

            maybe_reconnect_rt(N1, N4, "c456"),

            First = 1, Last = fill_size(), Bucket = <<"upgrade">>,
            WriteGot = repl_util:do_write(N1, First, Last, Bucket, 3),
            ?assertEqual([], WriteGot),

            lager:info("waiting for reads on c456"),
            % manual testing indicates it gets there, just maybe not as
            % fast as this function expects.
            ReadGot = case repl_util:wait_for_reads(N4, First, Last, Bucket, 1) of
                Last ->
                    Last;
                _ ->
                    repl_util:wait_for_reads(N4, First, Last, bucket, 1)
            end,
            ?assertEqual(Last, ReadGot)

        end},

        {"upgrade sink and set full objects to 0", timeout, rt_cascading:timeout(50), fun() ->
            [N1 | _] = C123,
            [N4 | _] = C456,
            [rt:upgrade(Node, current) || Node <- C456],
            WaitForMutateCap = fun(Node) ->
                rpc:call(Node, riak_core_capability, get, [{riak_kv, mutators}, false])
            end,
            [rt:wait_until(Node, WaitForMutateCap) || Node <- C456],
            rpc:call(N4, riak_repl_console, full_objects, [["0"]]),

            First = fill_size() + 1, Last = First - 1 + fill_size(), Bucket = <<"upgrade">>,
            WriteGot = repl_util:do_write(N1, First, Last, Bucket, 3),
            ?assertEqual([], WriteGot),
            lager:info("waiting for reads on c456"),
            ReadGot = repl_util:wait_for_reads(N4, First, Last, Bucket, 1),
            ?assertEqual(fill_size(), ReadGot)
        end},

        {"enable proxy get and last notfounds become founds", timeout, rt_cascading:timeout(5), fun() ->
            [N1 | _] = C123,
            [N4 | _] = C456,
            rpc:call(N1, riak_repl_console, proxy_get, [["enable", "c456"]]),
            First = fill_size() + 1, Last = First - 1 + fill_size(), Bucket = <<"upgrade">>,
            ReadGot = repl_util:wait_for_reads(N4, First, Last, Bucket, 1),
            ?assertEqual(0, ReadGot)
        end}

    ] end}}.

maybe_reconnect_rt(Node, SinkNode, SinkName) ->
    maybe_reconnect_clusters(Node, SinkNode, SinkName),
    maybe_enable_rt(Node, SinkName),
    maybe_start_rt(Node, SinkName).

maybe_reconnect_clusters(Node, SinkNode, SinkName) ->
    {ok, Conns} = rpc:call(Node, riak_core_cluster_mgr, get_connections, []),
    case [Name || {{cluster_by_name, Name},_Pid} <- Conns, SinkName =:= Name] of
        [] ->
            Port = repl_util:get_cluster_mgr_port(SinkNode),
            repl_util:connect_cluster(Node, "127.0.0.1", Port),
            repl_util:wait_for_connection(Node, SinkName);
        _ ->
            ok
    end.

maybe_enable_rt(Node, SinkName) ->
    List = rpc:call(Node, riak_repl2_rt, enabled, []),
    case lists:delete(SinkName, List) of
        List ->
            repl_util:enable_realtime(Node, SinkName);
        _ ->
            ok
    end.

maybe_start_rt(Node, SinkName) ->
    List = rpc:call(Node, riak_repl2_rt, started, []),
    case lists:delete(SinkName, List) of
        List ->
            repl_util:start_realtime(Node, SinkName);
        _ ->
            ok
    end.

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
        {diff_batch_size, 10},
        {rt_heartbeat_interval, undefined}
    ]}].

exists(Nodes, Bucket, Key) ->
    exists({error, notfound}, Nodes, Bucket, Key).

exists(Got, [], _Bucket, _Key) ->
    Got;
exists({error, notfound}, [Node | Tail], Bucket, Key) ->
    Pid = rt:pbc(Node),
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
        rt:pbc(Node)
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

eunit(TestRep) ->
    case eunit:test(TestRep, [verbose]) of
        ok ->
            pass;
        error ->
            exit(error),
            fail
    end.

write_n_keys(Source, Destination, M, N) ->
    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-rt_test_a">>,
    First = M,
    Last = N,

    %% Write some objects to the source cluster (A),
    lager:info("Writing ~p keys to ~p, which should RT repl to ~p",
               [Last-First+1, Source, Destination]),
    ?assertEqual([], repl_util:do_write(Source, First, Last, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading ~p keys written from ~p", [Last-First+1, Destination]),
    ?assertEqual(0, repl_util:wait_for_reads(Destination, First, Last, TestBucket, 2)).

connect_rt(Node, Port, Sinkname) ->
    lager:info("attempting to ensure clusters are connected"),
    {Pid, Mon} = spawn_monitor(repl_util, connect_rt, [Node, Port, Sinkname]),
    receive
        {'DOWN', Mon, process, Pid, normal} ->
            wait_for_rt(Node, Sinkname),
            wait_for_rt_source(Node, Sinkname)
    after 2000 ->
        connect_rt(Node, Port, Sinkname)
    end.

wait_for_rt(Node, Sinkname) ->
    lager:info("attempting to ensure realtime is started"),
    rt:wait_until(fun() ->
        is_source_enabled(Node, Sinkname)
    end).

wait_for_rt_source(Node, Sinkname) ->
    rt:wait_until(fun() ->
        Status = rpc:call(Node, riak_repl_console, status, [quiet]),
        Sources = proplists:get_value(sources, Status),
        case is_source_connected(Sinkname, Sources) of
            false ->
                case is_source_enabled(Node, Sinkname) of
                    false ->
                        repl_util:enable_realtime(Node, Sinkname),
                        repl_util:start_realtime(Node, Sinkname),
                        false;
                    true ->
                        false
                end;
            true ->
                true
        end
    end).

is_source_enabled(Node, Sinkname) ->
    Status = rpc:call(Node, riak_repl_console, status, [quiet]),
    Started = proplists:get_value(realtime_started, Status),
    case {Started, Sinkname} of
        {A,A} ->
            true;
        {[], _} ->
            false;
        {B, _} when is_integer(hd(B)) ->
            false;
        {_, _} when is_list(Started) ->
            lists:member(Sinkname, Started)
    end.

is_source_connected(_Sinkname, []) ->
    lager:info("no connection this time, looping around"),
    false;
is_source_connected(Sinkname, [Source | Sources]) ->
    {source_stats, Data} = Source,
    ConnTo = proplists:get_value(rt_source_connected_to, Data),
    SourceName = proplists:get_value(source, ConnTo),
    if
        SourceName =:= Sinkname -> true;
        true -> is_source_connected(Sinkname, Sources)
    end.


fill_size() ->
    case rt_config:config_or_os_env(fill_size, ?default_fill) of
        FS when is_integer(FS) ->
            FS;
        FS when is_list(FS) ->
            list_to_integer(FS)
    end.

