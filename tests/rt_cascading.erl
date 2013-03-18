-module(rt_cascading).
-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

% cluster_mgr port = 10006 + 10n where n is devN

confirm() ->
    case eunit:test(?MODULE, [verbose]) of
        ok ->
            pass;
        error ->
            % At the time this is written, The return value isn't acutally
            % checked, the only way to fail is to crash the process.
            % I leave the fail here in hopes a future version will actually
            % do what the documentation says.
            exit(error),
            fail
    end.

-record(simple_state, {
    beginning = [] :: [node()],
    middle = [] :: [node()],
    ending = [] :: [node()]
}).

simple_test_() ->
    {timeout, 60000, {setup, fun() ->
        Conf = conf(),
        [BeginNode, MiddleNode, EndNode] = rt:deploy_nodes(3, Conf),
        repl_util:make_cluster([BeginNode]),
        repl_util:make_cluster([MiddleNode]),
        repl_util:make_cluster([EndNode]),
        repl_util:name_cluster(BeginNode, "beginning"),
        repl_util:name_cluster(MiddleNode, "middle"),
        repl_util:name_cluster(EndNode, "end"),
        #simple_state{beginning = BeginNode, middle = MiddleNode,
            ending = EndNode}
    end,
    fun(State) ->
        repl_util:disable_realtime(State#simple_state.beginning, "middle"),
        repl_util:disable_realtime(State#simple_state.middle, "end"),
        rt:stop_and_wait(State#simple_state.ending),
        rt:stop_and_wait(State#simple_state.middle),
        rt:stop_and_wait(State#simple_state.beginning)
    end,
    fun(State) -> [

        {"connecting Beginning to Middle", fun() ->
            repl_util:connect_cluster(State#simple_state.beginning, "127.0.0.1", 10026),
            repl_util:enable_realtime(State#simple_state.beginning, "middle"),
            repl_util:start_realtime(State#simple_state.beginning, "middle")
        end},

        {"connection Middle to End", fun() ->
            repl_util:connect_cluster(State#simple_state.middle, "127.0.0.1", 10036),
            repl_util:enable_realtime(State#simple_state.middle, "end"),
            repl_util:start_realtime(State#simple_state.middle, "end")
        end},

        {"Usual realtime still works", fun() ->
            Pid = rt:pbc(State#simple_state.beginning),
            Bin = <<"regular realtime">>,
            Obj = riakc_obj:new(<<"objects">>, Bin, Bin),
            riakc_pb_socket:put(Pid, Obj, [{w,1}]),
            riakc_pb_socket:stop(Pid),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.middle, <<"objects">>, Bin))
        end},

        {"cascade a put from beginning down to ending", fun() ->
            Pid = rt:pbc(State#simple_state.beginning),
            Bin = <<"cascading realtime">>,
            Obj = riakc_obj:new(<<"objects">>, Bin, Bin),
            riakc_pb_socket:put(Pid, Obj, [{w,1}]),
            riakc_pb_socket:stop(Pid),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.ending, <<"objects">>, Bin))
        end}

    ] end}}.

circle_test_() ->
    {timeout, 60000, {setup, fun() ->
        Conf = conf(),
        [One, Two, Three] = Nodes = rt:deploy_nodes(3, Conf),
        [repl_util:make_cluster([N]) || N <- Nodes],
        Names = ["one", "two", "three"],
        [repl_util:name_cluster(Node, Name) || {Node, Name} <- lists:zip(Nodes, Names)],

        Connections = [
            {One, 10026, "two"},
            {Two, 10036, "three"},
            {Three, 10016, "one"}
        ],
        lists:map(fun({Node, Port, Name}) ->
            repl_util:connect_cluster(Node, "127.0.0.1", Port),
            repl_util:enable_realtime(Node, Name),
            repl_util:start_realtime(Node, Name)
        end, Connections),
        Nodes
    end,
    fun(Nodes) ->
        ConnnectedTo = ["two", "three", "one"],
        [repl_util:disable_realtime(Node, Name) || {Node, Name} <- lists:zip(Nodes, ConnnectedTo)],
        [rt:stop_and_wait(Node) || Node <- Nodes]
    end,
    fun(Nodes) -> [

        {"cascade all the way to the other end, but no futher", timeout, 6000, fun() ->
            Client = rt:pbc(hd(Nodes)),
            Bin = <<"cascading">>,
            Obj = riakc_obj:new(<<"objects">>, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            Put = riakc_pb_socket:get(Client, <<"objects">>, Bin),
            ?assertEqual(Bin, maybe_eventually_exists(lists:last(Nodes), <<"objects">>, Bin)),
            % we want to ensure there's not a cascade back to the beginning, so
            % there's no event we can properly wait for. All we can do is wait
            % and make sure we didn't update/write the object.
            timer:sleep(1000),
            Got = riakc_pb_socket:get(Client, <<"objects">>, Bin),
            EndClient = rt:pbc(lists:last(Nodes)),
            EndGot = riakc_pb_socket:get(EndClient, <<"objects">>, Bin),
            ?debugFmt("~n"
                "    Result of original put: ~p~n"
                "    Result of last got: ~p~n"
                "    Result of end got: ~p~n"
                , [Put, Got, EndGot]),
            Status = rpc:call(hd(Nodes), riak_repl2_rt, status, []),
            [SinkData] = proplists:get_value(sinks, Status, [[]]),
            ?assertEqual(undefined, proplists:get_value(expect_seq, SinkData))
        end}

    ] end}}.

%% =====
%% utility functions for teh happy
%% ====

conf() ->
    [{riak_repl, [
        {fullsync_on_connect, false},
        {fullsync_interval, disabled},
        {diff_batch_size, 10}
    ]}].

maybe_eventually_exists(Node, Bucket, Key) ->
    maybe_eventually_exists(Node, Bucket, Key, 5, 100).

maybe_eventually_exists(Node, Bucket, Key, MaxTries, WaitMs) ->
    Pid = rt:pbc(Node),
    Got = riakc_pb_socket:get(Pid, Bucket, Key),
    maybe_eventually_exists(Got, Pid, Node, Bucket, Key, MaxTries - 1, WaitMs).

maybe_eventually_exists({error, notfound}, Pid, Node, Bucket, Key, MaxTries, WaitMs) when MaxTries > 0 ->
    timer:sleep(WaitMs),
    Got = riakc_pb_socket:get(Pid, Bucket, Key),
    maybe_eventually_exists(Got, Pid, Node, Bucket, Key, MaxTries - 1, WaitMs);

maybe_eventually_exists({ok, RiakObj}, _Pid, _Node, _Bucket, _Key, _MaxTries, _WaitMs) ->
    riakc_obj:get_value(RiakObj);

maybe_eventually_exists(Got, _Pid, _Node, _Bucket, _Key, _MaxTries, _WaitMs) ->
    Got.
