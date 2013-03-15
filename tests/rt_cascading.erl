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
    lager:debug("didn't find it, I'll try again after ~p ms", [WaitMs]),
    timer:sleep(WaitMs),
    Got = riakc_pb_socket:get(Pid, Bucket, Key),
    maybe_eventually_exists(Got, Pid, Node, Bucket, Key, MaxTries - 1, WaitMs);

maybe_eventually_exists({ok, RiakObj}, _Pid, _Node, _Bucket, _Key, _MaxTries, _WaitMs) ->
    riakc_obj:get_value(RiakObj);

maybe_eventually_exists(Got, _Pid, _Node, _Bucket, _Key, _MaxTries, _WaitMs) ->
    Got.
