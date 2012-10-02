-module(basic_command_line).
-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->

    %% Deploy a node to test against
    lager:info("Deploy node to test command line"),
    Nodes = rt:deploy_nodes(1),
    [Node1] = Nodes,
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),

    %% It is possible to save some time grouping tests into whether riak
    %% should be up or down when the test runs, but it is my opinion that the
    %% the individual tests should handle riak in their own context at the
    %% expense of testing time
    console_test(Node1),

    start_test(Node1),

    ping_test(Node1),

    restart_test(Node1),

    attach_test(Node1),

    status_test(Node1),

    pass.

console_test(Node) ->
    %% Make sure the cluster will start up with /usr/sbin/riak console, then quit
    lager:info("Testing riak console on ~s", [Node]),

    %% Ensure node is up to start with
    rt:start_and_wait(Node),
    lager:info("Node is already up, should fail"),
    {ok, ConsoleFail} = rt:riak(Node, ["console"]),
    ?assert(rt:str(ConsoleFail, "Node is already running")),

    %% Stop node, to test console working
    rt:stop_and_wait(Node),
    {ok, ConsolePass} = rt:riak(Node, ["console"]),

    ?assert(rt:str(ConsolePass, "(abort with ^G)")),

    ok.


start_test(Node) ->
    %% Test starting with /bin/riak start
    lager:info("Testing riak start works on ~s", [Node]),

    %% First stop riak
    rt:stop_and_wait(Node),

    {ok, StartPass} = rt:riak(Node, ["start"]),
    ?assertMatch(StartPass, ""),

    %% Try starting again and check you get the node is already running message
    lager:info("Testing riak start now will return 'already running'"),
    {ok, StartOut} = rt:riak(Node, ["start"]),
    ?assert(rt:str(StartOut, "Node is already running!")),

    ok.

ping_test(Node) ->

    %% check /usr/sbin/riak ping
    lager:info("Testing riak ping on ~s", [Node]),

    %% ping / pong
    rt:start_and_wait(Node),
    lager:info("Node up, should ping"),
    {ok, PongOut} = rt:riak(Node, ["ping"]),
    ?assert(rt:str(PongOut, "pong")),

    %% ping / pang
    lager:info("Stopping Node"),
    rt:stop_and_wait(Node),
    lager:info("Node down, should pang"),
    {ok, PangOut} = rt:riak(Node, ["ping"]),
    ?assert(rt:str(PangOut, "not responding to pings")),
    ok.

attach_test(Node) ->

    %% check /usr/sbin/riak attach')
    %% Sort of a punt on this test, it tests that attach
    %% connects to the pipe, but doesn't run any commands.
    %% This is probably okay for a basic cmd line test

    lager:info("Testing riak attach"),
    rt:start_and_wait(Node),
    %{ok, AttachOut} = rt:riak(Node, ["attach"]),
    %?assert(rt:str(AttachOut, "erlang.pipe.1 \(^D to exit\)")),

    rt:attach(Node, [{expect, "erlang.pipe.1 \(^D to exit\)"},
                     {send, "riak_core_ring_manager:get_my_ring()."},
                     {expect, "dict,"},
                     {send, [4]}]), %% 4 = Ctrl + D

    ok.

restart_test(Node) ->
    lager:info("Testing riak restart on ~s", [Node]),

    %% Riak should be running
    rt:start_and_wait(Node),

    %% Issue restart
    {ok, RestartOut} = rt:riak(Node, ["restart"]),
    ?assert(rt:str(RestartOut, "ok")),

    %% Its not that we don't trust you 'ok'
    %% but make sure riak is really up
    ?assert(rt:is_pingable(Node)),

    ok.

status_test(Node) ->
    lager:info("Test riak-admin status on ~s", [Node]),

    % riak-admin status needs things completely started
    % to work, so are more careful to wait
    rt:start_and_wait(Node),

    lager:info("Waiting for status from riak_kv"),
    rt:wait_until_status_ready(Node),

    lager:info("Now testing 'riak-admin status'"),
    {ok, StatusOut} = rt:admin(Node, ["status"]),
    io:format("Result of status: ~s", [StatusOut]),


    ?assert(rt:str(StatusOut, "1-minute stats")),
    ?assert(rt:str(StatusOut, "kernel_version")),

    ok.
