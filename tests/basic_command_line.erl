%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2016 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basic_command_line).
-include_lib("eunit/include/eunit.hrl").

-behavior(riak_test).
-compile(export_all).
-export([confirm/0]).

% node_package 3.x changes this - new first, old second
-define(PING_FAILURE_OUTPUT,
    ["Node did not respond to ping!", "Node is not running!"]).

confirm() ->

    %% Deploy a node to test against
    lager:info("Deploy node to test command line"),
    [Node] = rt:deploy_nodes(1),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node])),

    %% Verify node-up behavior
    ping_up_test(Node),
    attach_direct_up_test(Node),
    status_up_test(Node),
    console_up_test(Node),
    start_up_test(Node),
    getpid_up_test(Node),

    %% Stop the node, Verify node-down behavior
    stop_test(Node),
    ping_down_test(Node),
    attach_down_test(Node),
    attach_direct_down_test(Node),
    status_down_test(Node),
    console_test(Node),
    start_test(Node),
    getpid_down_test(Node),

    pass.

console_up_test(Node) ->
    lager:info("Node is already up, `riak console` should fail"),
    {ok, ConsoleFail} = rt:riak(Node, ["console"]),
    ?assert(rt:str(ConsoleFail, "Node is already running")),
    ok.

console_test(Node) ->
    %% Make sure the cluster will start up with /usr/sbin/riak console, then quit
    lager:info("Testing riak console on ~s", [Node]),

    %% Stop node, to test console working
    rt:console(Node, [{expect, "\(abort with ^G\)"},
                      {send, "riak_core_ring_manager:get_my_ring()."},
                      {expect, "dict,"},
                      {send, "q()."},
                      {expect, "ok"}]),
    rt:wait_until_unpingable(Node),
    ok.

start_up_test(Node) ->
    %% Try starting again and check you get the node is already running message
    lager:info("Testing riak start now will return 'already running'"),
    {ok, StartOut} = rt:riak(Node, ["start"]),
    ?assert(rt:str(StartOut, "Node is already running!")),
    ok.


start_test(Node) ->
    %% Test starting with /bin/riak start
    lager:info("Testing riak start works on ~s", [Node]),

    {ok, StartPass} = rt:riak(Node, ["start"]),
    lager:info("StartPass: ~p", [StartPass]),
    ?assert(StartPass =:= "" orelse string:str(StartPass, "WARNING") =/= 0),
    rt:stop_and_wait(Node),
    ok.

stop_test(Node) ->
    ?assert(rt:is_pingable(Node)),

    {ok, "ok\n"} = rt:riak(Node, "stop"),

    ?assertNot(rt:is_pingable(Node)),
    ok.

ping_up_test(Node) ->

    %% check /usr/sbin/riak ping
    lager:info("Testing riak ping on ~s", [Node]),

    %% ping / pong
    %% rt:start_and_wait(Node),
    lager:info("Node up, should ping"),
    {ok, PongOut} = rt:riak(Node, ["ping"]),
    ?assert(rt:str(PongOut, "pong")),
    ok.

ping_down_test(Node) ->
    %% ping / pang
    lager:info("Node down, should pang"),
    {ok, PangOut} = rt:riak(Node, ["ping"]),
    ?assert(rt:str(PangOut, "not responding to pings")),
    ok.

attach_down_test(Node) ->
    lager:info("Testing riak attach while down"),
    {ok, AttachOut} = rt:riak(Node, ["attach"]),
    ?assert(rt:str_mult(AttachOut, ?PING_FAILURE_OUTPUT)),
    ok.

attach_direct_up_test(Node) ->
    lager:info("Testing riak attach-direct"),

    rt:attach_direct(Node, [{expect, "\(^D to exit\)"},
                            {send, "riak_core_ring_manager:get_my_ring()."},
                            {expect, "dict,"},
                            {send, [4]}]), %% 4 = Ctrl + D
    ok.

attach_direct_down_test(Node) ->
    lager:info("Testing riak attach-direct while down"),
    {ok, AttachOut} = rt:riak(Node, ["attach-direct"]),
    ?assert(rt:str_mult(AttachOut, ?PING_FAILURE_OUTPUT)),
    ok.

status_up_test(Node) ->
    lager:info("Test riak-admin status on ~s", [Node]),

    {ok, {ExitCode, StatusOut}} = rt:admin(Node, ["status"], [return_exit_code]),
    io:format("Result of status: ~s", [StatusOut]),
    ?assertEqual(0, ExitCode),
    ?assert(rt:str(StatusOut, "1-minute stats")),
    ?assert(rt:str(StatusOut, "kernel_version")),

    ok.

status_down_test(Node) ->
    lager:info("Test riak-admin status while down"),
    {ok, {ExitCode, StatusOut}} = rt:admin(Node, ["status"], [return_exit_code]),
    ?assertEqual(1, ExitCode),
    ?assert(rt:str_mult(StatusOut, ?PING_FAILURE_OUTPUT)),
    ok.

getpid_up_test(Node) ->
    lager:info("Test riak getpid on ~s", [Node]),
    {ok, PidOut} = rt:riak(Node, ["getpid"]),
    ?assertNot(rt:str(PidOut, "")),
    ?assert(rt:str(PidOut, rpc:call(Node, os, getpid, []))),
    ok.

getpid_down_test(Node) ->
    lager:info("Test riak getpid fails on ~s", [Node]),
    {ok, PidOut} = rt:riak(Node, ["getpid"]),
    ?assert(rt:str(PidOut, "Node is not running!")),
    ok.
