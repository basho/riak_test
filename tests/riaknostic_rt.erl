%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
-module(riaknostic_rt).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% Build a small cluster
    [Node1, _Node2] = rt:build_cluster(2, []),
    ?assertEqual(ok, rt:wait_until_nodes_ready([Node1])),

    %% Run through all tests on Node1
    check_riaknostic_execute(Node1),
    check_riaknostic_usage(Node1),
    check_riaknostic_command_list(Node1),
    check_riaknostic_log_levels(Node1),

    %% Done!
    lager:info("Test riaknostic: PASS"),
    pass.

%% Check that riaknostic executes
check_riaknostic_execute(Node) ->
    %% Execute
    lager:info("**  Check Riaknostic executes"),
    {ok, RiaknosticOut} = rt:admin(Node, ["diag"]),
    ?assertNot(rt:str(RiaknosticOut, "is not present!")),
    ?assertNot(rt:str(RiaknosticOut, "[debug]")),
    ok.

%% Check that riaknostic gives a usage message
check_riaknostic_usage(Node) ->
    %% Check usage message
    lager:info("**  Run Riaknostic usage message"),
    {ok, RiaknosticOut} = rt:admin(Node, ["diag", "--help"]),
    ?assert(rt:str(RiaknosticOut, "Usage: riak-admin")),
    ok.

%% Check that riaknostic gives a command listing
check_riaknostic_command_list(Node) ->
    %% Check commands list
    lager:info("**  Run Riaknostic commands list message"),
    {ok, RiaknosticOut} = rt:admin(Node, ["diag", "--list"]),
    ?assert(rt:str(RiaknosticOut, "Available diagnostic checks")),
    ?assert(rt:str(RiaknosticOut, "  disk           ")),
    ?assert(rt:str(RiaknosticOut, "  dumps          ")),
    ?assert(rt:str(RiaknosticOut, "  memory_use     ")),
    ?assert(rt:str(RiaknosticOut, "  nodes_connected")),
    ?assert(rt:str(RiaknosticOut, "  ring_membership")),
    ?assert(rt:str(RiaknosticOut, "  ring_preflists ")),
    ?assert(rt:str(RiaknosticOut, "  ring_size      ")),
    ?assert(rt:str(RiaknosticOut, "  search         ")),
    ok.

%% Check that log levels can be set
check_riaknostic_log_levels(Node) ->
    %% Check log levels
    lager:info("**  Run Riaknostic with a different log level"),
    {ok, RiaknosticOut} = rt:admin(Node, ["diag", "--level", "debug"]),
    ?assert(rt:str(RiaknosticOut, "[debug]")),
    ok.

