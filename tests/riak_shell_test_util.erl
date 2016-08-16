%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
-module(riak_shell_test_util).

-define(CLUSTERSIZE, 3).
-define(EMPTYCONFIG, []).

-export([
         shell_init/0,
         run_commands/3,
         build_cluster/0,
         loop/0
        ]).

-define(PREFIX, "Riak-shell tests: ").

shell_init() ->
        Config =  [
               {logging, off},
               {cookie, riak},
               {nodes, [
                        'dev1@127.0.0.1',
                        'dev2@127.0.0.1',
                        'dev3@127.0.0.1',
                        'dev4@127.0.0.1',
                        'dev5@127.0.0.1',
                        'dev6@127.0.0.1',
                        'dev7@127.0.0.1',
                        'dev8@127.0.0.1'
                       ]}
              ],
    State = riak_shell_app:boot_TEST(Config),
    receive
        %% after initialising the riak_shell gets a succeed/fail connection
        %% message - we need to drain that msg here if we want the runner
        %% to work, or it will be off-by-1 in the test runner
        ConnectionMsg ->
            lager:info("Got a connection message ~p on shell_init",
                       [ConnectionMsg])
    end,
    State.

build_cluster() ->
    rt:set_backend(eleveldb),
    _Nodes  = rt:build_cluster(?CLUSTERSIZE, ?EMPTYCONFIG).

run_commands([], _State, _ShouldIncrement) ->
    pass;
run_commands([{drain, discard} | T], State, ShouldIncrement) ->
    {_Error, Response, NewState, NewShdIncr} = riak_shell:loop_TEST(riak_shell:make_cmd(), State, ShouldIncrement),
    lager:info("Message drained and discared unchecked ~p", [lists:flatten(Response)]),
    run_commands(T, NewState, NewShdIncr);
run_commands([{drain, Expected} | T], State, ShouldIncrement) ->
    {_Error, Response, NewState, NewShdIncr} = riak_shell:loop_TEST(riak_shell:make_cmd(), State, ShouldIncrement),
    case lists:flatten(Response) of
        Expected -> lager:info("Message drained successfully ~p", [Expected]),
                    run_commands(T, NewState, NewShdIncr);
        Got      -> print_error("Message Expected", "", Expected, Got),
                    fail
    end;
run_commands([{start_node, Node} | T], State, ShouldIncrement) ->
    rt:start(Node),
    rt:wait_until_pingable(Node),
    run_commands(T, State, ShouldIncrement);
run_commands([{stop_node, Node} | T], State, ShouldIncrement) ->
    rt:stop(Node),
    rt:wait_until_unpingable(Node),
    run_commands(T, State, ShouldIncrement);
run_commands([sleep | T], State, ShouldIncrement) ->
    timer:sleep(1000),
    run_commands(T, State, ShouldIncrement);
run_commands([{{match, Expected}, Cmd} | T], State, ShouldIncrement) ->
    {_Error, Response, NewState, NewShdIncr} = run_cmd(Cmd, State, ShouldIncrement),
    %% when you start getting off-by-1 weirdness you will WANT to uncomment this
    %% Trim off the newlines to aid in string comparison
    ExpectedTrimmed = cleanup_output(Expected),
    ResultTrimmed = cleanup_output(Response),
    case ResultTrimmed of
        ExpectedTrimmed -> lager:info("Successful match of ~p from ~p", [Expected, Cmd]),
                           run_commands(T, NewState, NewShdIncr);
        _               -> print_error("Ran ~p:", Cmd, Expected, Response),
                           fail
    end;
run_commands([{run, Cmd} | T], State, ShouldIncrement) ->
    lager:info("Run command: ~p", [Cmd]),
    {_Error, Result, NewState, NewShdIncr} = run_cmd(Cmd, State, ShouldIncrement),
    lists:map(fun(X) -> lager:info("~s~n", [X]) end, re:split(Result, "\n", [trim])),
    run_commands(T, NewState, NewShdIncr).

cleanup_output(In) ->
    re:replace(lists:flatten(In), "[\r\n]", "", [global,{return,list}]).

run_cmd(Cmd, State, ShouldIncrement) ->
    %% the riak-shell works by spawning a process that has captured
    %% standard input and then dropping into a receive that the spawned
    %% process sends a message to
    %% we have to emulate that here as we are the shell
    %% we are going to send a message at some time in the future
    %% and then go into a loop waiting for it
    timer:apply_after(500, riak_shell, send_to_shell, [self(), {command, Cmd}]),
    riak_shell:loop_TEST(riak_shell:make_cmd(Cmd), State, ShouldIncrement).

print_error(Format, Cmd, Expected, Got) ->
    lager:info(?PREFIX ++ "Match Failure"),
    lager:info("**************************************************************"),
    lager:info(Format, [Cmd]),
    lager:info("Exp: ~s", [list_to_binary(lists:flatten(Expected))]),
    lager:info("Got: ~s", [list_to_binary(lists:flatten(Got))]),
    lager:info("**************************************************************").

loop() ->
    Return = receive
                 pass  -> pass;
                 fail  -> exit(fail);
                 Other -> io:format("Got message ~p~n", [Other]),
                          loop()
             end,
    application:stop(riak_shell),
    Return.
