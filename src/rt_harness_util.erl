%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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

-module(rt_harness_util).

-include_lib("eunit/include/eunit.hrl").
-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(PATH, (rt_config:get(rtdev_path))).

-export([admin/2,
         attach/2,
         attach_direct/2,
         cmd/1,
         cmd/2,
         console/2,
         get_ip/1,
         node_id/1,
         node_version/1,
         riak/2,
         set_conf/2,
         spawn_cmd/1,
         spawn_cmd/2,
         whats_up/0]).

admin(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = riak_admin_cmd(Path, N, Args),
    lager:info("Running: ~s", [Cmd]),
    Result = os:cmd(Cmd),
    lager:info("~s", [Result]),
    {ok, Result}.

attach(Node, Expected) ->
    interactive(Node, "attach", Expected).

attach_direct(Node, Expected) ->
    interactive(Node, "attach-direct", Expected).

console(Node, Expected) ->
    interactive(Node, "console", Expected).

interactive(Node, Command, Exp) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = riakcmd(Path, N, Command),
    lager:info("Opening a port for riak ~s.", [Command]),
    lager:debug("Calling open_port with cmd ~s", [binary_to_list(iolist_to_binary(Cmd))]),
    P = open_port({spawn, binary_to_list(iolist_to_binary(Cmd))},
                  [stream, use_stdio, exit_status, binary, stderr_to_stdout]),
    interactive_loop(P, Exp).

interactive_loop(Port, Expected) ->
    receive
        {Port, {data, Data}} ->
            %% We've gotten some data, so the port isn't done executing
            %% Let's break it up by newline and display it.
            Tokens = string:tokens(binary_to_list(Data), "\n"),
            [lager:debug("~s", [Text]) || Text <- Tokens],

            %% Now we're going to take hd(Expected) which is either {expect, X}
            %% or {send, X}. If it's {expect, X}, we foldl through the Tokenized
            %% data looking for a partial match via rt:str/2. If we find one,
            %% we pop hd off the stack and continue iterating through the list
            %% with the next hd until we run out of input. Once hd is a tuple
            %% {send, X}, we send that test to the port. The assumption is that
            %% once we send data, anything else we still have in the buffer is
            %% meaningless, so we skip it. That's what that {sent, sent} thing
            %% is about. If there were a way to abort mid-foldl, I'd have done
            %% that. {sent, _} -> is just a pass through to get out of the fold.

            NewExpected = lists:foldl(fun(X, Expect) ->
                    [{Type, Text}|RemainingExpect] = case Expect of
                        [] -> [{done, "done"}|[]];
                        E -> E
                    end,
                    case {Type, rt:str(X, Text)} of
                        {expect, true} ->
                            RemainingExpect;
                        {expect, false} ->
                            [{Type, Text}|RemainingExpect];
                        {send, _} ->
                            port_command(Port, list_to_binary(Text ++ "\n")),
                            [{sent, "sent"}|RemainingExpect];
                        {sent, _} ->
                            Expect;
                        {done, _} ->
                            []
                    end
                end, Expected, Tokens),
            %% Now that the fold is over, we should remove {sent, sent} if it's there.
            %% The fold might have ended not matching anything or not sending anything
            %% so it's possible we don't have to remove {sent, sent}. This will be passed
            %% to interactive_loop's next iteration.
            NewerExpected = case NewExpected of
                [{sent, "sent"}|E] -> E;
                E -> E
            end,
            %% If NewerExpected is empty, we've met all expected criteria and in order to boot
            %% Otherwise, loop.
            case NewerExpected of
                [] -> ?assert(true);
                _ -> interactive_loop(Port, NewerExpected)
            end;
        {Port, {exit_status,_}} ->
            %% This port has exited. Maybe the last thing we did was {send, [4]} which
            %% as Ctrl-D would have exited the console. If Expected is empty, then
            %% We've met every expectation. Yay! If not, it means we've exited before
            %% something expected happened.
            ?assertEqual([], Expected)
        after rt_config:get(rt_max_wait_time) ->
            %% interactive_loop is going to wait until it matches expected behavior
            %% If it doesn't, the test should fail; however, without a timeout it
            %% will just hang forever in search of expected behavior. See also: Parenting
            ?assertEqual([], Expected)
    end.

spawn_cmd(Cmd) ->
    spawn_cmd(Cmd, []).
spawn_cmd(Cmd, Opts) ->
    Port = open_port({spawn, lists:flatten(Cmd)}, [stream, in, exit_status] ++ Opts),
    Port.

wait_for_cmd(Port) ->
    rt:wait_until(node(),
                  fun(_) ->
                          receive
                              {Port, Msg={exit_status, _}} ->
                                  catch port_close(Port),
                                  self() ! {Port, Msg},
                                  true
                          after 0 ->
                                  false
                          end
                  end),
    get_cmd_result(Port, []).

cmd(Cmd) ->
    cmd(Cmd, []).

cmd(Cmd, Opts) ->
    wait_for_cmd(spawn_cmd(Cmd, Opts)).

get_cmd_result(Port, Acc) ->
    receive
        {Port, {data, Bytes}} ->
            get_cmd_result(Port, [Bytes|Acc]);
        {Port, {exit_status, Status}} ->
            Output = lists:flatten(lists:reverse(Acc)),
            {Status, Output}
    after 0 ->
            timeout
    end.

get_ip(_Node) ->
    %% localhost 4 lyfe
    "127.0.0.1".

node_id(Node) ->
    NodeMap = rt_config:get(rt_nodes),
    orddict:fetch(Node, NodeMap).

node_version(N) ->
    VersionMap = rt_config:get(rt_versions),
    orddict:fetch(N, VersionMap).

riak(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Result = run_riak(N, Path, Args),
    lager:info("~s", [Result]),
    {ok, Result}.

-spec set_conf(atom() | string(), [{string(), string()}]) -> ok.
set_conf(all, NameValuePairs) ->
    lager:info("rtdev:set_conf(all, ~p)", [NameValuePairs]),
    [ set_conf(DevPath, NameValuePairs) || DevPath <- devpaths()],
    ok;
set_conf(Node, NameValuePairs) when is_atom(Node) ->
    append_to_conf_file(get_riak_conf(Node), NameValuePairs),
    ok;
set_conf(DevPath, NameValuePairs) ->
    [append_to_conf_file(RiakConf, NameValuePairs) || RiakConf <- all_the_files(DevPath, "etc/riak.conf")],
    ok.

whats_up() ->
    io:format("Here's what's running...~n"),

    Up = [rpc:call(Node, os, cmd, ["pwd"]) || Node <- nodes()],
    [io:format("  ~s~n",[string:substr(Dir, 1, length(Dir)-1)]) || Dir <- Up].

riak_admin_cmd(Path, N, Args) ->
    Quoted =
        lists:map(fun(Arg) when is_list(Arg) ->
                          lists:flatten([$", Arg, $"]);
                     (_) ->
                          erlang:error(badarg)
                  end, Args),
    ArgStr = string:join(Quoted, " "),
    ExecName = rt_config:get(exec_name, "riak"),
    io_lib:format("~s/dev/dev~b/bin/~s-admin ~s", [Path, N, ExecName, ArgStr]).

% Private functions

relpath(Vsn) ->
    Path = ?PATH,
    relpath(Vsn, Path).

relpath(Vsn, Paths=[{_,_}|_]) ->
    orddict:fetch(Vsn, orddict:from_list(Paths));
relpath(current, Path) ->
    Path;
relpath(root, Path) ->
    Path;
relpath(_, _) ->
    throw("Version requested but only one path provided").

riakcmd(Path, N, Cmd) ->
    ExecName = rt_config:get(exec_name, "riak"),
    io_lib:format("~s/dev/dev~b/bin/~s ~s", [Path, N, ExecName, Cmd]).

run_riak(N, Path, Cmd) ->
    lager:info("Running: ~s", [riakcmd(Path, N, Cmd)]),
    R = os:cmd(riakcmd(Path, N, Cmd)),
    case Cmd of
        "start" ->
            rt_cover:maybe_start_on_node(?DEV(N), node_version(N)),
            %% Intercepts may load code on top of the cover compiled
            %% modules. We'll just get no coverage info then.
            case rt_intercept:are_intercepts_loaded(?DEV(N)) of
                false ->
                    ok = rt_intercept:load_intercepts([?DEV(N)]);
                true ->
                    ok
            end,
            R;
        "stop" ->
            rt_cover:maybe_stop_on_node(?DEV(N)),
            R;
        _ ->
            R
    end.

append_to_conf_file(File, NameValuePairs) ->
    Settings = lists:flatten(
        [io_lib:format("~n~s = ~s~n", [Name, Value]) || {Name, Value} <- NameValuePairs]),
    file:write_file(File, Settings, [append]).

get_riak_conf(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    io_lib:format("~s/dev/dev~b/etc/riak.conf", [Path, N]).

all_the_files(DevPath, File) ->
    case filelib:is_dir(DevPath) of
        true ->
            Wildcard = io_lib:format("~s/dev/dev*/~s", [DevPath, File]),
            filelib:wildcard(Wildcard);
        _ ->
            lager:debug("~s is not a directory.", [DevPath]),
            []
    end.

devpaths() ->
    lists:usort([ DevPath || {_Name, DevPath} <- proplists:delete(root, rt_config:get(rtdev_path))]).
