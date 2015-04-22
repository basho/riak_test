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
-module(riak_test_group_leader).

-export([new_group_leader/1, group_leader_loop/1, tidy_up/1]).

% @doc spawns the new group leader
new_group_leader(Runner) ->
    spawn_link(?MODULE, group_leader_loop, [Runner]).

% @doc listens for io_requests, and pipes them into lager
group_leader_loop(Runner) ->
    receive
    {io_request, From, ReplyAs, Req} ->
        P = process_flag(priority, normal),
        %% run this part under normal priority always
        io_request(From, ReplyAs, Req),
        process_flag(priority, P),
        group_leader_loop(Runner);
    stab ->
        kthxbye;
    _ ->
        %% discard any other messages
        group_leader_loop(Runner)
    end.

% @doc closes group leader down
tidy_up(FormerGroupLeader) ->
    GroupLeaderToMurder = group_leader(),
    group_leader(FormerGroupLeader, self()),
    GroupLeaderToMurder ! stab.

%% Processes an io_request and sends a reply
io_request(From, ReplyAs, Req) ->
    Reply = io_request(Req),
    io_reply(From, ReplyAs, Reply),
    ok.

%% sends a reply back to the sending process
io_reply(From, ReplyAs, Reply) ->
    From ! {io_reply, ReplyAs, Reply}.

%% If we're processing io:put_chars, Chars shows up as binary
io_request({put_chars, Chars}) when is_binary(Chars) ->
    io_request({put_chars, binary_to_list(Chars)});
io_request({put_chars, Chars}) ->
    log_chars(Chars),
    ok;
io_request({put_chars, M, F, As}) ->
    try apply(M, F, As) of
    Chars ->
        log_chars(Chars), 
        ok
    catch
    C:T -> {error, {C,T,erlang:get_stacktrace()}}
    end;
io_request({put_chars, _Enc, Chars}) ->
    io_request({put_chars, Chars});
io_request({put_chars, _Enc, Mod, Func, Args}) ->
    io_request({put_chars, Mod, Func, Args});
%% The rest of these functions just handle expected messages from
%% the io module. They're mostly i, but we only care about o.
io_request({get_chars, _Enc, _Prompt, _N}) ->
    eof;
io_request({get_chars, _Prompt, _N}) ->
    eof;
io_request({get_line, _Prompt}) ->
    eof;
io_request({get_line, _Enc, _Prompt}) ->
    eof;
io_request({get_until, _Prompt, _M, _F, _As}) ->
    eof;
io_request({setopts, _Opts}) ->
    ok;
io_request(getopts) ->
    {error, enotsup};
io_request({get_geometry,columns}) ->
    {error, enotsup};
io_request({get_geometry,rows}) ->
    {error, enotsup};
io_request({requests, Reqs}) ->
    io_requests(Reqs, ok);
io_request(_) ->
    {error, request}.

io_requests([R | Rs], ok) ->
    io_requests(Rs, io_request(R));
io_requests(_, Result) ->
    Result.

%% If we get multiple lines, we'll split them up for lager to maximize the prettiness.
log_chars(Chars) ->
    [lager:info("~s", [Line]) || Line <- string:tokens(lists:flatten(Chars), "\n")].
