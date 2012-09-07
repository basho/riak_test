-module(riak_test_group_leader).

-export([new_group_leader/1, group_leader_process/1]).

% @doc stolen without shame from eunit_proc.erl
new_group_leader(Runner) ->
    spawn_link(?MODULE, group_leader_process, [Runner]).

% @doc stolen without shame from eunit_proc.erl
group_leader_process(Runner) ->
    group_leader_loop(Runner, infinity, []).

% @doc stolen without shame from eunit_proc.erl
group_leader_loop(Runner, Wait, Buf) ->
    receive
    {io_request, From, ReplyAs, Req} ->
        lager:notice("io_request"),
        P = process_flag(priority, normal),
        %% run this part under normal priority always
        Buf1 = io_request(From, ReplyAs, Req, Buf),
        process_flag(priority, P),
        group_leader_loop(Runner, Wait, Buf1);
    stop ->
        lager:notice("stop"),
        %% quitting time: make a minimal pause, go low on priority,
        %% set receive-timeout to zero and schedule out again
        receive after 2 -> ok end,
        process_flag(priority, low),
        group_leader_loop(Runner, 0, Buf);
    _ ->
        %% discard any other messages
        group_leader_loop(Runner, Wait, Buf)
    after Wait ->
        lager:notice("after after"),
        %% no more messages and nothing to wait for; we ought to
        %% have collected all immediately pending output now
        process_flag(priority, normal),
        Runner ! {self(), buffer_to_binary(Buf)}
    end.

% @doc stolen without shame from eunit_proc.erl
buffer_to_binary([B]) when is_binary(B) -> B;  % avoid unnecessary copying
buffer_to_binary(Buf) -> list_to_binary(lists:reverse(Buf)).

%% Implementation of buffering I/O for group leader processes. (Note that
%% each batch of characters is just pushed on the buffer, so it needs to
%% be reversed when it is flushed.)

io_request(From, ReplyAs, Req, Buf) ->
    {Reply, Buf1} = io_request(Req, Buf),
    io_reply(From, ReplyAs, Reply),
    Buf1.

io_reply(From, ReplyAs, Reply) ->
    From ! {io_reply, ReplyAs, Reply}.

io_request({put_chars, Chars}, _Buf) ->
    log_chars(Chars),
    {ok, []};
io_request({put_chars, M, F, As}, Buf) ->
    try apply(M, F, As) of
    Chars ->
        log_chars(Chars), 
        {ok, []}
    catch
    C:T -> {{error, {C,T,erlang:get_stacktrace()}}, Buf}
    end;
io_request({put_chars, _Enc, Chars}, Buf) ->
    io_request({put_chars, Chars}, Buf);
io_request({put_chars, _Enc, Mod, Func, Args}, Buf) ->
    io_request({put_chars, Mod, Func, Args}, Buf);
io_request({get_chars, _Enc, _Prompt, _N}, Buf) ->
    {eof, Buf};
io_request({get_chars, _Prompt, _N}, Buf) ->
    {eof, Buf};
io_request({get_line, _Prompt}, Buf) ->
    {eof, Buf};
io_request({get_line, _Enc, _Prompt}, Buf) ->
    {eof, Buf};
io_request({get_until, _Prompt, _M, _F, _As}, Buf) ->
    {eof, Buf};
io_request({setopts, _Opts}, Buf) ->
    {ok, Buf};
io_request(getopts, Buf) ->
    {error, {error, enotsup}, Buf};
io_request({get_geometry,columns}, Buf) ->
    {error, {error, enotsup}, Buf};
io_request({get_geometry,rows}, Buf) ->
    {error, {error, enotsup}, Buf};
io_request({requests, Reqs}, Buf) ->
    io_requests(Reqs, {ok, Buf});
io_request(_, Buf) ->
    {{error, request}, Buf}.

io_requests([R | Rs], {ok, Buf}) ->
    io_requests(Rs, io_request(R, Buf));
io_requests(_, Result) ->
    Result.

log_chars(Chars) ->
    [lager:debug("~s", [Line]) || Line <- string:tokens(lists:flatten(Chars), "\n")].