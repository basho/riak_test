-module(riak_repl_aae_source_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl_aae_source_orig).

%% @doc Introduce 10ms of latency in receiving message off of the
%%      socket.
delayed_get_reply(State) ->
    io:format("delayed~n"),
    ?I_INFO("delayed~n"),
    timer:sleep(10),
    ?M:get_reply_orig(State).

%% @doc Introduce 100ms of latency in receiving message off of the
%%      socket.
really_delayed_get_reply(State) ->
    io:format("really delayed~n"),
    ?I_INFO("really delayed~n"),
    timer:sleep(100),
    ?M:get_reply_orig(State).
