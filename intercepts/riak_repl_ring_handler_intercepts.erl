-module(riak_repl_ring_handler_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl_ring_handler_orig).

%% @doc Make all commands take abnormally long.
slow_handle_event(Event, State) ->
    io:format("slow handle event triggered by intercept", []),
    ?I_INFO("slow handle event triggered by intercept"),
    timer:sleep(500),
    ?M:handle_event_orig(Event, State).
