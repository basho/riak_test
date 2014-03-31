-module(riak_repl2_fssource_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl2_fssource_orig).

slow_handle_info(Msg, State) ->
    io:format("slow_handle_info~n"),
    ?I_INFO("slow_handle_info~n"),
    timer:sleep(10),
    ?M:handle_info_orig(Msg, State).

really_slow_handle_info(Msg, State) ->
    io:format("really_slow_handle_info~n"),
    ?I_INFO("really_slow_handle_info~n"),
    timer:sleep(100),
    ?M:handle_info_orig(Msg, State).
