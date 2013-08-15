%% Intercepts functions for the riak_test in ../tests/repl_rt_heartbeat.erl
-module(riak_repl2_rtq_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl2_rtq).

%% @doc Drop the heartbeat messages from the rt source.
slow_trim_q(State) ->
    %% ?I_INFO("slow_trim_q"),
    timer:sleep(1),
    ?M:trim_q(State),
    ok.
