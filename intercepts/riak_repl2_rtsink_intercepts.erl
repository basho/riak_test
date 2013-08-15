%% Intercepts functions for the riak_test in ../tests/repl_rt_heartbeat.erl
-module(riak_repl2_rtsink_intercepts).
-compile(export_all).
-include("intercept.hrl").

%% @doc Reply with heartbeat ack at normal speed.
normal_send_heartbeat(T,S) ->
    %% ?I_INFO("forward_heartbeat"),
    riak_repl2_rtsink_conn_orig:send_heartbeat_orig(T,S).

%% @doc Slow down reply of heartbeat ack.
slow_send_heartbeat(T,S) ->
    %% ?I_INFO("drop_heartbeat"),
    F = fun() ->
                timer:sleep(1000),
                riak_repl2_rtsink_conn_orig:send_heartbeat_orig(T,S)
        end,
    spawn(F),
    ok.
