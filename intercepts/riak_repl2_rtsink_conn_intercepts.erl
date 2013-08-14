%% Intercepts functions for the riak_test in ../tests/repl_rt_heartbeat.erl
-module(riak_repl2_rtsink_conn_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl2_rtsink_conn).

%% @doc Drop the heartbeat messages from the rt source.
slow_handle_info({Proto, S, TcpBin}, State) ->
    %% ?I_INFO("slow_handle_info"),
    timer:sleep(2000),
    ?M:handle_info({Proto, S, TcpBin}, State),
    ok.
