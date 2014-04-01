%% Intercepts functions for the riak_test in ../tests/repl_rt_heartbeat.erl
-module(riak_repl2_rtsink_conn_intercepts).
-compile(export_all).
-include("intercept.hrl").

%% @doc Forward the heartbeat messages from the rt source by
%%      calling the original function.
forward_send_heartbeat_resp(Transport, Socket) ->
    %% ?I_INFO("forward_heartbeat"),
    riak_repl2_rtsource_helper_orig:send_heartbeat_orig(Transport, Socket).

%% @doc Drop the heartbeat messages from the rt source.
drop_send_heartbeat_resp(_Transport, _Socket) ->
    %% ?I_INFO("drop_heartbeat"),
    ok.
