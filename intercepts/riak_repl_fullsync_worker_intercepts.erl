%% Intercepts functions for the riak_test in ../tests/repl_rt_heartbeat.erl
-module(riak_repl_fullsync_worker_intercepts).
-compile(export_all).
-include("intercept.hrl").

%% @doc Forward the heartbeat messages from the rt source by
%%      calling the original function.
slow_do_binputs_internal(BinObjs, DoneFun, Pool, Ver) ->
    %% ?I_INFO("forward_heartbeat"),
    lager:log(info, self(), "stop!!! ~p", [self()]),
    timer:sleep(15000),
    riak_repl_fullsync_worker_orig:do_binputs_internal(BinObjs, DoneFun, Pool, Ver).
