-module(riak_repl_util_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_repl_util_orig).
start_fullsync_timer(Pid, FullsyncIvalMins, Cluster) ->
    io:format(user, "Scheduled fullsync from ~p ~p ~p~n",[Pid,
                                                          FullsyncIvalMins,
                                                          Cluster]),
    %% fs to B should always be 1 minute
    %% fs to C should always be 2 minutes
    %% the fs schedule test that doesn't specify
    %% a cluster uses 99
    case Cluster of
      "B" when  FullsyncIvalMins =/= 1
                andalso FullsyncIvalMins =/= 99
                -> throw("Invalid interval for cluster");
      "C" when  FullsyncIvalMins =/= 2
                andalso FullsyncIvalMins =/= 99
                -> throw("Invalid interval for cluster");
        _ -> ok
    end,
    gen_server:cast(Pid, start_fullsync).


schedule_fullsync(Pid) ->
    io:format(user, "Scheduled v2 fullsync in ~p minutes~n", [application:get_env(riak_repl,
                                                                    fullsync_interval)]),
    erlang:send_after(1, Pid, start_fullsync).

