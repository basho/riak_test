%% Intercepts functions for the riak_test in ../tests/repl_rt_heartbeat.erl
-module(riak_repl2_rtsink_helper_intercepts).
-compile(export_all).
-include("intercept.hrl").

%% @doc Forward the heartbeat messages from the rt source by
%%      calling the original function.
slow_do_write_objects(BinObjs, DoneFun, Ver) ->
    %% ?I_IFO("forward_heartbeat"),
    %% lager:log(info, self(), "boom! ~p", [process_info(self())]),
    lager:log(info, self(), "boom! ~p", [self()]),
    receive 
        noooooooooooooooooop ->  %% blocking forever
            ok
    end,
    riak_repl2_rtsink_helper_orig:do_write_objects(BinObjs, DoneFun, Ver).
