-module(riak_test_runner).
%% @doc riak_test_runner runs a riak_test module's run/0 function. 
-export([confirm/1]).

-spec(confirm(atom()) -> [tuple()]).
%% @doc Runs a module's run/0 function after setting up a log capturing backend for lager. 
%% It then cleans up that backend and returns the logs as part of the return proplist.
confirm(TestModule) ->
    start_lager_backend(),
    
    %% Check for api compatibility
    Result = case proplists:get_value(confirm, 
                        proplists:get_value(exports, TestModule:module_info()),
                        -1) of
        0 ->
            lager:info("Running Test ~s", [TestModule]), 
            execute(TestModule);
        _ ->
            lager:info("~s is not a runable test", [TestModule]),
            not_a_runable_test
    end,
    
    lager:info("~s Test Run Complete", [TestModule]),
    {ok, Log} = stop_lager_backend(),
    
    [{test, TestModule}, {status, Result}, {log, Log}].
    
    
start_lager_backend() ->
    LagerLevel = rt:config(rt_lager_level, debug),
    gen_event:add_handler(lager_event, riak_test_lager_backend, [LagerLevel, false]).
    
stop_lager_backend() ->
    gen_event:delete_handler(lager_event, riak_test_lager_backend, []).
    
execute(TestModule) ->
    try TestModule:confirm() of
        ReturnVal -> ReturnVal
    catch
        error:Error ->
            lager:warning("~s failed: ~p", [TestModule, Error]),
            fail
    end.
    