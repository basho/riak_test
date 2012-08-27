-module(riak_test_runner).
%% @doc riak_test_runner runs a riak_test module's run/0 function. 
-export([confirm/1]).

-spec(confirm(atom()) -> [tuple()]).
%% @doc Runs a module's run/0 function after setting up a log capturing backend for lager. 
%% It then cleans up that backend and returns the logs as part of the return proplist.
confirm(TestModule) ->
    LagerLevel = rt:config(rt_lager_level, debug),
    gen_event:add_handler(lager_event, riak_test_lager_backend, [LagerLevel, false]),
    
    Result = TestModule:confirm(),
    
    {ok, Log} = gen_event:delete_handler(lager_event, riak_test_lager_backend, []),
    
    [{status, Result}, {log, Log}].