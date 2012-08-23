-module(riak_test_runner).

-export([run/1]).

run(TestModule) ->
    LagerLevel = rt:config(rt_lager_level, debug),
    gen_event:add_handler(lager_event, riak_test_lager_backend, [LagerLevel, false]),
    
    Result = TestModule:run(),
    
    {ok, Log} = gen_event:delete_handler(lager_event, riak_test_lager_backend, []),
    
    [{status, Result}, {log, Log}].