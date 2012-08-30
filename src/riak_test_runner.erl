-module(riak_test_runner).
%% @doc riak_test_runner runs a riak_test module's run/0 function. 
-export([confirm/2]).

-spec(confirm(atom(), string()) -> [tuple()]).
%% @doc Runs a module's run/0 function after setting up a log capturing backend for lager. 
%% It then cleans up that backend and returns the logs as part of the return proplist.
confirm(TestModule, Outdir) ->
    start_lager_backend(TestModule, Outdir),
    
    %% Check for api compatibility
    {Status, Reason} = case proplists:get_value(confirm, 
                        proplists:get_value(exports, TestModule:module_info()),
                        -1) of
        0 ->
            lager:notice("Running Test ~s", [TestModule]), 
            execute(TestModule);
        _ ->
            lager:info("~s is not a runable test", [TestModule]),
            {not_a_runable_test, undefined}
    end,
    
    lager:notice("~s Test Run Complete", [TestModule]),
    {ok, Log} = stop_lager_backend(),
    
    RetList = [{test, TestModule}, {status, Status}, {log, Log}],
    case Status of
        fail -> RetList ++ [{reason, Reason}];
        _ -> RetList
    end.
    
    
start_lager_backend(TestModule, Outdir) ->
    case Outdir of
        undefined -> ok;
        _ -> gen_event:add_handler(lager_event, lager_file_backend, {Outdir ++ "/" ++ atom_to_list(TestModule) ++ ".dat_test_output", debug, 10485760, "$D0", 1})
    end,
    gen_event:add_handler(lager_event, riak_test_lager_backend, [debug, false]).
    
stop_lager_backend() ->
    gen_event:delete_handler(lager_event, lager_file_backend, []),
    gen_event:delete_handler(lager_event, riak_test_lager_backend, []).
    
execute(TestModule) ->
    try TestModule:confirm() of
        ReturnVal -> {ReturnVal, undefined}
    catch
        error:Error ->
            lager:warning("~s failed: ~p", [TestModule, Error]),
            {fail, Error}
    end.
    