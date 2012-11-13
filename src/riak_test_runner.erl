%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc riak_test_runner runs a riak_test module's run/0 function. 
-module(riak_test_runner).
-export([confirm/3]).

-spec(confirm(integer(), atom(), [{atom(), term()}]) -> [tuple()]).
%% @doc Runs a module's run/0 function after setting up a log capturing backend for lager. 
%%      It then cleans up that backend and returns the logs as part of the return proplist.
confirm(TestModule, Outdir, TestMetaData) ->
    start_lager_backend(TestModule, Outdir),
    rt:setup_harness(TestModule, []),
    
    %% Check for api compatibility
    {Status, Reason, Backend} = case proplists:get_value(confirm, 
                        proplists:get_value(exports, TestModule:module_info()),
                        -1) of
        0 ->
            lager:notice("Running Test ~s", [TestModule]), 
            SetBackend = rt:set_backend(proplists:get_value(backend, TestMetaData)),
            {S, R} = execute(TestModule, TestMetaData),
            {S, R, SetBackend};
        _ ->
            lager:info("~s is not a runnable test", [TestModule]),
            {not_a_runnable_test, undefined, undefined}
    end,
    
    lager:notice("~s Test Run Complete", [TestModule]),
    {ok, Log} = stop_lager_backend(),
    Logs = iolist_to_binary(lists:foldr(fun(L, Acc) -> [L ++ "\n" | Acc] end, [], Log)),
    
    
    RetList = [{test, TestModule}, {status, Status}, {log, Logs}, {backend, Backend} | proplists:delete(backend, TestMetaData)],
    case Status of
        fail -> RetList ++ [{reason, iolist_to_binary(io_lib:format("~p", [Reason]))}];
        _ -> RetList
    end.
    
    
start_lager_backend(TestModule, Outdir) ->
    case Outdir of
        undefined -> ok;
        _ -> 
            gen_event:add_handler(lager_event, lager_file_backend, {Outdir ++ "/" ++ atom_to_list(TestModule) ++ ".dat_test_output", debug, 10485760, "$D0", 1}),
            lager:set_loglevel(lager_file_backend, debug)
    end,
    gen_event:add_handler(lager_event, riak_test_lager_backend, [debug, false]),
    lager:set_loglevel(riak_test_lager_backend, debug).
    
stop_lager_backend() ->
    gen_event:delete_handler(lager_event, lager_file_backend, []),
    gen_event:delete_handler(lager_event, riak_test_lager_backend, []).

%% does some group_leader swapping, in the style of EUnit.
execute(TestModule, TestMetaData) ->
    process_flag(trap_exit, true),
    GroupLeader = group_leader(),
    NewGroupLeader = riak_test_group_leader:new_group_leader(self()),
    group_leader(NewGroupLeader, self()),
    
    Pid = case proplists:get_value(confirm, 
                        proplists:get_value(exports, TestModule:module_info()),
                        -1) of
        1 ->
            spawn_link(TestModule, confirm, [TestMetaData]);
        0 ->
            spawn_link(TestModule, confirm, []);
        %% This isn't reachable, because Arities greater than 1 are caught in confirm/3.
        %% still, better safe than sorry.
        BadArity ->
            spawn_link(fun() -> erlang:error("~p:confirm/~p is not exported.", [TestModule, BadArity]) end)
    end,
    Return = rec_loop(Pid, TestModule, TestMetaData),
    group_leader(GroupLeader, self()),
    Return.


rec_loop(Pid, TestModule, TestMetaData) ->
    receive
        metadata ->
            Pid ! {metadata, TestMetaData},
            rec_loop(Pid, TestModule, TestMetaData);
        {'EXIT', Pid, normal} -> {pass, undefined};
        {'EXIT', Pid, Error} ->
            lager:warning("~s failed: ~p", [TestModule, Error]),
            {fail, Error}
    end.