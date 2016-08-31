%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------

-module(riak_core_vnode_manager_intercepts).
-include("intercept.hrl").
%% API
-compile(export_all).
-define(M, riak_core_vnode_manager_orig).

return_dead_process_pid_from_get_vnode_pid(Index, VNodeMod = riak_pipe_vnode) ->
    %% ?I_INFO("Intercepting riak_core_vnode_master:get_vnode_pid"),
    random:seed(os:timestamp()),
    case random:uniform(100) of
        7 ->
            %% Simulate what happens when a VNode completes handoff between get_vnode_pid
            %% and the fold attempting to start - other attempts to intercept and slow
            %% certain parts of Riak to invoke the particular race condition were unsuccessful
            ?I_INFO("Replaced VNode with spawned function in get_vnode_pid"),
            VNodePid = spawn(fun() ->
                ok
                             end),
            MonRef = erlang:monitor(process, VNodePid),
            receive
                {'DOWN', MonRef, process, VNodePid, _Reason} -> ok
            end,
            {ok, VNodePid};
        _ ->
            ?M:get_vnode_pid_orig(Index, VNodeMod)
    end;
return_dead_process_pid_from_get_vnode_pid(Index, VNodeMod) ->
    ?M:get_vnode_pid_orig(Index, VNodeMod).