%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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

-module(vnode_util).
-compile(export_all).

load(Nodes) ->
    rt:load_modules_on_nodes([?MODULE], Nodes),
    ok.

suspend_vnode(Node, Idx) ->
    lager:info("Suspending vnode ~p/~p", [Node, Idx]),
    Pid = rpc:call(Node, ?MODULE, remote_suspend_vnode, [Idx], infinity),
    Pid.

remote_suspend_vnode(Idx) ->
    Parent = self(),
    Pid = spawn(fun() ->
                        {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Idx, riak_kv_vnode),
                        erlang:suspend_process(Pid, []),
                        Parent ! suspended,
                        receive resume ->
                                erlang:resume_process(Pid)
                        end
                end),
    receive suspended -> ok end,
    Pid.

resume_vnode(Pid) ->
    Pid ! resume.
