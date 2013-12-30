%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
-module(verify_aae_throttle).
-export([confirm/0]).
-export([remote_suspend_vnode/1, remote_vnode_queue/1]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    Config = [{riak_kv, [{anti_entropy_build_limit, {100, 1000}},
                         {anti_entropy_concurrency, 100},
                         {anti_entropy_tick, 1000},
                         {anti_entropy, {on, []}}]},
              {riak_core, [{vnode_management_timer, 1000},
                           {ring_creation_size, 16}]}],
    Nodes = rt:build_cluster(3, Config),
    Node1 = hd(Nodes),
    rt:load_modules_on_nodes([?MODULE], Nodes),
    Limits = rpc:call(Node1, riak_kv_entropy_manager, get_aae_throttle_limits, []),
    {ok, Ring} = rpc:call(Node1, riak_core_ring_manager, get_my_ring, []),
    Owners = riak_core_ring:all_owners(Ring),

    ?assert(Limits =/= []),
    lager:info("AAE limits: ~p", [Limits]),

    {_, Baseline} = hd(Limits),
    lager:info("Check baseline throttle of ~b", [Baseline]),
    true = check_aae_throttle(Nodes, Baseline),

    lager:info("Progressively backup vnodes and check for proper throttle increase"),
    Stalled = trigger_throttles(tl(Limits), Owners, Owners, Nodes, []),

    lager:info("Resume vnodes in reverse order, checking for proper throttle decrease"),
    Stalled2 = [{Len, Suspended, VNode} || {VNode, {Suspended, Len}} <- Stalled],
    Stalled3 = lists:reverse(lists:sort(Stalled2)),
    resume_stalled(Stalled3, lists:reverse(lists:sort(Limits)), Nodes),
    pass.

check_aae_throttle(Nodes, Expected) ->
    {Throttles, []} = rpc:multicall(Nodes, riak_kv_entropy_manager, get_aae_throttle, []),
    lists:all(fun(X) -> X =:= Expected end, Throttles).

trigger_throttles([], _, _, _, Stalled) ->
    Stalled;
trigger_throttles(Limits, [], AllOwners, Nodes, Stalled) ->
    trigger_throttles(Limits, AllOwners, AllOwners, Nodes, Stalled);
trigger_throttles([{VMax, Throttle}|Limits], [{Idx, Node}|Owners], AllOwners, Nodes, Stalled) ->
    Stalled2 = maybe_suspend_vnode(Idx, Node, Stalled),
    Len = vnode_queue(Node, Idx),
    Send = erlang:max(0, VMax - Len + 1),
    lager:info("~p/~p currently has ~b messages. Sending ~b more, for total of ~b",
               [Node, Idx, Len, Send, Len + Send]),
    [rpc:call(Node, riak_kv_vnode, request_hashtree_pid, [Idx]) || _ <- lists:seq(1, Send)],
    lager:info("Waiting until all nodes adjust throttle to ~b", [Throttle]),
    ok = rt:wait_until(fun() ->
                               check_aae_throttle(Nodes, Throttle)
                       end),
    Stalled3 = set_stalled(Idx, Node, Len + Send, Stalled2),
    trigger_throttles(Limits, Owners, AllOwners, Nodes, Stalled3).

resume_stalled([], _, _) ->
    ok;
resume_stalled([{_, Suspended, VNode}|Stalled], Limits, Nodes) ->
    NextWorst = case Stalled of
                    [{NextLen, _, _}|_] ->
                        NextLen;
                    [] ->
                        0
                end,
    [Throttle|_] = [Throttle || {Max, Throttle} <- Limits,
                                Max < NextWorst],
    {Idx, Node} = VNode,
    Len = vnode_queue(Node, Idx),
    lager:info("Resuming ~p (message queue: ~b)", [VNode, Len]),
    resume_vnode(Suspended),
    lager:info("Waiting until all nodes adjust throttle to ~b", [Throttle]),
    ok = rt:wait_until(fun() ->
                               check_aae_throttle(Nodes, Throttle)
                       end),
    resume_stalled(Stalled, Limits, Nodes).

maybe_suspend_vnode(Idx, Node, Stalled) ->
    case orddict:find({Idx, Node}, Stalled) of
        error ->
            Suspended = suspend_vnode(Node, Idx),
            orddict:store({Idx, Node}, {Suspended, 0}, Stalled);
        _ ->
            Stalled
    end.

set_stalled(Idx, Node, Total, Stalled) ->
    orddict:update({Idx, Node},
                   fun({Suspended, _}) ->
                           {Suspended, Total}
                   end,
                   Stalled).

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

vnode_queue(Node, Idx) ->
    rpc:call(Node, ?MODULE, remote_vnode_queue, [Idx]).

remote_vnode_queue(Idx) ->
    {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Idx, riak_kv_vnode),
    {message_queue_len, Len} = process_info(Pid, message_queue_len),
    Len.

resume_vnode(Pid) ->
    Pid ! resume.
