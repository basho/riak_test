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
-module(verify_dynamic_ring).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"dynring">>).
-define(W, 2).
-define(R, 2).
-define(START_SIZE, 16).
-define(EXPANDED_SIZE, 64).
-define(SHRUNK_SIZE, 8).

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt_config:set_conf(all, [{"buckets.default.allow_mult", "false"}]),
    rt_config:update_app_config(all, [{riak_core,
                                [{ring_creation_size, ?START_SIZE}]}]),
    [ANode, AnotherNode, YetAnother, _ReplacingNode] = _AllNodes = rt_cluster:deploy_nodes(4),
    NewNodes = Nodes = [ANode, AnotherNode, YetAnother],
    %% This assignment for `NewNodes' is commented until riak_core
    %% issue #570 is resolved
    %% NewNodes = [ANode, YetAnother, ReplacingNode],
    rt_node:join(AnotherNode, ANode),
    rt_node:join(YetAnother, ANode),
    rt_node:wait_until_nodes_agree_about_ownership(Nodes),
    rt:wait_until_ring_converged(Nodes),
    rt:wait_until_no_pending_changes(Nodes),

    test_resize(?START_SIZE, ?EXPANDED_SIZE, ANode, Nodes),

    test_resize(?EXPANDED_SIZE, ?SHRUNK_SIZE, ANode, Nodes),
    wait_until_extra_vnodes_shutdown(Nodes),
    wait_until_extra_proxies_shutdown(Nodes),

    lager:info("writing 500 keys"),
    ?assertEqual([], rt_systest:write(ANode, 1, 500, ?BUCKET, ?W)),
    test_resize(?SHRUNK_SIZE, ?START_SIZE, ANode, Nodes, {501, 750}),
    lager:info("verifying previously written data"),
    ?assertEqual([], rt_systest:read(ANode, 1, 500, ?BUCKET, ?R)),

    test_resize(?START_SIZE, ?EXPANDED_SIZE, ANode, Nodes),
    lager:info("verifying previously written data"),
    ?assertEqual([], rt_systest:read(ANode, 1, 750, ?BUCKET, ?R)),

    %% This following test code for force-replace is commented until
    %% riak_core issue #570 is resolved. At that time the preceding 3
    %% lines should also be removed

    %% lager:info("testing force-replace during resize"),
    %% submit_resize(?EXPANDED_SIZE, ANode),
    %% %% sleep for a second, yes i know this is nasty but we just care that the resize has
    %% %% been submitted and started, we aren't really waiting on a condition
    %% timer:sleep(3000),
    %% rpc:multicall(Nodes, riak_core_handoff_manager, kill_handoffs, []),
    %% Statuses = rpc:multicall(Nodes, riak_core_handoff_manager, status, []),
    %% lager:info("Handoff statuses: ~p", [Statuses]),
    %% ok = rpc:call(ReplacingNode, riak_core, staged_join, [ANode]),
    %% rt:wait_until_ring_converged(AllNodes),
    %% ok = rpc:call(ANode, riak_core_claimant, force_replace, [AnotherNode, ReplacingNode]),
    %% {ok, _, _} = rpc:call(ANode, riak_core_claimant, plan, []),
    %% ok = rpc:call(ANode, riak_core_claimant, commit, []),
    %% rpc:multicall(AllNodes, riak_core_handoff_manager, set_concurrency, [4]),
    %% rt:wait_until_no_pending_changes(NewNodes),
    %% assert_ring_size(?EXPANDED_SIZE, NewNodes),
    %% lager:info("verifying written data"),
    %% ?assertEqual([], rt_systest:read(ANode, 1, 750, ?BUCKET, ?R)),

    test_resize(?EXPANDED_SIZE, ?SHRUNK_SIZE, ANode, NewNodes),
    lager:info("verifying written data"),
    ?assertEqual([], rt_systest:read(ANode, 1, 750, ?BUCKET, ?R)),
    wait_until_extra_vnodes_shutdown(NewNodes),
    wait_until_extra_proxies_shutdown(NewNodes),

    lager:info("submitting resize to subsequently abort. ~p -> ~p", [?SHRUNK_SIZE, ?START_SIZE]),
    submit_resize(?START_SIZE, ANode),
    %% sleep for a second, yes i know this is nasty but we just care that the resize has
    %% made some progress. not really waiting on a condition
    timer:sleep(1000),
    lager:info("aborting resize operation, verifying cluster still has ~p partitions",
               [?SHRUNK_SIZE]),
    rpc:multicall(NewNodes, riak_core_handoff_manager, kill_handoffs, []),
    rt:wait_until_ring_converged(NewNodes),
    abort_resize(ANode),
    rt:wait_until_no_pending_changes(NewNodes),
    lager:info("verifying running vnodes abandoned during aborted resize are shutdown"),
    %% force handoffs so we don't wait on vnode manager tick
    rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
    wait_until_extra_vnodes_shutdown(NewNodes),
    lager:info("verifying vnodes abandoned during aborted resize have proxies shutdown"),
    wait_until_extra_proxies_shutdown(NewNodes),
    rt:wait_until_ring_converged(NewNodes),
    assert_ring_size(?SHRUNK_SIZE, NewNodes),
    lager:info("verifying written data"),
    ?assertEqual([], rt_systest:read(ANode, 1, 750, ?BUCKET, ?R)),

    pass.

test_resize(CurrentSize, NewSize, ANode, Nodes) ->
    test_resize(CurrentSize, NewSize, ANode, Nodes, {undefined, undefined}).

test_resize(CurrentSize, NewSize, ANode, Nodes, {WriteStart,WriteEnd}) ->
    assert_ring_size(CurrentSize, Nodes),
    Str = case CurrentSize > NewSize of
              true -> "shrinking";
              false -> "expansion"
          end,
    lager:info("testing ring ~s. ~p -> ~p", [Str, CurrentSize, NewSize]),
    rt:wait_until_ring_converged(Nodes),
    submit_resize(NewSize, ANode),
    write_during_resize(ANode, WriteStart, WriteEnd),
    rt:wait_until_no_pending_changes(Nodes),
    rt:wait_until_ring_converged(Nodes),
    assert_ring_size(NewSize, ANode),
    verify_write_during_resize(ANode, WriteStart, WriteEnd).

write_during_resize(_, Start, End) when Start =:= undefined orelse End =:= undefined ->
    ok;
write_during_resize(Node, Start, End) ->
    Pid = self(),
    spawn(fun() ->
                  case rt_systest:write(Node, Start, End, ?BUCKET, ?W) of
                      [] ->
                          Pid ! done_writing;
                      Ers ->
                          Pid ! {errors_writing, Ers}
                  end
          end).

verify_write_during_resize(_, Start, End) when Start =:= undefined orelse End =:= undefined ->
    ok;
verify_write_during_resize(Node, Start, End) ->
    receive
        done_writing ->
            lager:info("verifying data written during operation"),
            ?assertEqual([], rt_systest:read(Node, Start, End, ?BUCKET, ?R)),
            ok;
        {errors_writing, Ers} ->
            lager:error("errors were encountered while writing during operation: ~p", [Ers]),
            throw(writes_failed)
    after
        10000 ->
            lager:error("failed to complete writes during operation before timeout"),
            throw(writes_timedout)
    end.

submit_resize(NewSize, Node) ->
    ?assertEqual(ok, rpc:call(Node, riak_core_claimant, resize_ring, [NewSize])),
    {ok, _, _} = rpc:call(Node, riak_core_claimant, plan, []),
    ?assertEqual(ok, rpc:call(Node, riak_core_claimant, commit, [])).

abort_resize(Node) ->
    ?assertEqual(ok, rpc:call(Node, riak_core_claimant, abort_resize, [])),
    {ok, _, _} = rpc:call(Node, riak_core_claimant, plan, []),
    ?assertEqual(ok, rpc:call(Node, riak_core_claimant, commit, [])).

assert_ring_size(Size, Nodes) when is_list(Nodes) ->
    [assert_ring_size(Size, Node) || Node <- Nodes];
assert_ring_size(Size, Node) ->
    {ok, R} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    ?assertEqual(Size, riak_core_ring:num_partitions(R)).

wait_until_extra_vnodes_shutdown([]) ->
    ok;
wait_until_extra_vnodes_shutdown([Node | Nodes]) ->
    wait_until_extra_vnodes_shutdown(Node),
    wait_until_extra_vnodes_shutdown(Nodes);
wait_until_extra_vnodes_shutdown(Node) ->
    {ok, R} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    AllIndexes = [Idx || {Idx, _} <- riak_core_ring:all_owners(R)],
    F = fun(_N) ->
                Running = rpc:call(Node, riak_core_vnode_manager, all_index_pid, [riak_kv_vnode]),
                StillRunning = [Idx || {Idx, _} <- Running, not lists:member(Idx, AllIndexes)],
                length(StillRunning) =:= 0
        end,
    ?assertEqual(ok, rt:wait_until(Node, F)).

wait_until_extra_proxies_shutdown([]) ->
    ok;
wait_until_extra_proxies_shutdown([Node | Nodes]) ->
    wait_until_extra_proxies_shutdown(Node),
    wait_until_extra_proxies_shutdown(Nodes);
wait_until_extra_proxies_shutdown(Node) ->
    {ok, R} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
    AllIndexes = [Idx || {Idx, _} <- riak_core_ring:all_owners(R)],
    F = fun(_N) ->
                Running = running_vnode_proxies(Node),
                StillRunning = [Idx || Idx <- Running, not lists:member(Idx, AllIndexes)],
                length(StillRunning) =:= 0
         end,
    ?assertEqual(ok, rt:wait_until(Node, F)).

running_vnode_proxies(Node) ->
    Children = rpc:call(Node, supervisor, which_children, [riak_core_vnode_proxy_sup]),
    [Idx || {{_,Idx},Pid,_,_} <- Children, is_pid(Pid)].
