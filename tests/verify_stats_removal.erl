%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

%% @doc
%% Testing a fix for an issue where, once vnode-level stats were created, they were not removed
%% even when the vnode was handed off. Not a big deal, unless you have 1024 vnodes, in which case
%% the first node in a rolling restart of the cluster will consume significantly more memory
%% than the others, as at some point it will have been a fallback for many of the vnodes.
%% Tests https://github.com/basho/riak_kv/pull/1282
%% TODO: Need to also test for vnode crashing, not just clean shutdown.
%%
-module(verify_stats_removal).
-behavior(riak_test).
-export([confirm/0, get_stats_remote/1]).
-include_lib("eunit/include/eunit.hrl").



confirm() ->
    Nodes = rt:deploy_nodes(2),
    [Node1, Node2] = Nodes,
    %% Need to write some data to pump the stats system - seems at least 5 values before it starts counting
    %% stolen from verify_riak_stats
    C = rt:httpc(Node1),
    [rt:httpc_write(C, <<"systest">>, <<X>>, <<"12345">>) || X <- lists:seq(1, 5)],
    [rt:httpc_read(C, <<"systest">>, <<X>>) || X <- lists:seq(1, 5)],

    %% Now, join the nodes into a cluster.
    rt:join(Node2, Node1),
    rt:wait_until_all_members(Nodes),
    rt:wait_until_no_pending_changes(Nodes),
    rt:wait_until_transfers_complete(Nodes),
    rt:load_modules_on_nodes([?MODULE], Nodes),
    NonRunningStatsCount1 = get_stats_count_for_non_running_vnodes(Node1),
    ?assertEqual(0, NonRunningStatsCount1),
    NonRunningStatsCount2 = get_stats_count_for_non_running_vnodes(Node2),
    ?assertEqual(0, NonRunningStatsCount2),
    pass.

get_stats_count_for_non_running_vnodes(Node1) ->
    spawn_link(Node1, verify_stats_removal, get_stats_remote, [self()]),
    receive
        {stats, NumStats} ->
            NumStats;
        _Other ->
            throw("Failed to retrieve count of stats from vnode.")
    end.

get_stats_remote(Sender) ->
    Running = [list_to_atom(integer_to_list(Idx)) || {_, Idx, _} <- riak_core_vnode_manager:all_vnodes(
        riak_kv_vnode)],
    Counters = [N || {[riak, riak_kv, vnode, Op, Idx] = N, _, _} <- exometer:find_entries(['_']), not lists:member(
        Idx, Running), Op == gets orelse Op == puts, Idx /= time],
    Spirals = [N || {[riak, riak_kv, vnode, Op, time, Idx] = N, _, _} <- exometer:find_entries(
        ['_']), not lists:member(Idx, Running), Op == gets orelse Op == puts],
    Stats = Counters ++ Spirals,
    Sender ! {stats, length(Stats)}.


%% Spirals = [N || {[riak, riak_kv, vnode, Op, time, Idx] = N, _, _} <- exometer:find_entries(['_']), Op == gets orelse Op == puts].
