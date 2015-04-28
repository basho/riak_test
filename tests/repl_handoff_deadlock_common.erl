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
%% -------------------------------------------------------------------

%%% @copyright (C) 2015, Basho Technologies
%%% @doc
%%% riak_test for timing deadlock on riak_repl2_keylist_server:diff_bloom and riak_repl_aae_source:finish_sending_differences
%%% which can occur when the owner VNode is handed off after the original monitor (MonRef) is created. Neither function
%%% handles the {'DOWN', MonRef, process, VNodePid, normal} case (which happens when the node exits after handoff.
%%% Note that this uses code from verify_counter_repl as its tests, as that seemed to be able to evoke the issue, even without intercepts
%%%
%%% Also tests fixes for riak_repl2_fscoordinator which used to cache the owner of an index at replication start, but those could change with handoff.
%%% @end
-module(repl_handoff_deadlock_common).
-export([confirm/1]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"counter-bucket">>).
-define(KEY, <<"counter-key">>).

-define(CONF(Strategy), [
    {riak_kv,
        [
            %% Specify fast building of AAE trees
            {anti_entropy, {on, []}},
            {anti_entropy_build_limit, {100, 1000}},
            {anti_entropy_concurrency, 100}
        ]
    },
    {riak_repl,
        [
            {fullsync_strategy, Strategy},
            {fullsync_on_connect, false},
            {fullsync_interval, disabled},
            %% Force usage of Bloom to invoke race
            {fullsync_direct_percentage_limit, 0},
            {fullsync_direct_limit, 1}

        ]}
]).

confirm(Strategy) ->

    inets:start(),
    lager:info("Testing fullsync handoff deadlock with strategy ~p~n", [Strategy]),
    {ClusterA, ClusterB} = make_clusters(Strategy),

    %% Simulate stop of 1/10th of vnodes before fold begins to provoke deadlock
    [
        rt_intercept:add(Node, {riak_core_vnode_master,
            [{{command_return_vnode, 4},
                stop_vnode_after_bloom_fold_request_succeeds}]})
        || {_, Node} <- ClusterA ++ ClusterB],

    %% Write the data to both sides of the cluster
    write_data(hd(ClusterA), 1, 1000),
    write_data(hd(ClusterB), 1001, 1000),

    %% let the repl flow
    repl_power_activate(ClusterA, ClusterB),
    verify_data(hd(ClusterA), 2000),
    verify_data(hd(ClusterB), 2000),
    pass.

make_clusters(KeylistOrAae) ->
    Nodes = rt:deploy_nodes(6, ?CONF(KeylistOrAae), [riak_kv, riak_repl]),
    {ClusterA, ClusterB} = lists:split(3, Nodes),
    A = make_cluster(ClusterA, "A"),
    B = make_cluster(ClusterB, "B"),
    {A, B}.

make_cluster(Nodes, Name) ->
    repl_util:make_cluster(Nodes),
    repl_util:name_cluster(hd(Nodes), Name),
    repl_util:wait_until_leader_converge(Nodes),
    Clients = [ rt:pbc(Node) || Node <- Nodes ],
    lists:zip(Clients, Nodes).

write_data({Client, _Node}, Start, Count) ->
    [riakc_pb_socket:put(Client, riakc_obj:new(?BUCKET, integer_to_binary(Num), integer_to_binary(Num)))
        || Num <- lists:seq(Start, (Start-1) + Count)].

verify_data({Client, Node}, Count) ->
    [
        begin
            case (riakc_pb_socket:get(Client, ?BUCKET, integer_to_binary(Num), [{notfound_ok, false}])) of
                {error, notfound} ->
                    erlang:error({not_found, lists:flatten(io_lib:format("Could not find ~p in cluster with node ~p", [Num, Node]))});
                {ok, Object} ->
                    ?assertEqual(riakc_obj:get_value(Object), integer_to_binary(Num))
            end
        end
        || Num <- lists:seq(1, Count)].


%% Set up bi-directional full sync replication.
repl_power_activate(ClusterA, ClusterB) ->
    lager:info("repl power...ACTIVATE!"),
    LeaderA = get_leader(hd(ClusterA)),
    info("got leader A"),
    LeaderB = get_leader(hd(ClusterB)),
    info("Got leader B"),
    MgrPortA = get_mgr_port(hd(ClusterA)),
    info("Got manager port A"),
    MgrPortB = get_mgr_port(hd(ClusterB)),
    info("Got manager port B"),
    info("connecting A to B"),
    repl_util:connect_cluster(LeaderA, "127.0.0.1", MgrPortB),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderA, "B")),
    info("A connected to B"),
    info("connecting B to A"),
    repl_util:connect_cluster(LeaderB, "127.0.0.1", MgrPortA),
    ?assertEqual(ok, repl_util:wait_for_connection(LeaderB, "A")),
    info("B connected to A"),
    info("Enabling Fullsync bi-directional"),
    repl_util:enable_fullsync(LeaderA, "B"),
    info("Enabled A->B"),
    repl_util:enable_fullsync(LeaderB, "A"),
    info("Enabled B->A"),
    info("Awaiting fullsync completion"),
    repl_util:start_and_wait_until_fullsync_complete(LeaderA),
    info("A->B complete"),
    repl_util:start_and_wait_until_fullsync_complete(LeaderB),
    info("B->A complete").

get_leader({_, Node}) ->
    rpc:call(Node, riak_core_cluster_mgr, get_leader, []).

get_mgr_port({_, Node}) ->
    {ok, {_IP, Port}} = rpc:call(Node, application, get_env,
        [riak_core, cluster_mgr]),
    Port.

info(Message) ->
    lager:info(Message).
