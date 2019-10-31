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

-module(ensemble_remove_node).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-compile({parse_transform, rt_intercept_pt}).
-define(M, riak_kv_ensemble_backend_orig).

confirm() ->
    NumNodes = 4,
    NVal = 4,
    Config = ensemble_util:fast_config(NVal),
    lager:info("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(NumNodes, Config, NVal),
    [Node, Node2, Node3, Node4] = Nodes,
    ok = ensemble_util:wait_until_stable(Node, NVal),
    lager:info("Store a value in the root ensemble"),
    {ok, _} = riak_ensemble_client:kput_once(Node, root, testerooni, 
        testvalue, 1000),
    lager:info("Read value from the root ensemble"),
    {ok, _} = riak_ensemble_client:kget(Node, root, testerooni, 1000),

    EnsembleStatusPid = spawn(fun()-> ensemble_status_server([]) end),
    rt_intercept:add(Node, {riak_kv_ensemble_backend, [{{maybe_async_update, 2},
        {[EnsembleStatusPid], 
        fun(Changes, State) ->
            case lists:keyfind(del, 1, Changes) of
                false ->
                    ?M:maybe_async_update_orig(Changes, State);
                {del, {_, Node}}-> 
                    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
                    ExitingMembers = riak_core_ring:members(Ring, [exiting]),
                    EnsembleStatusPid ! {exiting_members, Node, ExitingMembers},
                    ?M:maybe_async_update_orig(Changes, State)
            end
    end}}]}),

    lager:info("Removing Nodes 2, 3 and 4 from the cluster"),
    rt:leave(Node2),
    ok = ensemble_util:wait_until_stable(Node, NVal),
    rt:leave(Node3),
    ok = ensemble_util:wait_until_stable(Node, NVal),
    rt:leave(Node4),
    ok = ensemble_util:wait_until_stable(Node, NVal),
    Remaining = Nodes -- [Node2, Node3, Node4],
    rt:wait_until_nodes_agree_about_ownership(Remaining), 
    ok = rt:wait_until_unpingable(Node2),
    ok = rt:wait_until_unpingable(Node3),
    ok = rt:wait_until_unpingable(Node4),
    lager:info("Read value from the root ensemble"),
    {ok, _Obj} = riak_ensemble_client:kget(Node, root, testerooni, 1000),
    Members3 = rpc:call(Node, riak_ensemble_manager, get_members, [root]),
    ?assertEqual(1, length(Members3)),
    timer:sleep(30000),
    Cluster = rpc:call(Node, riak_ensemble_manager, cluster, []),
    lager:info("Cluster ~w", [Cluster]),
    ?assertEqual(1, length(Cluster)),
    EnsembleStatusPid ! {get_errors, self()},
    ExitingErrors = receive E -> E end,
    ?assertEqual(ExitingErrors, []),
    pass.

ensemble_status_server(Errors) ->
    receive
        {exiting_members, Node, ExitingMembers} ->
            case lists:member(Node, ExitingMembers) of
                false -> 
                    ensemble_status_server(Errors);
                true ->
                    E = {invalid_exiting_status, Node, ExitingMembers},
                    ensemble_status_server([E | Errors])
            end;
        {get_errors, From} ->
            From ! Errors
    end.
            

