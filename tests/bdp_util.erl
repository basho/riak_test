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
%% @doc Helper functions for bdp_service_manager.erl and others.

-module(bdp_util).

-export([build_cluster/1,
         make_node_leave/2, make_node_join/2]).
-export([get_services/1, wait_services/2]).

-include_lib("eunit/include/eunit.hrl").


%% copied from ensemble_util.erl
build_cluster(Size) ->
    Nodes = rt:deploy_nodes(Size),
    rt:join_cluster(Nodes),
    ensemble_util:wait_until_cluster(Nodes),
    Nodes.


get_services(Node) ->
    {Running_, Available_} =
        case rpc:call(Node, data_platform_global_state, services, []) of
            {error, timeout} ->
                lager:info("RPC call to ~p timed out", [Node]),
                ?assert(false);
            {badrpc, Reason} ->
                lager:info("RPC call to ~p failed with reason: ~p", [Node, Reason]),
                ?assert(false);
            Result ->
                Result
        end,
    {Running, Available} =
        {lists:sort([SName || {_Type, SName, _Node} <- Running_]),
         lists:sort([SName || {SName, _Type, _Conf} <- Available_])},
    lager:debug("Services running: ~p, available: ~p", [Running, Available]),
    {Running, Available}.

wait_services(Node, Services) ->
    wait_services_(Node, Services, 20).
wait_services_(_Node, _Services, SecsToWait) when SecsToWait =< 0 ->
    {error, services_not_ready};
wait_services_(Node, Services, SecsToWait) ->
    case get_services(Node) of
        Services ->
            ok;
        _Incomplete ->
            timer:sleep(1000),
            wait_services_(Node, Services, SecsToWait - 1)
    end.




%% Makeshift SM API: node join/leave.
%% Bits of code copied here from data_platform_console
%% This really calls for implementing proper SM API in data_platform_*.

make_node_leave(Node, FromNode) ->
    lager:info("away goes ~p", [Node]),
    rt:wait_until(
      fun() -> ok == do_leave(Node, FromNode) end).

do_leave(Node, FromNode) ->
    F = fun(M, F, A) -> rpc:call(FromNode, M, F, A) end,
    RootLeader = F(riak_ensemble_manager, rleader_pid, []),
    case F(riak_ensemble_peer, update_members, [RootLeader, [{del, {root, Node}}]]) of
        ok ->
            case wait_for_root_leave(F, Node, 30) of
                ok ->
                    NewRootLeader = F(riak_ensemble_manager, rleader_pid, []),
                    case F(riak_ensemble_manager, remove, [node(NewRootLeader), Node]) of
                        ok ->
                            ok;
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

wait_for_root_leave(F, Node, Timeout) ->
    wait_for_root_leave(F, Node, 0, Timeout).

wait_for_root_leave(_F, _Node, RetryCount, RetryLimit) when RetryCount =:= RetryLimit ->
    {error, timeout_waiting_to_leave_root_ensemble};
wait_for_root_leave(F, Node, RetryCount, RetryLimit) ->
    case in_root_ensemble(F, Node) of
        true ->
            timer:sleep(1000),
            wait_for_root_leave(F, Node, RetryCount + 1, RetryLimit);
        false ->
            ok
    end.

in_root_ensemble(F, Node) ->
    RootNodes = [N || {root, N} <- F(riak_ensemble_manager, get_members, [root])],
    lists:member(Node, RootNodes).


make_node_join(Node, ToNode) ->
    lager:info("Join ~p", [Node]),
    %% ring-readiness and transfer-complete status is of no
    %% consequence for our tests, so just proceed
    case rpc:call(ToNode, riak_ensemble_manager, join, [Node, ToNode]) of
        ok ->
            ok;
        remote_not_enabled ->
            %% it's an error, in the context of a test run
            lager:error("Ensemble not enabled on node ~p", [Node]);
        Error ->
            lager:error("Joining node ~p from ~p failed: ~p", [Node, ToNode, Error])
    end.
