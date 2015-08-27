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

-export([build_cluster/1, build_cluster/2,
         add_service/4, remove_service/2, start_seervice/4, stop_service/4]).
-export([get_services/1, wait_services/2]).

-include_lib("eunit/include/eunit.hrl").

-type group_name() :: string().
-type config_name() :: string().
-type service_type() :: string().
-type service_config() :: [{string(), string()}].
-type service() :: {group_name(), config_name(), node()}.
-type package() :: {config_name(), service_type(), service_config()}.

-define(SM_RPC_RETRIES, 20).

%% @ignore
%% This is a workaround for the nasty habit of Service Manager to kick
%% back with an {error, timeout} in response to any call (in
%% particular, any rpc calls from riak_test over to dev1).  The error
%% is returned immediately, which is weird, but goes away after some 8
%% seconds of trying.  It's reasonable to stick that to SM itself and
%% get it fixed there; for the time being, this workaround is here.

call_with_patience(Node, M, F, A) ->
    call_with_patience_(Node, M, F, A, ?SM_RPC_RETRIES).
call_with_patience_(Node, M, F, A, 0) ->
    lager:error("Exhausted ~b retries for an RPC call to ~p ~p:~p/~b",
                [?SM_RPC_RETRIES, Node, M, F, length(A)]),
    error({rpc_retries_exhausted, {Node, M, F, A}});
call_with_patience_(Node, M, F, A, Retries) ->
    case rpc:call(Node, M, F, A) of
        {badrpc, Reason} = Error ->
            lager:error("RPC call to ~p failed with reason: ~p", [Node, Reason]),
            error(Error);
        {error, timeout} ->
            lager:warning("RPC call to ~p:~p/~b on ~p timed out, ~b attempts remaining",
                          [M, F, length(A), Node, Retries]),
            timer:sleep(2000),
            call_with_patience_(Node, M, F, A, Retries - 1);
        Result ->
            Result
    end.


%% @ignore
%% copied from ensemble_util.erl
-spec build_cluster(non_neg_integer()) -> [node()].
build_cluster(Size) ->
    build_cluster(Size, []).
-spec build_cluster(non_neg_integer(), list()) -> [node()].
build_cluster(Size, Config) ->
    [Node1|_] = Nodes = rt:deploy_nodes(Size, Config),
    rt:join_cluster(Nodes),
    ensemble_util:wait_until_cluster(Nodes),
    ensemble_util:wait_for_membership(Node1),
    ensemble_util:wait_until_stable(Node1, Size),
    Nodes.


-spec get_services(node()) -> {[service()], [package()]}.
get_services(Node) ->
    {Running_, Available_} =
        call_with_patience(Node, data_platform_global_state, services, []),
    {Running, Available} =
        {lists:sort([SName || {_Type, SName, _Node} <- Running_]),
         lists:sort([SName || {SName, _Type, _Conf} <- Available_])},
    lager:debug("Services running: ~p, available: ~p", [Running, Available]),
    {Running, Available}.

-spec wait_services(node(), {[config_name()], [config_name()]}) -> ok.
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


-spec add_service(node(), config_name(), service_type(), service_config()) -> ok.
add_service(Node, ServiceName, ServiceType, Config) ->
    {Rnn0, Avl0} = get_services(Node),
    Res = call_with_patience(
            Node, data_platform_global_state, add_service_config,
            [ServiceName, ServiceType, Config, false]),
    ?assert(Res == ok orelse Res == {error, config_already_exists}),
    Res == {error, config_already_exists} andalso
        begin lager:warning("Adding a service ~p that already exists", [ServiceName]) end,
    Avl1 = lists:usort(Avl0 ++ [ServiceName]),
    ok = wait_services(Node, {Rnn0, Avl1}).

-spec remove_service(node(), config_name()) -> ok.
remove_service(Node, ServiceName) ->
    {Rnn0, Avl0} = get_services(Node),
    Res = call_with_patience(
            Node, data_platform_global_state, remove_service,
            [ServiceName]),
    ?assert(Res == ok orelse Res == {error, config_not_found}),
    Res == {error, config_not_found} andalso
        begin lager:warning("Removing a service ~p that does not exists", [ServiceName]) end,
    Avl1 = lists:usort(Avl0 -- [ServiceName]),
    ok = wait_services(Node, {Rnn0, Avl1}).


-spec start_seervice(node(), node(), config_name(), service_type()) -> ok.
start_seervice(Node, ServiceNode, ServiceName, Group) ->
    {Rnn0, Avl0} = get_services(Node),
    Res = call_with_patience(
           Node, data_platform_global_state, start_service,
           [Group, ServiceName, ServiceNode]),
    ?assert(Res == ok orelse Res == {error, already_running}),
    Res == {error, already_running} andalso
        begin lager:warning("Starting a service ~p that's already running", [ServiceName]) end,
    Rnn1 = lists:usort(Rnn0 ++ [ServiceName]),
    ok = wait_services(Node, {Rnn1, Avl0}).

-spec stop_service(node(), node(), config_name(), service_type()) -> ok.
stop_service(Node, ServiceNode, ServiceName, Group) ->
    {Rnn0, Avl0} = get_services(Node),
    Res = call_with_patience(
            Node, data_platform_global_state, stop_service,
            [Group, ServiceName, ServiceNode]),
    ?assert(Res == ok orelse Res == {error, service_not_found}),
    Res == {error, service_not_found} andalso
        begin lager:warning("Stopping a service ~p that's not running", [ServiceName]) end,
    Rnn1 = lists:usort(Rnn0 -- [ServiceName]),
    ok = wait_services(Node, {Rnn1, Avl0}).
