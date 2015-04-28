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
-module(yz_rt).

-type index_name() :: binary().
-type bucket() :: binary() | {riak_core_bucket_type:bucket_type(), binary()}.

-export([expire_trees/1,
         rolling_upgrade/2,
         wait_for_aae/1,
         write_data/4]).

%% @doc Write `Keys' via the PB inteface to a `Bucket' and have them
%%      searchable in an `Index'.
-spec write_data(pid(), index_name(), bucket(), [binary()]) -> ok.
write_data(Pid, Index, Bucket, Keys) ->
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    %% Create a search index and associate with a bucket
    ok = riakc_pb_socket:create_search_index(Pid, Index),
    ok = riakc_pb_socket:set_search_index(Pid, Bucket, Index),
    timer:sleep(1000),

    %% Write keys
    lager:info("Writing ~p keys", [length(Keys)]),
    [ok = rt:pbc_write(Pid, Bucket, Key, Key, "text/plain") || Key <- Keys],
    ok.

%% @doc Peform a rolling upgrade of the `Cluster' to a different `Version' based
%%      on current | previous | legacy.
-spec rolling_upgrade([node()], current | previous | legacy) -> ok.
rolling_upgrade(Cluster, Vsn) ->
    lager:info("Perform rolling upgrade on cluster ~p", [Cluster]),
    SolrPorts = lists:seq(11000, 11000 + length(Cluster) - 1),
    Cluster2 = lists:zip(SolrPorts, Cluster),
    [begin
         Cfg = [{riak_kv, [{anti_entropy, {on, [debug]}},
                           {anti_entropy_concurrency, 12},
                           {anti_entropy_build_limit, {6,500}}
                          ]},
                {yokozuna, [{anti_entropy, {on, [debug]}},
                            {anti_entropy_concurrency, 12},
                            {anti_entropy_build_limit, {6,500}},
                            {anti_entropy_tick, 1000},
                            {enabled, true},
                            {solr_port, SolrPort}]}],
         rt:upgrade(Node, Vsn, Cfg),
         rt:wait_for_service(Node, riak_kv),
         rt:wait_for_service(Node, yokozuna)
     end || {SolrPort, Node} <- Cluster2],
    ok.

%% @doc Use AAE status to verify that exchange has occurred for all
%%      partitions since the time this function was invoked.
-spec wait_for_aae([node()]) -> ok.
wait_for_aae(Cluster) ->
    lager:info("Wait for AAE to migrate/repair indexes"),
    wait_for_all_trees(Cluster),
    wait_for_full_exchange_round(Cluster, erlang:now()),
    ok.

%% @doc Wait for all AAE trees to be built.
-spec wait_for_all_trees([node()]) -> ok.
wait_for_all_trees(Cluster) ->
    F = fun(Node) ->
                lager:info("Check if all trees built for node ~p", [Node]),
                Info = rpc:call(Node, yz_kv, compute_tree_info, []),
                NotBuilt = [X || {_,undefined}=X <- Info],
                NotBuilt == []
        end,
    rt:wait_until(Cluster, F),
    ok.

%% @doc Wait for a full exchange round since `Timestamp'.  This means
%%      that all `{Idx,N}' for all partitions must have exchanged after
%%      `Timestamp'.
-spec wait_for_full_exchange_round([node()], os:now()) -> ok.
wait_for_full_exchange_round(Cluster, Timestamp) ->
    lager:info("wait for full AAE exchange round on cluster ~p", [Cluster]),
    MoreRecent =
        fun({_Idx, _, undefined, _RepairStats}) ->
                false;
           ({_Idx, _, AllExchangedTime, _RepairStats}) ->
                AllExchangedTime > Timestamp
        end,
    AllExchanged =
        fun(Node) ->
                Exchanges = rpc:call(Node, yz_kv, compute_exchange_info, []),
                {_Recent, WaitingFor1} = lists:partition(MoreRecent, Exchanges),
                WaitingFor2 = [element(1,X) || X <- WaitingFor1],
                lager:info("Still waiting for AAE of ~p ~p", [Node, WaitingFor2]),
                [] == WaitingFor2
        end,
    rt:wait_until(Cluster, AllExchanged),
    ok.

%% @doc Expire YZ trees
-spec expire_trees([node()]) -> ok.
expire_trees(Cluster) ->
    lager:info("Expire all trees and verify exchange still happens"),

    lager:info("Expire all trees"),
    _ = [ok = rpc:call(Node, yz_entropy_mgr, expire_trees, [])
         || Node <- Cluster],

    %% The expire is async so just give it a moment
    timer:sleep(100),
    ok.
