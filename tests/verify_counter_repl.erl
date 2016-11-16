%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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

%%% @copyright (C) 2013, Basho Technologies
%%% @doc
%%% riak_test for riak_dt counter convergence over repl
%%% @end

-module(verify_counter_repl).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"counter-bucket">>).
-define(KEY, <<"counter-key">>).

confirm() ->
    inets:start(),

    {ClusterA, ClusterB} = make_clusters(),

    %% Write the data to both sides of the cluster
    AIncrements = increment_cluster_counter(ClusterA),
    BIncrements = increment_cluster_counter(ClusterB),
    AExpected = lists:sum(AIncrements),
    BExpected = lists:sum(BIncrements),

    AValue0 = get_counter(hd(ClusterA)),
    BValue0 = get_counter(hd(ClusterB)),

    ?assertEqual(AExpected, AValue0),
    ?assertEqual(BExpected, BValue0),

    %% let the repl flow
    repl_power_activate(ClusterA, ClusterB),

    ExpectedValue = AExpected + BExpected,

    ?assertEqual(ok, rt:wait_until(fun() -> verify_counter(ExpectedValue, hd(ClusterA)) end)),
    ?assertEqual(ok, rt:wait_until(fun() -> verify_counter(ExpectedValue, hd(ClusterB)) end)),
    pass.

make_clusters() ->
    Conf = [{riak_repl, [{fullsync_on_connect, false},
                         {fullsync_interval, disabled}]},
           {riak_core, [{default_bucket_props, [{allow_mult, true}, {dvv_enabled, true}]}]}],
    Nodes = rt:deploy_nodes(6, Conf, [riak_kv, riak_repl]),
    {ClusterA, ClusterB} = lists:split(3, Nodes),
    A = make_cluster(ClusterA, "A"),
    B = make_cluster(ClusterB, "B"),
    {A, B}.

make_cluster(Nodes, Name) ->
    repl_util:make_cluster(Nodes),
    repl_util:name_cluster(hd(Nodes), Name),
    repl_util:wait_until_leader_converge(Nodes),
    Clients = [ rt:httpc(Node) || Node <- Nodes ],
    lists:zip(Clients, Nodes).

increment_cluster_counter(Cluster) ->
    [increment_counter(Client, rand_amt()) || {Client, _Node} <- Cluster].

increment_counter(Client, Amt) ->
    rhc:counter_incr(Client, ?BUCKET, ?KEY, Amt),
    Amt.

get_counter({Client, _Node}) ->
    {ok, Val} = rhc:counter_val(Client, ?BUCKET, ?KEY),
    Val.

verify_counter(ExpectedValue, Node) ->
    ExpectedValue == get_counter(Node).

rand_amt() ->
    crypto:rand_uniform(-100, 100).

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
