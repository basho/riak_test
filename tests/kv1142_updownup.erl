%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
%%% @copyright (C) 2014, Basho Technologies
%%%
%%% @doc riak_test for kv1142 where a vnode_status file is a reult of
%%% an upgrade to 2.1, then downgrade, then up again
%%%
%%% @end

-module(kv1142_updownup).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"kv1142">>).
-define(KEY, <<"test">>).

confirm() ->
    %% Configure cluster.
    TestMetaData = riak_test_runner:metadata(),
    NewVsn = proplists:get_value(upgrade_version, TestMetaData, "2.1.1"),
    Nodes = rt:build_cluster([NewVsn]),

    %% Upgrade all nodes.
    [downgrade(N, "2.0.6") || N <- Nodes],

    [upgrade(N, "2.1.1") || N <- Nodes],

    pass.

get_vnodeids(PLAnn, Node) ->
    PL = [{Idx, N} || {{Idx, N}, Type} <- PLAnn,
                         Type == primary],
    Statuses = rpc:call(Node, riak_kv_vnode, vnode_status, [PL]),
    [{Idx, proplists:get_value(vnodeid, Status)} || {Idx, Status} <- Statuses].

upgrade(Node, NewVsn) ->
    lager:info("Upgrading ~p to ~p", [Node, NewVsn]),
    rt:upgrade(Node, NewVsn),
    rt:wait_for_service(Node, riak_kv),
    ok.

downgrade(Node, NewVsn) ->
    lager:info("Downgrade ~p to ~p", [Node, NewVsn]),
    %% lol!
    rt:upgrade(Node, NewVsn),
    rt:wait_for_service(Node, riak_kv),
    ok.
