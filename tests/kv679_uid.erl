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
%%% @doc
%%% riak_test for kv679-ish bug were vnodes on same node get same ID
%%%
%%% @end

-module(kv679_uid).
-behavior(riak_test).
-compile([export_all, nowarn_export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"kv679">>).
-define(KEY, <<"test">>).

confirm() ->
    [Node] = rt:deploy_nodes(1),
    PL = kv679_tombstone:get_preflist(Node),

    %% Get vnodeids for each primary
    PartitionIdMap = get_vnodeids(PL, Node),

    lager:info("ids = ~p", [PartitionIdMap]),
    %% assert each is unique
    {_Idxes, VnodeIds} = lists:unzip(PartitionIdMap),
    ?assertEqual(3,length(PartitionIdMap)),
    ?assertEqual(3, length(lists:usort(VnodeIds))),

    pass.

get_vnodeids(PLAnn, Node) ->
    PL = [{Idx, N} || {{Idx, N}, Type} <- PLAnn,
                         Type == primary],
    Statuses = rpc:call(Node, riak_kv_vnode, vnode_status, [PL]),
    [{Idx, proplists:get_value(vnodeid, Status)} || {Idx, Status} <- Statuses].
