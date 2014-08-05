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

-module(ensemble_ring_changes).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(NVAL, 3).

config() ->
    [{riak_core, [{default_bucket_props, [{n_val, 5}]},
                  {vnode_management_timer, 1000},
                  {ring_creation_size, 16},
                  {enable_consensus, true},
                  {target_n_val, 8}]}].

confirm() ->
    Config = config(),
    {ok, Joined, NotJoined} = build_initial_cluster(Config),
    Node = hd(Joined),
    Bucket = {<<"strong">>, <<"test">>},
    Key = <<"test-key">>,
    Val = <<"test-val">>,
    create_strong_bucket_type(Node, ?NVAL),
    {ok, PL} = get_preflist(Node, Bucket, Key, ?NVAL),
    ?assertEqual(?NVAL, length(PL)),
    lager:info("PREFERENCE LIST: ~n  ~p", [PL]),

    {{Idx0, _Node0}, primary} = hd(PL),
    Ensemble = {kv, Idx0, 3},
    lager:info("Sleeping for 60 seconds to allow consensus groups to come up for
        strong bucket type"),
    timer:sleep(60000),
    Leader = rpc:call(Node, riak_ensemble_manager, get_leader, [Ensemble]),
    lager:info("Leader = ~p~n", [Leader]),
    PBC = rt:pbc(Node),
    %% maps to a riak_ensemble put_once since there is no vclock
    {ok, Obj} = initial_write(PBC, Bucket, Key, Val),
    expand_cluster(Joined, NotJoined),
    NewVal = <<"test-val2">>,
    ok = update(PBC, Obj, NewVal),
    Obj2 = rt:pbc_read(PBC, Bucket, Key),
    ?assertEqual(NewVal, riakc_obj:get_value(Obj2)),
    pass.

update(PBC, Obj0, NewVal) ->
    Obj = riakc_obj:update_value(Obj0, NewVal),
    riakc_pb_socket:put(PBC, Obj).

initial_write(PBC, Bucket, Key, Val) ->
    lager:info("Writing a consistent key"),
    ok = rt:pbc_write(PBC, Bucket, Key, Val),
    lager:info("Read key to verify it exists"),
    Obj = rt:pbc_read(PBC, Bucket, Key),
    ?assertEqual(Val, riakc_obj:get_value(Obj)),
    {ok, Obj}.

get_preflist(Node, Bucket, Key, NVal) ->
    DocIdx = rpc:call(Node, riak_core_util, chash_std_keyfun, [{Bucket, Key}]),
    PL = rpc:call(Node, riak_core_apl, get_primary_apl, [DocIdx, NVal, riak_kv]),
    {ok, PL}.

create_strong_bucket_type(Node, NVal) ->
    lager:info("Creating/activating 'strong' bucket type"),
    rt:create_and_activate_bucket_type(Node, <<"strong">>,
                                       [{consistent, true}, {n_val, NVal}]),
    ensemble_util:wait_until_stable(Node, NVal).

expand_cluster(OldNodes, NewNodes) ->
    PNode = hd(OldNodes),
    [rt:join(Node, PNode) || Node <- NewNodes],
    ensemble_util:wait_until_cluster(OldNodes ++ NewNodes),
    ensemble_util:wait_for_membership(PNode),
    ensemble_util:wait_until_stable(PNode, ?NVAL).


build_initial_cluster(Config) ->
    TotalNodes = 7,
    InitialNodes = 3,
    Nodes = rt:deploy_nodes(TotalNodes, Config),
    Node = hd(Nodes),
    {ToJoin, NotToJoin} = lists:split(InitialNodes, Nodes),
    rt:join_cluster(ToJoin),
    ensemble_util:wait_until_cluster(ToJoin),
    ensemble_util:wait_for_membership(Node),
    ensemble_util:wait_until_stable(Node, InitialNodes),
    vnode_util:load(Nodes),
    ensemble_util:wait_until_stable(Node, InitialNodes),
    {ok, ToJoin, NotToJoin}.
