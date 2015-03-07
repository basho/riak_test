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

-module(verify_fast_path).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(NVAL, 2).
-define(BUCKET_TYPE, <<"immutable">>).
-define(BUCKET, {?BUCKET_TYPE, <<"bucket">>}).

%%
%% TODO
%%      w, dw, pw values
%%      recv_timeout
%%      partitions and sibling prevention
%%

confirm() ->
    %%
    %% Set up a cluster of nodes
    %%
    NumNodes = 4,
    Nodes = rt:deploy_nodes(NumNodes, config(?DEFAULT_RING_SIZE, ?NVAL)),
    rt:join_cluster(Nodes),
    lager:info("Set up ~p node cluster: ~p", [NumNodes, Nodes]),
    %%
    %% Select a random node, and use it to create an immutable bucket
    %%
    Node = lists:nth(random:uniform(length((Nodes))), Nodes),
    rt:create_and_activate_bucket_type(Node, ?BUCKET_TYPE, [{fast_path, true}]),
    rt:wait_until_bucket_type_status(?BUCKET_TYPE, active, Nodes),
    lager:info("Created ~p bucket type on ~p", [?BUCKET_TYPE, Node]),
    %%
    %%
    %%
    pass = confirm_put(Node),
    pass = confirm_pw(Nodes),
    %pass = confirm_dw(Node, Nodes),
    %pass = confirm_w(Node, Nodes),
    pass = confirm_rww(Nodes),
    pass.

%%
%% private
%%


confirm_put(Node) ->
    ok = verify_put(Node, ?BUCKET, <<"confirm_put_key">>, <<"confirm_put_value">>),
    lager:info("confirm_put...ok"),
    pass.


confirm_pw(Nodes) ->
    %%
    %% split the cluster into 2 paritions [dev1, dev2, dev3], [dev4]
    %%
    P1 = lists:sublist(Nodes, 3),
    P2 = lists:sublist(Nodes, 4, 1),
    PartitonInfo = rt:partition(P1, P2),
    [Node1 | _Rest1] = P1,
    verify_put(Node1, ?BUCKET, <<"confirm_w_key">>, <<"confirm_w_value">>),
    [Node2 | _Rest2] = P2,
    verify_put_timeout(Node2, ?BUCKET, <<"confirm_pw_key">>, <<"confirm_pw_value">>, [{pw, all}, {timeout, 1000}]),
    rt:heal(PartitonInfo),
    lager:info("confirm_pw...ok"),
    pass.

confirm_rww(Nodes) ->
    %%
    %% split the cluster into 2 paritions
    %%
    P1 = lists:sublist(Nodes, 2),
    P2 = lists:sublist(Nodes, 3, 2),
    PartitonInfo = rt:partition(P1, P2),
    %%
    %% put different values into each partiton
    %%
    [Node1 | _Rest1] = P1,
    verify_put(Node1, ?BUCKET, <<"confirm_rww_key">>, <<"confirm_rww_value1">>),
    [Node2 | _Rest2] = P2,
    verify_put(Node2, ?BUCKET, <<"confirm_rww_key">>, <<"confirm_rww_value2">>),
    %%
    %% After healing, both should agree on an arbitrary value, and that one merge has taken place
    %%
    rt:heal(PartitonInfo),
    rt:wait_until(fun() ->
        V1 = get(Node1, ?BUCKET, <<"confirm_rww_key">>),
        V2 = get(Node2, ?BUCKET, <<"confirm_rww_key">>),
        V1 =:= V2
    end),
    lager:info("confirm_rww...ok"),
    pass.


verify_put(Node, Bucket, Key, Value) ->
    Client = rt:pbc(Node),
    _Ret = riakc_pb_socket:put(
        Client, riakc_obj:new(
            Bucket, Key, Value
        )
    ),
    {ok, Val} = riakc_pb_socket:get(Client, Bucket, Key),
    ?assertEqual(Value, riakc_obj:get_value(Val)),
    ok.


verify_put_timeout(Node, Bucket, Key, Value, Options) ->
    Client = rt:pbc(Node),
    ?assertEqual(
        {error, <<"timeout">>},
        riakc_pb_socket:put(
            Client, riakc_obj:new(
                Bucket, Key, Value
            ), Options
        )
    ),
    ok.


get(Node, Bucket, Key) ->
    Client = rt:pbc(Node),
    {ok, Val} = riakc_pb_socket:get(Client, Bucket, Key),
    riakc_obj:get_value(Val).

config(RingSize, NVal) ->
    [
        {riak_core, [
            {default_bucket_props, [{n_val, NVal}]},
            {vnode_management_timer, 1000},
            {ring_creation_size, RingSize}]
        },
        {riak_kv, [
            {anti_entropy_build_limit, {100, 1000}},
            {anti_entropy_concurrency, 100},
            {anti_entropy_tick, 100},
            {anti_entropy, {on, []}},
            {anti_entropy_timeout, 5000},
            {storage_backend, riak_kv_memory_backend}]
        }
    ].

