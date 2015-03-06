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

-module(verify_immutable_put).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(NVAL, 3).
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
    NumNodes = 1,
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
    pass = confirm_w(Node, Nodes),
    pass = confirm_dw(Node, Nodes),
    pass = confirm_pw(Node, Nodes),
    pass = confirm_rww(Node, Nodes),
    pass.

%%
%% private
%%


confirm_put(Node) ->
    %%
    %% Do a put through a protobuf client connected to the selected node
    %%
    Client = rt:pbc(Node),
    _Ret = riakc_pb_socket:put(
        Client, riakc_obj:new(
            ?BUCKET, <<"key">>, <<"value">>
        )
    ),
    %%
    %% verify the result
    %%
    {ok, Val} = riakc_pb_socket:get(Client, ?BUCKET, <<"key">>),
    ?assertEqual(<<"value">>, riakc_obj:get_value(Val)),
    lager:info("confirm_put...ok"),
    pass.


confirm_w(_Node, _Nodes) -> unimplemented.
confirm_dw(_Node, _Nodes) -> unimplemented.
confirm_pw(_Node, _Nodes) -> unimplemented.
confirm_rww(_Node, _Nodes) -> unimplemented.

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

