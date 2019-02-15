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

%%% with the addition of `node_confirms` as a default bucket property
%%% we must ensure that custom buckets and typed buckets all inherit
%%% the default for `node_onfirms` when upgraded. This test verifies
%%% that. https://github.com/nhs-riak/riak_kv/issues/9

-module(node_confirms_upgrade).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TYPE, <<"type1">>).
-define(KEY, <<"k">>).
-define(BUCKET, <<"b">>).
-define(TYPED_BUCKET, {?TYPE, ?BUCKET}).

-define(CONF, [
        {riak_core,
            [{ring_creation_size, 8}]
        }]).

confirm() ->
    rt:set_advanced_conf(all, ?CONF),

    %% Configure cluster.
    TestMetaData = riak_test_runner:metadata(),
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    Nodes = [Node|_] = rt:build_cluster([OldVsn]),

    %% Create PB connection.
    Pid = rt:pbc(Node),
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    %% Create a typed bucket (with no custom props)
    rt:create_and_activate_bucket_type(Node, ?TYPE, []),
    %% Create a custom bucket (change allow_mult to true!)
    rt:pbc_set_bucket_prop(Pid, ?BUCKET, [{allow_mult, true}, {custom_prop, x}]),

    lager:info("Write values on `previous'"),
    %% Write some sample data.
    TypedObject = riakc_obj:new(?TYPED_BUCKET, ?KEY, <<"v1">>),
    UntypedObject = riakc_obj:new(?BUCKET, ?KEY, <<"v1">>),
    ok = riakc_pb_socket:put(Pid, TypedObject),
    ok = riakc_pb_socket:put(Pid, UntypedObject),

    %% Stop PB connection.
    riakc_pb_socket:stop(Pid),

    lager:info("Upgrade to current"),
    %% Upgrade all nodes.
    [upgrade(N, current) || N <- Nodes],

    %% Create PB connection.
    Pid2 = rt:pbc(Node),
    riakc_pb_socket:set_options(Pid2, [queue_if_disconnected]),

    {ok, UTO2} = riakc_pb_socket:get(Pid2, ?BUCKET, ?KEY),
    UTO3 = riakc_obj:update_value(UTO2, <<"v2">>),
    lager:info("Update custom bucket object on upgraded cluster"),
    ok = riakc_pb_socket:put(Pid2, UTO3),

    {ok, TO2} = riakc_pb_socket:get(Pid2, ?TYPED_BUCKET, ?KEY),
    TO3 = riakc_obj:update_value(TO2, <<"v2">>),
    lager:info("Update typed bucket object on upgraded cluster"),
    riakc_pb_socket:put(Pid2, TO3),
    ok = riakc_pb_socket:put(Pid2, TO3),

    %% Stop PB connection.
    riakc_pb_socket:stop(Pid2),

    pass.

upgrade(Node, NewVsn) ->
    lager:info("Upgrading ~p to ~p", [Node, NewVsn]),
    rt:upgrade(Node, NewVsn),
    rt:wait_for_service(Node, riak_kv),
    ok.
