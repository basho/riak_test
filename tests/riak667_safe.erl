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
-module(riak667_safe).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).
-define(INDEX, <<"maps">>).
-define(TYPE, <<"maps">>).
-define(KEY, "cmeiklejohn").
-define(BUCKET, {?TYPE, <<"testbucket">>}).
-define(NAME_REGISTER_VALUE, "Christopher Meiklejohn").

-define(CONF, [
        {riak_core,
            [{ring_creation_size, 8}]
        }]).

confirm() ->
    rt:set_advanced_conf(all, ?CONF),

    %% Configure cluster.
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, "2.0.2"),
    Nodes = [Node|_] = rt:build_cluster([OldVsn]),

    %% Create PB connection.
    Pid = rt:pbc(Node),
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    %% Create bucket type for maps.
    rt:create_and_activate_bucket_type(Node, ?TYPE, [{datatype, map}]),

    %% Write some sample data.
    Map = riakc_map:update(
            {<<"name">>, register},
            fun(R) ->
                    riakc_register:set(<<"Original">>, R)
            end,
            riakc_map:new()),
    ok = riakc_pb_socket:update_type(
            Pid,
            ?BUCKET,
            ?KEY,
            riakc_map:to_op(Map)),

    %% Stop PB connection.
    riakc_pb_socket:stop(Pid),

    %% Upgrade all nodes.
    [upgrade(N, current) || N <- Nodes],

    %% Create PB connection.
    Pid2 = rt:pbc(Node),
    riakc_pb_socket:set_options(Pid2, [queue_if_disconnected]),

    %% Read value.
    {ok, O} = riakc_pb_socket:fetch_type(Pid2, ?BUCKET, ?KEY),

    %% Write some sample data.
    Map2 = riakc_map:update(
            {<<"name">>, register},
            fun(R) ->
                    riakc_register:set(<<"Updated">>, R)
                    end, O),
    ok = riakc_pb_socket:update_type(
            Pid2,
            ?BUCKET,
            ?KEY,
            riakc_map:to_op(Map2)),

    %% Stop PB connection.
    riakc_pb_socket:stop(Pid2),

    %% Clean cluster.
    rt:clean_cluster(Nodes),

    pass.

upgrade(Node, NewVsn) ->
    lager:info("Upgrading ~p to ~p", [Node, NewVsn]),
    rt:upgrade(Node, NewVsn),
    rt:wait_for_service(Node, riak_kv),
    lager:info("Ensuring keys still exist"),
    rt:systest_read(Node, 100, 1),
    ok.
