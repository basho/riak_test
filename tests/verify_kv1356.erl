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

%%% @doc This test was written after
%%% https://github.com/basho/riak_kv/issues/1356 was opened. The user
%%% reported, under certain config, that riak simply "forgets"
%%% data. This is the bare minimum for a database. This test verifies
%%% that if we write a key/value we can read it. If we stop and start
%%% the node we can read it. If we kill and start the node we can read
%%% it.
%%% @end


-module(verify_kv1356).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"b">>).
-define(KEY, <<"k">>).
-define(VALUE, <<"v">>).

-define(HARNESS, (rt_config:get(rt_harness))).

-define(ELEVELDB_CONF(Path),
        [{eleveldb, [{data_root, "leveldb"},
                        {tiered_slow_level, 4},
                        {tiered_fast_prefix, Path ++ "/fast"},
                        {tiered_slow_prefix, Path ++ "/slow"}
                       ]}]).

confirm() ->
    Conf = [{riak_kv, [{handoff_concurrency, 100},
                       {storage_backend, riak_kv_eleveldb_backend}]},
            {riak_core, [ {ring_creation_size, 8},
                          {vnode_management_timer, 1000}]}],
    lager:info("Deploy a node"),
    [NodeInit] = rt:deploy_nodes(1, Conf),
    Path = data_path(NodeInit),
    rt:clean_cluster([NodeInit]),
    ok = filelib:ensure_dir(Path ++ "/fast/leveldb/"),
    ok = filelib:ensure_dir(Path ++ "/slow/leveldb/"),

    [Node] = rt:deploy_nodes(1, ?ELEVELDB_CONF(Path) ++ Conf),

    Client = rt:pbc(Node, [{auto_reconnect, true}, {queue_if_disconnected, true}]),
    lager:info("write value"),
    ok = rt:pbc_write(Client, ?BUCKET, ?KEY, ?VALUE),
    lager:info("read it back"),
    assertObjectValueEqual(?VALUE, rt:pbc_read(Client, ?BUCKET, ?KEY)),
    lager:info("Stop the node"),
    rt:stop_and_wait(Node),
    lager:info("Start the node"),
    rt:start_and_wait(Node),
    lager:info("read the value"),
    assertObjectValueEqual(?VALUE, rt:pbc_read(Client, ?BUCKET, ?KEY)),
    pass.

assertObjectValueEqual(Val, Obj) ->
    ?assertEqual(Val, riakc_obj:get_value(Obj)).

data_path(Node) ->
    ?HARNESS:node_path(Node) ++ "tiered_path".
