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
-module(riak667_mixed).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).
-define(TYPE, <<"maps">>).
-define(KEY, <<"cmeiklejohn">>).
-define(KEY2, <<"cmeik">>).
-define(BUCKET, {?TYPE, <<"testbucket">>}).

-define(CONF, [
               {riak_core,
                [{ring_creation_size, 8}]
               },
               {riak_kv,
                [{mdc_crdt_epoch, 1}]}
              ]).

confirm() ->
    rt:set_advanced_conf(all, ?CONF),

    %% Configure cluster.
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, "2.0.2"),
    Nodes = [Node1, Node2] = rt:build_cluster([OldVsn, OldVsn]),

    CurrentVer = rt:get_version(),

    lager:info("mdc_crdt_epoch? ~p", [rpc:multicall(Nodes, application, get_env, [riak_kv, mdc_crdt_epoch])]),

    %% Create PB connection.
    Pid = rt:pbc(Node1),
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    %% Create bucket type for maps.
    rt:create_and_activate_bucket_type(Node1, ?TYPE, [{datatype, map}]),

    %% Write some sample data.
    Map = riakc_map:update(
            {<<"names">>, set},
            fun(R) ->
                    riakc_set:add_element(<<"Original">>, R)
            end, riakc_map:new()),
    Map2 = riakc_map:update({<<"profile">>, map},
                            fun(M) ->
                                    riakc_map:update(
                                      {<<"name">>, register},
                                      fun(R) ->
                                              riakc_register:set(<<"Bob">>, R)
                                      end, M)
                            end, Map),

    ok = riakc_pb_socket:update_type(
            Pid,
            ?BUCKET,
            ?KEY,
            riakc_map:to_op(Map2)),

    %% Upgrade one node and wait until the ring has converged so the correct
    %% capabilities will be negotiated
    upgrade(Node2, "2.0.4"),
    rt:wait_until_ring_converged([Node1, Node2]),

    lager:notice("running mixed 2.0.2 and 2.0.4"),

    %% Create PB connection.
    Pid2 = rt:pbc(Node2),
    riakc_pb_socket:set_options(Pid2, [queue_if_disconnected]),

    %% Read value.
    ?assertMatch({error, <<"Error processing incoming message: error:{badrecord,dict}", _/binary>>},
                 riakc_pb_socket:fetch_type(Pid2, ?BUCKET, ?KEY)),

    lager:notice("Can't read a 2.0.2 map from 2.0.4 node"),

    %% Write some 2.0.4 data.
    Oh4Map = riakc_map:update(
            {<<"names">>, set},
            fun(R) ->
                    riakc_set:add_element(<<"Original">>, R)
            end, riakc_map:new()),
    Oh4Map2 = riakc_map:update({<<"profile">>, map},
                            fun(M) ->
                                    riakc_map:update(
                                      {"name", register},
                                      fun(R) ->
                                              riakc_register:set(<<"Bob">>, R)
                                      end, M)
                            end, Oh4Map),

    ok = riakc_pb_socket:update_type(
            Pid2,
            ?BUCKET,
            ?KEY2,
            riakc_map:to_op(Oh4Map2)),

    lager:notice("Created a 2.0.4 map"),

    %% and read 2.0.4 data?? Nope, dict is not an orddict
    ?assertMatch({error,<<"Error processing incoming message: error:function_clause:[{orddict,fold", _/binary>>},
                 riakc_pb_socket:fetch_type(Pid, ?BUCKET, ?KEY2)),

    lager:notice("Can't read 2.0.4 map from 2.0.2 node"),

    %% upgrade 2.0.4 to 2.0.5
    riakc_pb_socket:stop(Pid2),
    upgrade(Node2, current),

    lager:notice("running mixed 2.0.2 and ~s", [CurrentVer]),

    %% Create PB connection.
    Pid3 = rt:pbc(Node2),
    riakc_pb_socket:set_options(Pid3, [queue_if_disconnected]),

    %% read both maps
    {ok, K1O} = riakc_pb_socket:fetch_type(Pid3, ?BUCKET, ?KEY),
    {ok, K2O} = riakc_pb_socket:fetch_type(Pid3, ?BUCKET, ?KEY2),

    lager:notice("2.0.2 map ~p", [K1O]),
    lager:notice("2.0.4 map ~p", [K2O]),

    %% update 2.0.2 map on new node ?KEY Pid3
    K1OU = riakc_map:update({<<"profile">>, map},
                            fun(M) ->
                                    riakc_map:update(
                                      {<<"name">>, register},
                                      fun(R) ->
                                              riakc_register:set(<<"Rita">>, R)
                                      end, M)
                            end, K1O),

    ok = riakc_pb_socket:update_type(Pid3, ?BUCKET, ?KEY, riakc_map:to_op(K1OU)),
    lager:notice("Updated 2.0.2 map on ~s", [CurrentVer]),

    %% read 2.0.2 map from 2.0.2 node ?KEY Pid
    {ok, K1OR} = riakc_pb_socket:fetch_type(Pid, ?BUCKET, ?KEY),
    lager:notice("Read 2.0.2 map from 2.0.2 node: ~p", [K1OR]),

    ?assertEqual(<<"Rita">>, orddict:fetch({<<"name">>, register},
                                           riakc_map:fetch({<<"profile">>, map}, K1OR))),

    %% update 2.0.2 map on old node ?KEY Pid
    K1O2 = riakc_map:update({<<"profile">>, map},
                            fun(M) ->
                                    riakc_map:update(
                                      {<<"name">>, register},
                                      fun(R) ->
                                              riakc_register:set(<<"Sue">>, R)
                                      end, M)
                            end, K1OR),

    ok = riakc_pb_socket:update_type(Pid, ?BUCKET, ?KEY, riakc_map:to_op(K1O2)),
    lager:notice("Updated 2.0.2 map on 2.0.2 node"),

    %% read it from 2.0.5 node ?KEY Pid3
    {ok, K1OC} = riakc_pb_socket:fetch_type(Pid3, ?BUCKET, ?KEY),
    lager:notice("Read 2.0.2 map from ~s node: ~p", [CurrentVer, K1OC]),

    ?assertEqual(<<"Sue">>, orddict:fetch({<<"name">>, register},
                                          riakc_map:fetch({<<"profile">>, map}, K1OC))),

    %% update 2.0.4 map node ?KEY2 Pid3
    K2OU = riakc_map:update({<<"people">>, set},
                            fun(S) ->
                                    riakc_set:add_element(<<"Joan">>, S)
                            end, riakc_map:new()),

    ok = riakc_pb_socket:update_type(Pid3, ?BUCKET, ?KEY2, riakc_map:to_op(K2OU)),
    lager:notice("Updated 2.0.4 map on ~s node", [CurrentVer]),
    %% upgrade 2.0.2 node

    riakc_pb_socket:stop(Pid),
    upgrade(Node1, current),
    lager:notice("Upgraded 2.0.2 node to ~s", [CurrentVer]),

    %% read and write maps
    Pid4 = rt:pbc(Node1),

    {ok, K1N1} = riakc_pb_socket:fetch_type(Pid4, ?BUCKET, ?KEY),
    {ok, K1N2} = riakc_pb_socket:fetch_type(Pid3, ?BUCKET, ?KEY),
    ?assertEqual(K1N1, K1N2),

    {ok, K2N1} = riakc_pb_socket:fetch_type(Pid4, ?BUCKET, ?KEY2),
    {ok, K2N2} = riakc_pb_socket:fetch_type(Pid3, ?BUCKET, ?KEY2),
    ?assertEqual(K2N1, K2N2),
    lager:notice("Maps fetched from both nodes are same K1:~p K2:~p", [K1N1, K2N1]),

    K1M = riakc_map:update({<<"people">>, set},
                           fun(S) -> riakc_set:add_element(<<"Roger">>, S) end,
                           K1N1),
    ok = riakc_pb_socket:update_type(Pid3, ?BUCKET, ?KEY, riakc_map:to_op(K1M)),

    K2M = riakc_map:update({<<"people">>, set},
                           fun(S) -> riakc_set:add_element(<<"Don">>, S) end,
                           K2N1),
    ok = riakc_pb_socket:update_type(Pid4, ?BUCKET, ?KEY2, riakc_map:to_op(K2M)),
    %% (how???) check format is still v1 (maybe get raw kv object and inspect contents using riak_kv_crdt??

    {ok, Robj1} = riakc_pb_socket:get(Pid3, ?BUCKET, ?KEY),
    ?assert(map_contents_are_lists(Robj1)),

    {ok, Robj2} = riakc_pb_socket:get(Pid4, ?BUCKET, ?KEY2),

    lager:info("mdc_crdt_epoch? ~p", [rpc:multicall(Nodes, application, get_env, [riak_kv, mdc_crdt_epoch])]),

    ?assert(map_contents_are_lists(Robj2)),
    %% unset env var
    rpc:multicall(Nodes, application, set_env, [riak_kv, mdc_crdt_epoch, false]),

    lager:info("mdc_crdt_epoch? ~p", [rpc:multicall(Nodes, application, get_env, [riak_kv, mdc_crdt_epoch])]),

    %% read and write maps
    {ok, Up1N1} = riakc_pb_socket:fetch_type(Pid4, ?BUCKET, ?KEY),
    {ok, Up1N2} = riakc_pb_socket:fetch_type(Pid3, ?BUCKET, ?KEY),
    ?assertEqual(Up1N1, Up1N2),

    {ok, Up2N1} = riakc_pb_socket:fetch_type(Pid4, ?BUCKET, ?KEY2),
    {ok, Up2N2} = riakc_pb_socket:fetch_type(Pid3, ?BUCKET, ?KEY2),
    ?assertEqual(Up2N1, Up2N2),
    lager:notice("Maps fetched from both nodes are same K1:~p K2:~p", [Up1N1, Up2N1]),

    Up1M = riakc_map:update({<<"people">>, set},
                           fun(S) -> riakc_set:add_element(<<"Betty">>, S) end,
                           Up1N1),
    ok = riakc_pb_socket:update_type(Pid3, ?BUCKET, ?KEY, riakc_map:to_op(Up1M)),

    Up2M = riakc_map:update({<<"people">>, set},
                           fun(S) -> riakc_set:add_element(<<"Burt">>, S) end,
                           Up2N1),
    ok = riakc_pb_socket:update_type(Pid4, ?BUCKET, ?KEY2, riakc_map:to_op(Up2M)),

    %% (how??? see above?) check ondisk format is now v2
    {ok, UpObj1} = riakc_pb_socket:get(Pid3, ?BUCKET, ?KEY),
    ?assert(map_contents_are_dicts(UpObj1)),

    {ok, UpObj2} = riakc_pb_socket:get(Pid4, ?BUCKET, ?KEY2),
    ?assert(map_contents_are_dicts(UpObj2)),

    %% Stop PB connection.
    riakc_pb_socket:stop(Pid3),
    riakc_pb_socket:stop(Pid4),

    pass.

upgrade(Node, NewVsn) ->
    lager:notice("Upgrading ~p to ~p", [Node, NewVsn]),
    rt:upgrade(Node, NewVsn),
    rt:wait_for_service(Node, riak_kv),
    ok.

map_contents_are_lists(RObj) ->
    [{_MD, V}] = riakc_obj:get_contents(RObj),
    {riak_dt_map, {_Clock, Entries, Deferred}} = map_from_binary(V),
    lager:info("Top-level map: ~p || ~p", [Entries, Deferred]),
    is_list(Entries) andalso is_list(Deferred) andalso nested_are_lists(Entries).

nested_are_lists(Entries) ->
    %% This is ugly because it reaches into the guts of the data
    %% structure's internal format.
    lists:all(fun({{_, riak_dt_orswot}, {CRDTs, Tombstone}}) ->
                         lists:all(fun({_Dot, Set}) -> set_is_list(Set) end, CRDTs)
                             andalso set_is_list(Tombstone);
                 ({{_, riak_dt_map}, {CRDTs, Tombstone}}) ->
                         lists:all(fun({_Dot, Map}) -> map_is_list(Map) end, CRDTs)
                             andalso map_is_list(Tombstone);
                 (_) ->
                      true
              end, Entries).

set_is_list({_Clock, Entries, Deferred}) ->
    is_list(Entries) andalso is_list(Deferred).

map_is_list({_Clock, Entries, Deferred}) ->
    is_list(Deferred) andalso nested_are_lists(Entries).

map_contents_are_dicts(RObj) ->
    [{_MD, V}] = riakc_obj:get_contents(RObj),
    {riak_dt_map, {_Clock, Entries, Deferred}} = map_from_binary(V),
    lager:info("Top-level map: ~p || ~p", [Entries, Deferred]),
    is_dict(Entries) andalso is_dict(Deferred) andalso nested_are_dicts(Entries).

is_dict(V) ->
    is_tuple(V) andalso dict == element(1, V).

nested_are_dicts(Entries) ->
    %% This is ugly because it reaches into the guts of the data
    %% structure's internal format.
    lists:all(fun({{_, riak_dt_orswot}, {CRDTs, Tombstone}}) ->
                      is_dict(CRDTs) andalso
                          set_is_dict(Tombstone) andalso
                          dict:fold(fun(_K, Set, Acc) ->
                                            set_is_dict(Set) andalso Acc
                                    end, true, CRDTs);
                 ({{_, riak_dt_map}, {CRDTs, Tombstone}}) ->
                      is_dict(CRDTs) andalso map_is_dict(Tombstone) andalso
                          dict:fold(fun(_K, Map, Acc) ->
                                            map_is_dict(Map) andalso Acc
                                    end, true, CRDTs);
                 (_) ->
                      true
              end, dict:to_list(Entries)).

set_is_dict({_Clock, Entries, Deferred}) ->
    is_dict(Entries) andalso is_dict(Deferred).

map_is_dict({_Clock, Entries, Deferred}) ->
    is_dict(Deferred) andalso nested_are_dicts(Entries).

%% Somewhat copy-pasta from riak_kv_crdt
%% NB:?TAG is 69 in riak_kv_crdt, version is 2
%%    ?TAG is 77 in riak_dt_types.hrl, version is 1 or 2
map_from_binary(<<69:8, 2:8, TypeLen:32/integer, Type:TypeLen/binary, 77:8, MapVer:8,
                  CRDTBin/binary>>) ->
    lager:notice("Deserialized Map: ~s v~p", [Type, MapVer]),
    Mod = binary_to_atom(Type, latin1),
    {Mod, riak_dt:from_binary(CRDTBin)}.
