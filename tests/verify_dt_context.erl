%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
%%% riak_test for riak_dt CRDT context operations
%%% @end

-module(verify_dt_context).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(STYPE, <<"sets">>).
-define(MTYPE, <<"maps">>).
-define(TYPES, [{?STYPE, set},
                {?MTYPE, map}]).

-define(BUCKET, <<"pbtest">>).
-define(KEY, <<"ctx">>).

-define(MODIFY_OPTS, [create]).

confirm() ->
    Config = [ {riak_kv, [{handoff_concurrency, 100}]},
               {riak_core, [ {ring_creation_size, 16},
                             {vnode_management_timer, 1000} ]}],

    [N1, N2]=Nodes = rt_cluster:build_cluster(2, Config),

    create_bucket_types(Nodes, ?TYPES),

    [P1, P2] = PBClients = create_pb_clients(Nodes),

    S = make_set([a, b]),

    ok = store_set(P1, S),

    S2 = make_set([x, y, z]),

    M = make_map([{<<"set1">>, S}, {<<"set2">>, S2}]),

    ok = store_map(P2, M),


    verify_dt_converge:check_value(P1, riakc_pb_socket,
                                   {?STYPE, ?BUCKET}, ?KEY, riakc_set,
                                   [<<"a">>, <<"b">>]),

    verify_dt_converge:check_value(P2, riakc_pb_socket,
                                   {?MTYPE, ?BUCKET}, ?KEY, riakc_map,
                                   [{{<<"set1">>, set}, [<<"a">>, <<"b">>]},
                                    {{<<"set2">>, set}, [ <<"x">>, <<"y">>, <<"z">>]}]),

    lager:info("Partition cluster in two."),

    PartInfo = rt:partition([N1], [N2]),

    lager:info("Modify data on side 1"),
    %% Modify one side
    S1_1 = make_set([c, d, e]),
    ok= store_set(P1, S1_1),

    S3 = make_set([r, s]),

    M_1 = make_map([{<<"set1">>, S1_1}, {<<"set3">>, S3}]),
    ok = store_map(P1, M_1),

    verify_dt_converge:check_value(P1, riakc_pb_socket,
                                   {?STYPE, ?BUCKET}, ?KEY, riakc_set,
                                   [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>]),

    verify_dt_converge:check_value(P1, riakc_pb_socket,
                                   {?MTYPE, ?BUCKET}, ?KEY, riakc_map,
                                   [{{<<"set1">>, set}, [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>]},
                                    {{<<"set2">>, set}, [ <<"x">>, <<"y">>, <<"z">>]},
                                    {{<<"set3">>, set}, [<<"r">>, <<"s">>]}]),

    verify_dt_converge:check_value(P2, riakc_pb_socket,
                                   {?STYPE, ?BUCKET}, ?KEY, riakc_set,
                                   [<<"a">>, <<"b">>]),

    verify_dt_converge:check_value(P2, riakc_pb_socket,
                                   {?MTYPE, ?BUCKET}, ?KEY, riakc_map,
                                   [{{<<"set1">>, set}, [<<"a">>, <<"b">>]},
                                    {{<<"set2">>, set}, [ <<"x">>, <<"y">>, <<"z">>]}]),

    %% get the modified side's values

    S1_2 = fetch(P1, ?STYPE),
    M_2 = fetch(P1, ?MTYPE),

    %% operate on them and send to the partitioned side
    S1_3 = riakc_set:del_element(<<"d">>, S1_2),
    M_3 = riakc_map:update({<<"set1">>, set}, fun(Set1) ->
                                                      riakc_set:del_element(<<"e">>, Set1) end,
                           riakc_map:erase({<<"set3">>, set}, M_2)),

    %% we've removed elements that aren't to be found on P2, and a
    %% field that's never been seen on P2

    %% update the unmodified side
    ok = store_map(P2, M_3),
    ok = store_set(P2, S1_3),

    %% the value should not have changed, as these removes should be deferred

    verify_dt_converge:check_value(P2, riakc_pb_socket,
                                   {?STYPE, ?BUCKET}, ?KEY, riakc_set,
                                   [<<"a">>, <<"b">>]),

    verify_dt_converge:check_value(P2, riakc_pb_socket,
                                   {?MTYPE, ?BUCKET}, ?KEY, riakc_map,
                                   [{{<<"set1">>, set}, [<<"a">>, <<"b">>]},
                                    {{<<"set2">>, set}, [ <<"x">>, <<"y">>, <<"z">>]}]),

    %% Check both sides
    %% heal
    lager:info("Heal and check merged values"),
    ok = rt:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, riak_kv),

    %% verify all nodes agree

    verify_dt_converge:check_value(P1, riakc_pb_socket,
                                   {?STYPE, ?BUCKET}, ?KEY, riakc_set,
                                   [<<"a">>, <<"b">>, <<"c">>, <<"e">>]),

    verify_dt_converge:check_value(P1, riakc_pb_socket,
                                   {?MTYPE, ?BUCKET}, ?KEY, riakc_map,
                                   [{{<<"set1">>, set}, [<<"a">>, <<"b">>, <<"c">>, <<"d">>]},
                                    {{<<"set2">>, set}, [ <<"x">>, <<"y">>, <<"z">>]}]),

    verify_dt_converge:check_value(P2, riakc_pb_socket,
                                   {?STYPE, ?BUCKET}, ?KEY, riakc_set,
                                   [<<"a">>, <<"b">>, <<"c">>, <<"e">>]),

    verify_dt_converge:check_value(P2, riakc_pb_socket,
                                   {?MTYPE, ?BUCKET}, ?KEY, riakc_map,
                                   [{{<<"set1">>, set}, [<<"a">>, <<"b">>, <<"c">>, <<"d">>]},
                                    {{<<"set2">>, set}, [ <<"x">>, <<"y">>, <<"z">>]}]),


    [riakc_pb_socket:stop(C) || C <- PBClients],

    pass.

fetch(Client, BType) ->
    {ok, DT} = riakc_pb_socket:fetch_type(Client, {BType, ?BUCKET}, ?KEY),
    DT.


make_set(Elems) ->
    lists:foldl(fun(E, Set) ->
                        riakc_set:add_element(atom_to_binary(E, latin1), Set)
                end,
                riakc_set:new(),
                Elems).

make_map(Fields) ->
    lists:foldl(fun({F, V}, Map) ->
                        riakc_map:update({F, set}, fun(_) ->
                                                           V end,
                                         Map)
                end,
                riakc_map:new(),
                Fields).

store_set(Client, Set) ->
    riakc_pb_socket:update_type(Client, {?STYPE, ?BUCKET}, ?KEY, riakc_set:to_op(Set)).

store_map(Client, Map) ->
    riakc_pb_socket:update_type(Client, {?MTYPE, ?BUCKET}, ?KEY, riakc_map:to_op(Map)).

create_pb_clients(Nodes) ->
    [begin
         C = rt:pbc(N),
         riakc_pb_socket:set_options(C, [queue_if_disconnected]),
         C
     end || N <- Nodes].

create_bucket_types([N1|_], Types) ->
    lager:info("Creating bucket types with datatypes: ~p", [Types]),
    [rt:create_and_activate_bucket_type(N1, Name, [{datatype, Type}, {allow_mult, true}])
     || {Name, Type} <- Types ].

bucket_type_ready_fun(Name) ->
    fun(Node) ->
            Res = rpc:call(Node, riak_core_bucket_type, activate, [Name]),
            lager:info("is ~p ready ~p?", [Name, Res]),
            Res == ok
    end.

bucket_type_matches_fun(Types) ->
    fun(Node) ->
            lists:all(fun({Name, Type}) ->
                              Props = rpc:call(Node, riak_core_bucket_type, get,
                                               [Name]),
                              Props /= undefined andalso
                                  proplists:get_value(allow_mult, Props, false)
                                  andalso
                                  proplists:get_value(datatype, Props) == Type
                      end, Types)
    end.
