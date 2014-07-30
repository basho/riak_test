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
%%% riak_test for riak_dt CRDT convergence
%%% @end

-module(verify_dt_converge).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(CTYPE, <<"counters">>).
-define(STYPE, <<"sets">>).
-define(MTYPE, <<"maps">>).
-define(TYPES, [{?CTYPE, counter},
                {?STYPE, set},
                {?MTYPE, map}]).

-define(PB_BUCKET, <<"pbtest">>).
-define(HTTP_BUCKET, <<"httptest">>).
-define(KEY, <<"test">>).

%% Type, Bucket, Client, Mod

-define(MODIFY_OPTS, [create]).

confirm() ->
    Config = [ {riak_kv, [{handoff_concurrency, 100}]},
               {riak_core, [ {ring_creation_size, 16},
                             {vnode_management_timer, 1000} ]}],

    [N1, N2, N3, N4]=Nodes = rt_cluster:build_cluster(4, Config),

    create_bucket_types(Nodes, ?TYPES),

    [P1, P2, P3, P4] = PBClients = create_pb_clients(Nodes),
    [H1, H2, H3, H4] = HTTPClients = create_http_clients(Nodes),

    %% Do some updates to each type
    [update_1(Type, ?PB_BUCKET, Client, riakc_pb_socket) ||
        {Type, Client} <- lists:zip(?TYPES, [P1, P2, P3])],

    [update_1(Type, ?HTTP_BUCKET, Client, rhc) ||
        {Type, Client} <- lists:zip(?TYPES, [H1, H2, H3])],

    %% Check that the updates are stored
    [check_1(Type, ?PB_BUCKET, Client, riakc_pb_socket) ||
        {Type, Client} <- lists:zip(?TYPES, [P4, P3, P2])],

    [check_1(Type, ?HTTP_BUCKET, Client, rhc) ||
        {Type, Client} <- lists:zip(?TYPES, [H4, H3, H2])],

    lager:info("Partition cluster in two."),

    PartInfo = rt:partition([N1, N2], [N3, N4]),

    lager:info("Modify data on side 1"),
    %% Modify one side
    [update_2a(Type, ?PB_BUCKET, Client, riakc_pb_socket) ||
        {Type, Client} <- lists:zip(?TYPES, [P1, P2, P1])],

    [update_2a(Type, ?HTTP_BUCKET, Client, rhc) ||
        {Type, Client} <- lists:zip(?TYPES, [H1, H2, H1])],

    lager:info("Check data is unmodified on side 2"),
    %% check value on one side is different from other
    [check_2b(Type, ?PB_BUCKET, Client, riakc_pb_socket) ||
        {Type, Client} <- lists:zip(?TYPES, [P4, P3, P4])],

    [check_2b(Type, ?HTTP_BUCKET, Client, rhc) ||
        {Type, Client} <- lists:zip(?TYPES, [H4, H3, H4])],

    lager:info("Modify data on side 2"),
    %% Modify other side
    [update_3b(Type, ?PB_BUCKET, Client, riakc_pb_socket) ||
        {Type, Client} <- lists:zip(?TYPES, [P3, P4, P3])],

    [update_3b(Type, ?HTTP_BUCKET, Client, rhc) ||
        {Type, Client} <- lists:zip(?TYPES, [H3, H4, H3])],

    lager:info("Check data is unmodified on side 1"),
    %% verify values differ
    [check_3a(Type, ?PB_BUCKET, Client, riakc_pb_socket) ||
        {Type, Client} <- lists:zip(?TYPES, [P2, P2, P1])],

    [check_3a(Type, ?HTTP_BUCKET, Client, rhc) ||
        {Type, Client} <- lists:zip(?TYPES, [H2, H2, H1])],

    %% heal
    lager:info("Heal and check merged values"),
    ok = rt:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, riak_kv),

    %% verify all nodes agree
    [?assertEqual(ok, check_4(Type, ?PB_BUCKET, Client, riakc_pb_socket))
     || Type <- ?TYPES, Client <- PBClients],

    [?assertEqual(ok, check_4(Type, ?HTTP_BUCKET, Client, rhc))
     || Type <- ?TYPES, Client <- HTTPClients],

    [riakc_pb_socket:stop(C) || C <- PBClients],

    pass.

create_pb_clients(Nodes) ->
    [begin
         C = rt_pb:pbc(N),
         riakc_pb_socket:set_options(C, [queue_if_disconnected]),
         C
     end || N <- Nodes].

create_http_clients(Nodes) ->
    [ rt:httpc(N) || N <- Nodes ].

create_bucket_types([N1|_]=Nodes, Types) ->
    lager:info("Creating bucket types with datatypes: ~p", [Types]),
    [ rpc:call(N1, riak_core_bucket_type, create,
               [Name, [{datatype, Type}, {allow_mult, true}]]) ||
        {Name, Type} <- Types ],
    [rt:wait_until(N1, bucket_type_ready_fun(Name)) || {Name, _Type} <- Types],
    [ rt:wait_until(N, bucket_type_matches_fun(Types)) || N <- Nodes].

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


update_1({BType, counter}, Bucket, Client, CMod) ->
    lager:info("update_1: Updating counter"),
    CMod:modify_type(Client,
                     fun(C) ->
                             riakc_counter:increment(5, C)
                     end,
                     {BType, Bucket}, ?KEY, ?MODIFY_OPTS);
update_1({BType, set}, Bucket, Client, CMod) ->
    lager:info("update_1: Updating set"),
    CMod:modify_type(Client,
                     fun(S) ->
                             riakc_set:add_element(<<"Riak">>, S)
                     end,
                     {BType, Bucket}, ?KEY, ?MODIFY_OPTS);
update_1({BType, map}, Bucket, Client, CMod) ->
    lager:info("update_1: Updating map"),
    CMod:modify_type(Client,
                     fun(M) ->
                             M1 = riakc_map:update(
                                    {<<"friends">>, set},
                                    fun(S) ->
                                            riakc_set:add_element(<<"Russell">>,
                                                                  S)
                                    end, M),
                             riakc_map:update(
                               {<<"followers">>, counter},
                               fun(C) ->
                                       riakc_counter:increment(10, C)
                               end, M1)
                     end,
                     {BType, Bucket}, ?KEY, ?MODIFY_OPTS).

check_1({BType, counter}, Bucket, Client, CMod) ->
    lager:info("check_1: Checking counter value is correct"),
    check_value(Client,CMod,{BType, Bucket},?KEY,riakc_counter,5);
check_1({BType, set}, Bucket, Client, CMod) ->
    lager:info("check_1: Checking set value is correct"),
    check_value(Client,CMod,{BType, Bucket},?KEY,riakc_set,[<<"Riak">>]);
check_1({BType, map}, Bucket, Client, CMod) ->
    lager:info("check_1: Checking map value is correct"),
    check_value(Client, CMod, {BType, Bucket}, ?KEY, riakc_map,
                [{{<<"followers">>, counter}, 10},
                 {{<<"friends">>, set}, [<<"Russell">>]}]).

update_2a({BType, counter}, Bucket, Client, CMod) ->
    CMod:modify_type(Client,
                     fun(C) ->
                             riakc_counter:decrement(10, C)
                     end,
                     {BType, Bucket}, ?KEY, ?MODIFY_OPTS);
update_2a({BType, set}, Bucket, Client, CMod) ->
    CMod:modify_type(Client,
                     fun(S) ->
                             riakc_set:add_element(
                               <<"Voldemort">>,
                               riakc_set:add_element(<<"Cassandra">>, S))
                     end,
                     {BType, Bucket}, ?KEY, ?MODIFY_OPTS);
update_2a({BType, map}, Bucket, Client, CMod) ->
    CMod:modify_type(Client,
                     fun(M) ->
                             M1 = riakc_map:update(
                                    {<<"friends">>, set},
                                    fun(S) ->
                                            riakc_set:add_element(<<"Sam">>, S)
                                    end, M),
                             riakc_map:update({<<"verified">>, flag}, fun(F) ->
                                                                              riakc_flag:disable(F)
                                                                                  end,
                                              M1)
                     end,
                     {BType, Bucket}, ?KEY, ?MODIFY_OPTS).

check_2b({BType, counter}, Bucket, Client, CMod) ->
    lager:info("check_2b: Checking counter value is unchanged"),
    check_value(Client, CMod, {BType, Bucket}, ?KEY, riakc_counter, 5);
check_2b({BType, set},Bucket,Client,CMod) ->
    lager:info("check_2b: Checking set value is unchanged"),
    check_value(Client, CMod, {BType, Bucket}, ?KEY, riakc_set, [<<"Riak">>]);
check_2b({BType, map},Bucket,Client,CMod) ->
    lager:info("check_2b: Checking map value is unchanged"),
    check_value(Client, CMod, {BType, Bucket}, ?KEY, riakc_map,
                [{{<<"followers">>, counter}, 10},
                 {{<<"friends">>, set}, [<<"Russell">>]}]).

update_3b({BType, counter}, Bucket, Client, CMod) ->
    CMod:modify_type(Client,
                     fun(C) ->
                             riakc_counter:increment(2, C)
                     end,
                     {BType, Bucket}, ?KEY, ?MODIFY_OPTS);
update_3b({BType, set}, Bucket, Client, CMod) ->
    CMod:modify_type(Client,
                     fun(S) ->
                             riakc_set:add_element(<<"Couchbase">>, S)
                     end,
                     {BType, Bucket}, ?KEY, ?MODIFY_OPTS);
update_3b({BType, map},Bucket,Client,CMod) ->
    CMod:modify_type(Client,
                     fun(M) ->
                             M1 = riakc_map:erase({<<"friends">>, set}, M),
                             riakc_map:update(
                               {<<"emails">>, map},
                               fun(MI) ->
                                       riakc_map:update(
                                         {<<"home">>, register},
                                         fun(R) ->
                                                 riakc_register:set(
                                                   <<"foo@bar.com">>, R)
                                         end, MI)
                               end,
                               M1)
                     end,
                     {BType, Bucket}, ?KEY, ?MODIFY_OPTS).

check_3a({BType, counter}, Bucket, Client, CMod) ->
    lager:info("check_3a: Checking counter value is unchanged"),
    check_value(Client,CMod,{BType, Bucket},?KEY,riakc_counter,-5);
check_3a({BType, set}, Bucket, Client, CMod) ->
    lager:info("check_3a: Checking set value is unchanged"),
    check_value(Client,CMod,{BType, Bucket},?KEY,riakc_set,
                [<<"Cassandra">>, <<"Riak">>, <<"Voldemort">>]);
check_3a({BType, map}, Bucket, Client, CMod) ->
    lager:info("check_3a: Checking map value is unchanged"),
    check_value(Client, CMod, {BType, Bucket}, ?KEY, riakc_map,
                [{{<<"followers">>, counter}, 10},
                 {{<<"friends">>, set}, [<<"Russell">>, <<"Sam">>]},
                 {{<<"verified">>, flag}, false}]).

check_4({BType, counter}, Bucket, Client, CMod) ->
    lager:info("check_4: Checking final merged value of counter"),
    check_value(Client,CMod,{BType, Bucket},?KEY,riakc_counter,-3,
                [{pr, 3}, {notfound_ok, false}]);
check_4({BType, set}, Bucket, Client, CMod) ->
    lager:info("check_4: Checking final merged value of set"),
    check_value(Client,
                CMod, {BType, Bucket},
                ?KEY,
                riakc_set,
                [<<"Cassandra">>, <<"Couchbase">>, <<"Riak">>, <<"Voldemort">>],
                [{pr, 3}, {notfound_ok, false}]);
check_4({BType, map}, Bucket, Client, CMod) ->
    lager:info("check_4: Checking final merged value of map"),
    check_value(Client, CMod, {BType, Bucket}, ?KEY, riakc_map,
                [{{<<"emails">>, map},
                  [
                   {{<<"home">>, register}, <<"foo@bar.com">>}
                  ]},
                 {{<<"followers">>, counter}, 10},
                 {{<<"friends">>, set}, [<<"Sam">>]},
                 {{<<"verified">>, flag}, false}],
                [{pr, 3}, {notfound_ok, false}]).

check_value(Client, CMod, Bucket, Key, DTMod, Expected) ->
    check_value(Client,CMod,Bucket,Key,DTMod,Expected,
                [{r,2}, {notfound_ok, true}, {timeout, 5000}]).

check_value(Client, CMod, Bucket, Key, DTMod, Expected, Options) ->
    rt:wait_until(fun() ->
                          try
                              Result = CMod:fetch_type(Client, Bucket, Key,
                                                       Options),
                              lager:info("Expected ~p~n got ~p~n", [Expected, Result]),
                              ?assertMatch({ok, _}, Result),
                              {ok, C} = Result,
                              ?assertEqual(true, DTMod:is_type(C)),
                              ?assertEqual(Expected, DTMod:value(C)),
                              true
                          catch
                              Type:Error ->
                                  lager:debug("check_value(~p,~p,~p,~p,~p) "
                                              "failed: ~p:~p", [Client, Bucket,
                                                                Key, DTMod,
                                                                Expected, Type,
                                                                Error]),
                                  false
                          end
                  end).
