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
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(CTYPE, <<"counters">>).
-define(STYPE, <<"sets">>).
-define(MTYPE, <<"maps">>).
-define(TYPES, [{?CTYPE, counter},
                {?STYPE, set},
                {?MTYPE, map}]).
-define(BUCKET, <<"test">>).
-define(KEY, <<"test">>).

confirm() ->
    [N1, N2, N3, N4]=Nodes = rt:build_cluster(4),

    create_bucket_types(Nodes, ?TYPES),

    [P1, P2, P3, P4] = Clients =
        [ begin
              C = rt:pbc(N),
              riakc_pb_socket:set_options(C, [queue_if_disconnected]),
              C
          end || N <- Nodes ],


    %% Do some updates to each type
    [ update_1(Type, Client) ||
        {Type, Client} <- lists:zip(?TYPES, [P1, P2, P3]) ],

    %% Check that the updates are stored
    [ check_1(Type, Client) ||
        {Type, Client} <- lists:zip(?TYPES, [P4, P3, P2]) ],

    lager:info("Partition cluster in two."),

    PartInfo = rt:partition([N1, N2], [N3, N4]),

    lager:info("Modify data on side 1"),
    %% Modify one side
    [ update_2a(Type, Client) ||
        {Type, Client} <- lists:zip(?TYPES, [P1, P2, P1]) ],

    lager:info("Check data is unmodified on side 2"),
    %% check value on one side is different from other
    [ check_2b(Type, Client) ||
        {Type, Client} <- lists:zip(?TYPES, [P4, P3, P4]) ],

    lager:info("Modify data on side 2"),
    %% Modify other side
    [ update_3b(Type, Client) ||
        {Type, Client} <- lists:zip(?TYPES, [P3, P4, P3]) ],

    lager:info("Check data is unmodified on side 1"),
    %% verify values differ
    [ check_3a(Type, Client) ||
        {Type, Client} <- lists:zip(?TYPES, [P2, P2, P1]) ],

    %% heal
    lager:info("Heal and check merged values"),
    ok = rt:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, riak_kv),

    %% verify all nodes agree
    [ ?assertEqual(ok, check_4(Type, Client))
      || Type <- ?TYPES, Client <- Clients ],

    pass.

create_bucket_types([N1|_]=Nodes, Types) ->
    lager:info("Creating bucket types with datatypes: ~p", [Types]),
    [ rpc:call(N1, riak_core_bucket_type, create,
               [Name, [{datatype, Type}, {allow_mult, true}]]) ||
        {Name, Type} <- Types ],
    [ rt:wait_until(N, bucket_type_matches_fun(Types)) || N <- Nodes].


bucket_type_matches_fun(Types) ->
    fun(Node) ->
            lists:all(fun({Name, Type}) ->
                              Props = rpc:call(Node, riak_core_bucket_type, get, [Name]),
                              Props /= undefined andalso
                                  proplists:get_value(allow_mult, Props, false) andalso
                                  proplists:get_value(datatype, Props) == Type
                      end, Types)
    end.


update_1({BType, counter}, Client) ->
    lager:info("update_1: Updating counter"),
    riakc_pb_socket:modify_type(Client,
                                fun(C) ->
                                        riakc_counter:increment(5, C)
                                end,
                                {BType, ?BUCKET}, ?KEY, [create]);
update_1({BType, set}, Client) ->
    lager:info("update_1: Updating set"),
    riakc_pb_socket:modify_type(Client,
                                fun(S) ->
                                        riakc_set:add_element(<<"Riak">>, S)
                                end,
                                {BType, ?BUCKET}, ?KEY, [create]);
update_1({BType, map}, Client) ->
    lager:info("update_1: Updating map"),
    riakc_pb_socket:modify_type(Client,
                                fun(M) ->
                                        M1 = riakc_map:update({<<"friends">>, set},
                                                              fun(S) ->
                                                                      riakc_set:add_element(<<"Russell">>, S)
                                                              end, M),
                                        riakc_map:update({<<"followers">>, counter},
                                                         fun(C) ->
                                                                 riakc_counter:increment(10, C)
                                                         end, M1)
                                end,
                                {BType, ?BUCKET}, ?KEY, [create]).

check_1({BType, counter}, Client) ->
    lager:info("check_1: Checking counter value is correct"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_counter, 5);
check_1({BType, set}, Client) ->
    lager:info("check_1: Checking set value is correct"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_set, [<<"Riak">>]);
check_1({BType, map}, Client) ->
    lager:info("check_1: Checking map value is correct"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_map,
                [{{<<"followers">>, counter}, 10},
                 {{<<"friends">>, set}, [<<"Russell">>]}]).

update_2a({BType, counter}, Client) ->
    riakc_pb_socket:modify_type(Client,
                                fun(C) ->
                                        riakc_counter:decrement(10, C)
                                end,
                                {BType, ?BUCKET}, ?KEY, [create]);
update_2a({BType, set}, Client) ->
    riakc_pb_socket:modify_type(Client,
                                fun(S) ->
                                        riakc_set:add_element(<<"Voldemort">>,
                                                              riakc_set:add_element(<<"Cassandra">>, S))
                                end,
                                {BType, ?BUCKET}, ?KEY, [create]);
update_2a({BType, map}, Client) ->
    riakc_pb_socket:modify_type(Client,
                                fun(M) ->
                                        M1 = riakc_map:update({<<"friends">>, set},
                                                              fun(S) ->
                                                                      riakc_set:add_element(<<"Sam">>, S)
                                                              end, M),
                                        riakc_map:add({<<"verified">>, flag}, M1)
                                end,
                                {BType, ?BUCKET}, ?KEY, [create]).

check_2b({BType, counter}, Client) ->
    lager:info("check_2b: Checking counter value is unchanged"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_counter, 5);
check_2b({BType, set}, Client) ->
    lager:info("check_2b: Checking set value is unchanged"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_set, [<<"Riak">>]);
check_2b({BType, map}, Client) ->
    lager:info("check_2b: Checking map value is unchanged"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_map,
                [{{<<"followers">>, counter}, 10},
                 {{<<"friends">>, set}, [<<"Russell">>]}]).

update_3b({BType, counter}, Client) ->
    riakc_pb_socket:modify_type(Client,
                                fun(C) ->
                                        riakc_counter:increment(2, C)
                                end,
                                {BType, ?BUCKET}, ?KEY, [create]);
update_3b({BType, set}, Client) ->
    riakc_pb_socket:modify_type(Client,
                                fun(S) ->
                                        riakc_set:add_element(<<"Couchbase">>, S)
                                end,
                                {BType, ?BUCKET}, ?KEY, [create]);
update_3b({BType, map}, Client) ->
    riakc_pb_socket:modify_type(Client,
                                fun(M) ->
                                        M1 = riakc_map:erase({<<"friends">>, set}, M),
                                        riakc_map:update({<<"emails">>, map},
                                                         fun(MI) ->
                                                                 riakc_map:update({<<"home">>, register},
                                                                                  fun(R) ->
                                                                                          riakc_register:set(<<"foo@bar.com">>, R)
                                                                                  end, MI)
                                                         end,
                                                         M1)
                                end,
                                {BType, ?BUCKET}, ?KEY, [create]).

check_3a({BType, counter}, Client) ->
    lager:info("check_3a: Checking counter value is unchanged"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_counter, -5);
check_3a({BType, set}, Client) ->
    lager:info("check_3a: Checking set value is unchanged"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_set, [<<"Cassandra">>, <<"Riak">>, <<"Voldemort">>]);
check_3a({BType, map}, Client) ->
    lager:info("check_3a: Checking map value is unchanged"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_map,
                [{{<<"followers">>, counter}, 10},
                 {{<<"friends">>, set}, [<<"Russell">>, <<"Sam">>]},
                 {{<<"verified">>, flag}, false}]).

check_4({BType, counter}, Client) ->
    lager:info("check_4: Checking final merged value of counter"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_counter, -3);
check_4({BType, set}, Client) ->
    lager:info("check_4: Checking final merged value of set"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_set,
                [<<"Cassandra">>, <<"Couchbase">>, <<"Riak">>, <<"Voldemort">>]);
check_4({BType, map}, Client) ->
    lager:info("check_4: Checking final merged value of map"),
    check_value(Client, {BType, ?BUCKET}, ?KEY, riakc_map,
                [{{<<"emails">>, map},
                  [
                   {{<<"home">>, register}, <<"foo@bar.com">>}
                  ]},
                 {{<<"followers">>, counter}, 10},
                 %% TODO: add on side A wins over erase side B (non reset-remove)
                 %% {{<<"friends">>, set}, [<<"Russell">>, <<"Sam">>]},
                 {{<<"friends">>, set}, [<<"Sam">>]}, %% reset-remove
                 {{<<"verified">>, flag}, false}]).



check_value(Client, Bucket, Key, Mod, Expected) ->
    rt:wait_until(fun() ->
                          try
                              Result = riakc_pb_socket:fetch_type(Client, Bucket, Key, [{r, 2}, {notfound_ok, true},
                                                                                        {timeout, 5000}]),
                              ?assertMatch({ok, _}, Result),
                              {ok, C} = Result,
                              ?assertEqual(true, Mod:is_type(C)),
                              ?assertEqual(Expected, Mod:value(C)),
                              true
                          catch
                              Type:Error ->
                                  lager:debug("check_value(~p,~p,~p,~p,~p) failed: ~p:~p", [Client, Bucket, Key, Mod, Expected, Type, Error]),
                                  false
                          end
                  end).
