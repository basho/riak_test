%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.
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

-module(bucket_expiry).
-export([confirm/0]).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

%% Yes, I know these are all just riak_objects under the hood, but
%% worth verifying this continues to work if something changes.
-define(SET_TYPE, <<"set_type">>).
-define(STD_TYPE, <<"just_a_type">>).
-define(W1C_TYPE, <<"write_once">>).

-define(BUCKET(TYPE), {TYPE, <<"bucket">>}).

-define(BUCKET_PROPS, "{\\\"props\\\":{\\\"default_time_to_live\\\":\\\"1m\\\"}}").

-define(MUTUAL_CONF, [
                      {riak_kv, [{storage_backend,riak_kv_eleveldb_backend}]}
                     ]).

-define(NO_TTL_CONF, ?MUTUAL_CONF).

-define(TTL_CONF, ?MUTUAL_CONF ++
            [{eleveldb,
              [{expiry_enabled, true}]
             }]).

%% @doc This test exercises leveldb bucket-specific expiry, which only
%%      works with enteprise Riak.
%%
confirm() ->
    lager:info("Deploying nodes"),
    %% Initially the cluster will not have TTL enabled. This means any
    %% data written at first should never expire.
    Nodes = rt:build_cluster(3, ?NO_TTL_CONF),
    lager:info("Nodes deployed"),

    %%
    %% Select a random node and use it to create the bucket types
    %%
    Node1 = lists:nth(random:uniform(length((Nodes))), Nodes),
    lager:info("Using ~p for client", [Node1]),
    %% Create the set type
    lager:info("Create a set bucket type"),
    rt:create_and_activate_bucket_type(Node1, ?SET_TYPE, [{datatype, set}]),
    rt:wait_until_bucket_type_status(?SET_TYPE, active, Nodes),

    lager:info("Create a w1c bucket type"),
    rt:create_and_activate_bucket_type(Node1, ?W1C_TYPE, [{write_once, true}]),
    rt:wait_until_bucket_type_status(?W1C_TYPE, active, Nodes),

    lager:info("Create a standard bucket type"),
    rt:create_and_activate_bucket_type(Node1, ?STD_TYPE, []),
    rt:wait_until_bucket_type_status(?STD_TYPE, active, Nodes),

    lager:info("Created all bucket types"),

    PutNode1 = lists:nth(random:uniform(length((Nodes))), Nodes),
    lager:info("Using ~p for client", [PutNode1]),
    put_values(PutNode1, ?BUCKET(?STD_TYPE), 1, 100),

    PutNode2 = lists:nth(random:uniform(length((Nodes))), Nodes),
    lager:info("Using ~p for client", [PutNode2]),
    put_values(PutNode2, ?BUCKET(?W1C_TYPE), 1, 100),

    PutNode3 = lists:nth(random:uniform(length((Nodes))), Nodes),
    lager:info("Using ~p for client", [PutNode3]),
    Set1 = create_set(1, 100),
    Set2 = create_set(1, 100),
    store_set(PutNode3, ?BUCKET(?SET_TYPE), <<"set1">>, Set1),
    verify_set(PutNode3, ?BUCKET(?SET_TYPE), <<"set1">>, 1, 100),
    store_set(PutNode3, ?BUCKET(?SET_TYPE), <<"set2">>, Set2),
    verify_set(PutNode3, ?BUCKET(?SET_TYPE), <<"set2">>, 1, 100),

    lager:info("Populated all bucket types with values 1->100"),

    %% Force leveldb to compact, to record existing keys as
    %% non-expiring. This restart cycle may not always be necessary
    lager:info("Restart cluster to force leveldb compaction"),
    [ rt:stop_and_wait(Node) || Node <- Nodes ],
    [ rt:start_and_wait(Node) || Node <- Nodes ],
    [ rt:wait_for_service(Node, riak_kv) || Node <- Nodes ],

    %% Any values created before this point will not automatically
    %% expire because the global expiry flag was not enabled

    lager:info("Reconfiguring cluster with TTL enabled"),
    lists:foreach(fun(N) -> rt:update_app_config(N, ?TTL_CONF) end,
                  Nodes),

%%    timer:sleep(2 * 60 * 1000),
    lists:foreach(fun(N) -> rt:wait_for_service(N, riak_kv) end,
                  Nodes),

    %% Add 100 more values to each bucket type. Extend set1 but not set2.
    lager:info("Adding new values"),
    UpdateNode1 = lists:nth(random:uniform(length((Nodes))), Nodes),
    lager:info("Using ~p for client", [UpdateNode1]),
    Set1_1 = fetch_set(UpdateNode1, ?BUCKET(?SET_TYPE), <<"set1">>),
    Set1_2 = extend_set(Set1_1, 101, 200),
    store_set(UpdateNode1, ?BUCKET(?SET_TYPE), <<"set1">>, Set1_2),

    put_values(UpdateNode1, ?BUCKET(?W1C_TYPE), 101, 200),
    put_values(UpdateNode1, ?BUCKET(?STD_TYPE), 101, 200),

    UpdateNode2 = lists:nth(random:uniform(length((Nodes))), Nodes),
    lager:info("Using ~p for client", [UpdateNode2]),

    lager:info("Double-checking data"),
    check_key_range(UpdateNode2, 1, 200, ?BUCKET(?W1C_TYPE), present),
    check_key_range(UpdateNode2, 1, 200, ?BUCKET(?STD_TYPE), present),
    ?assertEqual(200, length(riakc_set:value(fetch_set(UpdateNode2, ?BUCKET(?SET_TYPE), <<"set1">>)))),
    ?assertEqual(100, length(riakc_set:value(fetch_set(UpdateNode2, ?BUCKET(?SET_TYPE), <<"set2">>)))),

    %% Update bucket types with 1 minute expiry. Cannot use protobufs (yet)
    lager:info("Setting bucket types to 1 minute TTL"),
    rt:admin(UpdateNode2, ["bucket-type", "update", binary_to_list(?SET_TYPE), ?BUCKET_PROPS]),
    rt:admin(UpdateNode2, ["bucket-type", "update", binary_to_list(?W1C_TYPE), ?BUCKET_PROPS]),
    rt:admin(UpdateNode2, ["bucket-type", "update", binary_to_list(?STD_TYPE), ?BUCKET_PROPS]),

    lists:foreach(fun(N) -> rpc:call(N, eleveldb, property_cache_flush, []) end,
                  Nodes),

    lager:info("Sleeping 2 minutes"),
    timer:sleep(2 * 60 * 1000),

    %% Verify records written before TTL was enabled are still
    %% present, but everything more recent is gone
    lager:info("Verifying oldest data still present"),
    check_key_range(UpdateNode2, 1, 100, ?BUCKET(?STD_TYPE), present),
    check_key_range(UpdateNode2, 1, 100, ?BUCKET(?W1C_TYPE), present),
    ?assertEqual(100, length(riakc_set:value(fetch_set(UpdateNode2, ?BUCKET(?SET_TYPE), <<"set2">>)))),

    lager:info("Verifying newer data missing"),
    check_key_range(UpdateNode2, 101, 200, ?BUCKET(?STD_TYPE), missing),
    check_key_range(UpdateNode2, 101, 200, ?BUCKET(?W1C_TYPE), missing),
    ?assertEqual({error, notfound}, riakc_pb_socket:get(rt:pbc(UpdateNode2),
                                                        ?BUCKET(?SET_TYPE),
                                                        "set1")),


    %% Now update set2 and make certain it vanishes after 2 minutes
    lager:info("Updating set2 to reset expiry metadata"),
    Set2_1 = fetch_set(UpdateNode2, ?BUCKET(?SET_TYPE), <<"set2">>),
    Set2_2 = extend_set(Set2_1, 101, 200),
    store_set(UpdateNode2, ?BUCKET(?SET_TYPE), <<"set2">>, Set2_2),
    ?assertEqual(200, length(riakc_set:value(fetch_set(UpdateNode2, ?BUCKET(?SET_TYPE), <<"set2">>)))),

    lager:info("Waiting for 2 minutes"),
    timer:sleep(2 * 60 * 1000),

    lager:info("Verifying set2 now gone"),
    ?assertEqual({error, notfound}, riakc_pb_socket:get(rt:pbc(UpdateNode2),
                                                        ?BUCKET(?SET_TYPE),
                                                        "set2")),
    pass.
%%
%% private
%%


put_values(Node, Bucket, Bot, Top) ->
    lists:foreach(fun(I) -> verify_put(Node, Bucket, list_to_binary("key" ++ integer_to_list(I)),
                                       integer_to_binary(I))
                  end, lists:seq(Bot, Top)).

create_set(Bot, Top) ->
    riakc_set:add_elements(
      lists:map(fun erlang:integer_to_binary/1, lists:seq(Bot, Top)),
      riakc_set:new()).

extend_set(Set, Bot, Top) ->
    riakc_set:add_elements(
      lists:map(fun erlang:integer_to_binary/1, lists:seq(Bot, Top)),
      Set).

fetch_set(Node, Bucket, Key) ->
    {ok, Set} = riakc_pb_socket:fetch_type(rt:pbc(Node), Bucket, Key),
    Set.

verify_set(Node, Bucket, Key, Bot, Top) ->
    Set = fetch_set(Node, Bucket, Key),
    ?assertEqual(lists:sort(lists:map(fun erlang:integer_to_binary/1, lists:seq(Bot, Top))),
                 lists:sort(riakc_set:value(Set))),
    ok.

store_set(Node, Bucket, Key, Set) ->
    riakc_pb_socket:update_type(rt:pbc(Node), Bucket, Key, riakc_set:to_op(Set)).

verify_put(Node, Bucket, Key, Value) ->
    verify_put(Node, Bucket, Key, Value, [], Value).

verify_put(Node, Bucket, Key, Value, Options, ExpectedValue) ->
    Client = rt:pbc(Node),
    _Ret = riakc_pb_socket:put(
        Client, riakc_obj:new(
            Bucket, Key, Value
        ),
        Options
    ),
    {ok, Val} = riakc_pb_socket:get(Client, Bucket, Key),
    ?assertEqual(ExpectedValue, riakc_obj:get_value(Val)),
    ok.

%% get(Node, Bucket, Key) ->
%%     Client = rt:pbc(Node),
%%     {ok, Val} = riakc_pb_socket:get(Client, Bucket, Key),
%%     binary_to_integer(riakc_obj:get_value(Val)).

verify_get(Client, Bucket, Key, ExpectedValue) ->
    lager:info("Checking ~p/~p", [Bucket, Key]),
    Response = riakc_pb_socket:get(Client, Bucket, Key, [{notfound_ok, false}]),
    ?assertMatch({ok, _}, Response),
    {ok, Val} = Response,
    ?assertEqual(ExpectedValue, riakc_obj:get_value(Val)).

verify_gone(Client, Bucket, Key, _ExpectedValue) ->
    ?assertEqual({error, notfound}, riakc_pb_socket:get(Client, Bucket, Key)).

check_key_range(Node, Bot, Top, Bucket, present) ->
    check_key_range(Node, Bot, Top, Bucket, list_present),
    check_key_range(Node, Bot, Top, Bucket, fun verify_get/4);
check_key_range(Node, Bot, Top, Bucket, missing) ->
    check_key_range(Node, Bot, Top, Bucket, list_missing),
    check_key_range(Node, Bot, Top, Bucket, fun verify_gone/4);
check_key_range(Node, Bot, Top, Bucket, list_present) ->
    {ok, List} = riakc_pb_socket:list_keys(rt:pbc(Node), Bucket),
    Goal = lists:sort(lists:map(
                        fun(I) -> list_to_binary("key" ++ integer_to_list(I)) end,
                        lists:seq(Bot, Top))),
    ?assertEqual(Goal, lists:sort(sets:to_list(sets:intersection(sets:from_list(Goal), sets:from_list(List)))));
check_key_range(Node, Bot, Top, Bucket, list_missing) ->
    {ok, List} = riakc_pb_socket:list_keys(rt:pbc(Node), Bucket),
    AntiGoal = lists:sort(lists:map(
                            fun(I) -> list_to_binary("key" ++ integer_to_list(I)) end,
                            lists:seq(Bot, Top))),
    ?assert(sets:is_disjoint(sets:from_list(AntiGoal), sets:from_list(List)));
check_key_range(Node, Bot, Top, Bucket, Fun) when is_function(Fun) ->
    lager:info("Verifying key range ~B to ~B in ~p",
               [Bot, Top, Bucket]),
    lists:foreach(fun(I) -> Fun(rt:pbc(Node), Bucket,
                                list_to_binary("key" ++ integer_to_list(I)),
                                integer_to_binary(I))
                  end, lists:seq(Bot, Top)).
