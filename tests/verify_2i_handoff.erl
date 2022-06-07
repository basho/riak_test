%% -------------------------------------------------------------------
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
-module(verify_2i_handoff).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").
    % Need ?INDEX_STREAM_RESULT

-import(secondary_index_tests, [int_to_key/1]).

-define(FOO, "foo").
-define(Q_OPTS, [{return_terms, true}]).
-define(Q_LOOP, 20).
-define(Q_PAUSE, 500).


%% We unexpectedly saw in basho_bench testing in an environment where we
%% were testing handoffs, examples of the error logging for a match between
%% a binary value and a CRDT.  This was related to a sibling being present
%% without a dot on the metadata.
%%
%% This may have been as a result of at some time failing to have dvv_enabled
%% on the bucket being tested.
%%
%% To make sure this is not a real issue, this test mixes values that overlap
%% with the CRDT tag, generating siblings, and handoffs.  Nothing untoward
%% should occur

confirm() ->
    Items    = 10000, %% How many test items in each group to write/verify?
    run_test(Items, 4),

    lager:info("Test verify_2i_handoff passed."),
    pass.

run_test(Items, NTestNodes) ->
    lager:info("Testing handoff (items ~p, nodes: ~p)", [Items, NTestNodes]),

    lager:info("Spinning up test nodes"),
    [RootNode, FirstJoin, SecondJoin, LastJoin] = Nodes = 
        deploy_test_nodes(NTestNodes),

    rt:wait_for_service(RootNode, riak_kv),

    set_handoff_encoding(default, Nodes),

    lager:info("Initialise bucket type."),
    BProps = [{allow_mult, true}, {last_write_wins, false},
                {node_confirms, 1}, {dvv_enabled, true}],
    B1 = {<<"type1">>, <<"B1">>},
    B2 = <<"B2">>,
    {ok, C} = riak:client_connect(RootNode),
    ok = riak_client:set_bucket(B2, BProps, C),
    ok = rt:create_and_activate_bucket_type(RootNode, <<"type1">>, BProps),

    RootClient = rt:pbc(RootNode),

    lager:info("Populating initial data."),
    HttpC1 = rt:httpc(RootNode),
    lists:foreach(fun(N) -> put_an_object(HttpC1, B1, N) end, lists:seq(1, Items)),
    lists:foreach(fun(N) -> put_an_object(HttpC1, B2, N) end, lists:seq(1, Items)),

    lager:info("Testing 2i Queries"),
    repeatedly_test_query(RootClient, Items, B1, 1, assert),
    repeatedly_test_query(RootClient, Items, B2, 1, assert),

    lager:info("Waiting for service on second node."),
    rt:wait_for_service(FirstJoin, riak_kv),

    lager:info("Joining new node with cluster."),
    rt:join(FirstJoin, RootNode),
    repeatedly_test_query(RootClient, Items, B1, ?Q_LOOP, assert),
    ?assertEqual(ok, rt:wait_until_nodes_ready([RootNode, FirstJoin])),
    rt:wait_until_no_pending_changes([RootNode, FirstJoin]),
    lager:info("Handoff complete"),

    lager:info("Testing 2i Queries post-handoff"),
    FirstClient = rt:pbc(FirstJoin),
    repeatedly_test_query(FirstClient, Items, B1, 1, assert),
    repeatedly_test_query(FirstClient, Items, B2, 1, assert),
    riakc_pb_socket:stop(FirstClient),

    
    lager:info("Waiting for service on third node."),
    rt:wait_for_service(SecondJoin, riak_kv),

    lager:info("Joining new node with cluster."),
    rt:join(SecondJoin, RootNode),
    repeatedly_test_query(RootClient, Items, B1, ?Q_LOOP, assert),
    ?assertEqual(ok, rt:wait_until_nodes_ready([RootNode, SecondJoin])),
    rt:wait_until_no_pending_changes([RootNode, SecondJoin]),
    lager:info("Handoff complete"),

    lager:info("Testing 2i Queries post-handoff"),
    SecondClient = rt:pbc(SecondJoin),
    repeatedly_test_query(SecondClient, Items, B1, 1, assert),
    repeatedly_test_query(SecondClient, Items, B2, 1, assert),
    riakc_pb_socket:stop(SecondClient),

    lager:info("Joining new node with cluster."),
    rt:join(LastJoin, RootNode),
    % repeatedly_test_query(RootClient, Items, B1, ?Q_LOOP, assert),
    ?assertEqual(ok, rt:wait_until_nodes_ready([RootNode, LastJoin])),
    rt:wait_until_no_pending_changes([RootNode, LastJoin]),
    lager:info("Handoff complete"),

    lager:info("Testing 2i Queries post-handoff"),
    LastClient = rt:pbc(LastJoin),
    repeatedly_test_query(LastClient, Items, B1, 1, assert),
    repeatedly_test_query(LastClient, Items, B2, 1, assert),
    riakc_pb_socket:stop(LastClient),

    lager:info("Stopping node in cluster"),
    rt:stop(LastJoin),
    repeatedly_test_query(RootClient, Items, B1, ?Q_LOOP, assert),
    rt:wait_until_unpingable(LastJoin),

    lager:info("Loading data whilst node down"),
    lists:foreach(fun(N) -> put_an_object(HttpC1, B1, N) end, lists:seq(Items + 1, 2 * Items)),
    lists:foreach(fun(N) -> put_an_object(HttpC1, B2, N) end, lists:seq(Items + 1, 2 * Items)),

    lager:info("Check 2i shows new results"),
    repeatedly_test_query(RootClient, 2 * Items, B1, 1, assert),

    lager:info("Restarting node in cluster"),
    lager:info("Primary vnodes on restarted nodes ..."),
    lager:info("... will take over before hinted handoffs complete ..."),
    lager:info("... so 2i results will not contain recent additions ..."),
    lager:info("... until those transfers finish"),
    rt:start(LastJoin),
    repeatedly_test_query(RootClient, 2 * Items, B1, ?Q_LOOP, report),
    rt:wait_until_pingable(LastJoin),
    repeatedly_test_query(RootClient, 2 * Items, B1, ?Q_LOOP, report),
    rt:wait_until_no_pending_changes([RootNode, LastJoin]),
    repeatedly_test_query(RootClient, 2 * Items, B1, ?Q_LOOP, report),
    rt:wait_until_transfers_complete([RootNode, FirstJoin, SecondJoin, LastJoin]),

    lager:info("Check 2i now shows new results"),
    repeatedly_test_query(RootClient, 2 * Items, B1, 1, assert),
    repeatedly_test_query(RootClient, 2 * Items, B2, 1, assert),
    LastClientX = rt:pbc(LastJoin),
    repeatedly_test_query(LastClientX, 2 * Items, B1, 1, assert),
    repeatedly_test_query(LastClientX, 2 * Items, B2, 1, assert),
    riakc_pb_socket:stop(LastClientX),

    riakc_pb_socket:stop(RootClient),

    %% Prepare for the next call to our test (we aren't polite about it, it's faster that way):
    lager:info("Bringing down test nodes"),
    lists:foreach(fun(N) -> rt:brutal_kill(N) end, Nodes),
    pass.

%% Check the PB result against our expectations
%% and the non-streamed HTTP
assertEqual(PB, Expected, B, Query, Opts, ResultKey) ->
    {ok, PBRes} = stream_pb(PB, B, Query, Opts),
    PBKeys = proplists:get_value(ResultKey, PBRes, []),
    ?assertEqual(Expected, length(PBKeys)).

reportIfEqual(PB, Expected, B, Query, Opts, ResultKey) ->
    {ok, PBRes} = stream_pb(PB, B, Query, Opts),
    PBKeys = proplists:get_value(ResultKey, PBRes, []),
    case length(PBKeys) of
        Expected ->
            lager:info("Expected keys found ~w", [Expected]);
        N ->
            lager:info("Expected keys ~w but only ~w keys found",
                        [Expected, N])
    end.

set_handoff_encoding(default, _) ->
    lager:info("Using default encoding type."),
    true;
set_handoff_encoding(Encoding, Nodes) ->
    lager:info("Forcing encoding type to ~p.", [Encoding]),

    %% Update all nodes (capabilities are not re-negotiated):
    [begin
         rt:update_app_config(Node, override_data(Encoding)),
         assert_using(Node, {riak_kv, handoff_data_encoding}, Encoding)
     end || Node <- Nodes].

override_data(Encoding) ->
    [
     { riak_core,
       [
        { override_capability,
          [
           { handoff_data_encoding,
             [
              {    use, Encoding},
              { prefer, Encoding}
             ]
           }
          ]
        }
       ]}].

assert_using(Node, {CapabilityCategory, CapabilityName}, ExpectedCapabilityName) ->
    lager:info("assert_using ~p =:= ~p", [ExpectedCapabilityName, CapabilityName]),
    ExpectedCapabilityName =:= rt:capability(Node, {CapabilityCategory, CapabilityName}).


%% general 2i utility
put_an_object(HTTPc, B, N) ->
    Key = int_to_key(N),
    Data = io_lib:format("data~p", [N]),
    BinIndex = list_to_binary(?FOO ++ integer_to_list(N)),
    Indexes = [{"field1_bin", BinIndex},
               {"field2_int", N}
              ],
    put_an_object(HTTPc, B, Key, Data, Indexes).

put_an_object(HTTPc, B, Key, Data, Indexes) when is_list(Indexes) ->
    MetaData = dict:from_list([{<<"index">>, Indexes}]),
    Robj0 = riakc_obj:new(B, Key),
    Robj1 = riakc_obj:update_value(Robj0, Data),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    rhc:put(HTTPc, Robj2).

stream_pb(Pid, B, {F, S, E}, Opts) ->
    riakc_pb_socket:get_index_range(Pid, B, F, S, E, [stream|Opts]),
    stream_loop().

stream_loop() ->
    stream_loop(orddict:new()).

stream_loop(Acc) ->
    receive
        {_Ref, {done, undefined}} ->
            {ok, orddict:to_list(Acc)};
        {_Ref, {done, Continuation}} ->
            {ok, orddict:store(continuation, Continuation, Acc)};
        {_Ref, ?INDEX_STREAM_RESULT{terms=undefined, keys=Keys}} ->
            Acc2 = orddict:update(keys, fun(Existing) -> Existing++Keys end, Keys, Acc),
            stream_loop(Acc2);
        {_Ref, ?INDEX_STREAM_RESULT{terms=Results}} ->
            Acc2 = orddict:update(results, fun(Existing) -> Existing++Results end, Results, Acc),
            stream_loop(Acc2);
        {_Ref, {error, <<"{error,timeout}">>}} ->
            {error, timeout};
        {_Ref, Wat} ->
            lager:info("got a wat ~p", [Wat]),
            stream_loop(Acc)
    end.

repeatedly_test_query(_Client, _Items, _Bucket, 0, _Report) ->
    ok;
repeatedly_test_query(Client, Items, Bucket, N, assert) ->
    assertEqual(Client, Items, Bucket,
                {<<"field1_bin">>, list_to_binary(?FOO), list_to_binary(?FOO ++ "z")},
                ?Q_OPTS, results),
    assertEqual(Client, Items, Bucket,
                {<<"field2_int">>, 1, Items},
                ?Q_OPTS, results),
    timer:sleep(?Q_PAUSE),
    lager:info("2i test loop complete - ~w items found~n", [Items]),
    repeatedly_test_query(Client, Items, Bucket, N - 1, assert);
repeatedly_test_query(Client, Items, Bucket, N, report) ->
    reportIfEqual(Client, Items, Bucket,
                {<<"field1_bin">>, list_to_binary(?FOO), list_to_binary(?FOO ++ "z")},
                ?Q_OPTS, results),
    reportIfEqual(Client, Items, Bucket,
                {<<"field2_int">>, 1, Items},
                ?Q_OPTS, results),
    timer:sleep(?Q_PAUSE),
    repeatedly_test_query(Client, Items, Bucket, N - 1, report).

deploy_test_nodes(N) ->
    Config = [{riak_core, [{ring_creation_size, 8},
                           {handoff_acksync_threshold, 20},
                           {handoff_receive_timeout, 2000}]}],
    rt:deploy_nodes(N, Config).
