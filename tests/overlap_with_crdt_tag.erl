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
-module(overlap_with_crdt_tag).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

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
    Items    = 500, %% How many test items in each group to write/verify?
    run_test(Items, 4),

    lager:info("Test overlap_with_crdt_tag passed."),
    pass.

run_test(Items, NTestNodes) ->
    lager:info("Testing handoff (items ~p, nodes: ~p)", [Items, NTestNodes]),

    lager:info("Spinning up test nodes"),
    [RootNode, FirstJoin, SecondJoin, LastJoin] = Nodes = 
        deploy_test_nodes(NTestNodes),

    rt:wait_for_service(RootNode, riak_kv),

    set_handoff_encoding(default, Nodes),

    lager:info("Initialise bucket type."),
    CRDT_Tag = <<69:8/integer>>,
    Other_Tag = <<255:8/integer>>,
    BProps = [{allow_mult, true}, {last_write_wins, false},
                {node_confirms, 1}, {dvv_enabled, true}],
    B1 = {<<"type1">>, <<"B1">>},
    B2 = <<"B2">>,
    {ok, C} = riak:client_connect(RootNode),
    ok = riak_client:set_bucket(B2, BProps, C),
    ok = rt:create_and_activate_bucket_type(RootNode, <<"type1">>, BProps),

    lager:info("Populating initial data."),
    R1A = systest_write_binary(RootNode, 1, Items, B1, 3, CRDT_Tag),
    R2A = systest_write_binary(RootNode, Items + 1, Items * 2, B2, 3, CRDT_Tag),
    R3A = systest_write_binary(RootNode, Items * 2 + 1, Items * 3, B1, 3, CRDT_Tag),

    ?assertEqual([], R1A),
    ?assertEqual([], R2A),
    ?assertEqual([], R3A),

    lager:info("Waiting for service on second node."),
    rt:wait_for_service(FirstJoin, riak_kv),

    lager:info("Joining new node with cluster."),
    rt:join(FirstJoin, RootNode),
    check_joined([RootNode, FirstJoin]),
    lager:info("Handoff complete"),

    lager:info("Validating data - no siblings"),
    systest_read_binary(FirstJoin, 1, Items, B1, 3, CRDT_Tag, false),
    systest_read_binary(FirstJoin, Items + 1, Items * 2, B2, 3, CRDT_Tag, false),
    systest_read_binary(FirstJoin, Items * 2 + 1, Items * 3, B1, 1, CRDT_Tag, false),
    
    lager:info("Populating sibling data"),
    R1B = systest_write_binary(RootNode, 1, Items, B1, 3, Other_Tag),
    R2B = systest_write_binary(RootNode, Items + 1, Items * 2, B2, 3, Other_Tag),
    R3B = systest_write_binary(RootNode, Items * 2 + 1, Items * 3, B1, 3, Other_Tag),

    ?assertEqual([], R1B),
    ?assertEqual([], R2B),
    ?assertEqual([], R3B),

    lager:info("Waiting for service on third node."),
    rt:wait_for_service(SecondJoin, riak_kv),

    lager:info("Joining new node with cluster."),
    rt:join(SecondJoin, RootNode),
    check_joined([RootNode, FirstJoin, SecondJoin]),
    lager:info("Handoff complete"),

    lager:info("Validating data - siblings"),
    systest_read_binary(SecondJoin, 1, Items, B1, 3, CRDT_Tag, true),
    systest_read_binary(SecondJoin, Items + 1, Items * 2, B2, 3, CRDT_Tag, true),
    systest_read_binary(SecondJoin, Items * 2 + 1, Items * 3, B1, 1, CRDT_Tag, true),

    lager:info("Populating more sibling data"),
    R1C = systest_write_binary(RootNode, 1, Items, B1, 3, CRDT_Tag),
    R2C = systest_write_binary(RootNode, Items + 1, Items * 2, B2, 3, CRDT_Tag),
    R3C = systest_write_binary(RootNode, Items * 2 + 1, Items * 3, B1, 3, CRDT_Tag),

    ?assertEqual([], R1C),
    ?assertEqual([], R2C),
    ?assertEqual([], R3C),

    lager:info("Waiting for service on final node."),
    rt:wait_for_service(LastJoin, riak_kv),

    lager:info("Joining new node with cluster."),
    rt:join(LastJoin, RootNode),
    check_joined([RootNode, FirstJoin, SecondJoin, LastJoin]),
    lager:info("Handoff complete"),

    lager:info("Validating data - siblings"),
    systest_read_binary(LastJoin, 1, Items, B1, 3, CRDT_Tag, true),
    systest_read_binary(LastJoin, Items + 1, Items * 2, B2, 3, CRDT_Tag, true),
    systest_read_binary(LastJoin, Items * 2 + 1, Items * 3, B1, 1, CRDT_Tag, true),


    %% Prepare for the next call to our test (we aren't polite about it, it's faster that way):
    lager:info("Bringing down test nodes."),
    lists:foreach(fun(N) -> rt:brutal_kill(N) end, Nodes).

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

%% For some testing purposes, making these limits smaller is helpful:
deploy_test_nodes(N) ->
    Config =
        [{riak_core,
            [{ring_creation_size, 16},
            {handoff_acksync_threshold, 1},
            {handoff_receive_timeout, 30000}]},
        {riak_kv,
            [{read_repair_log, true}]}],
    rt:deploy_nodes(N, Config).

systest_write_binary(Node, Start, End, Bucket, W, CommonValBin)
                                                when is_binary(CommonValBin) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    F = fun(N, Acc) ->
                Obj = riak_object:new(Bucket,
                                        <<N:32/integer>>,
                                        <<CommonValBin/binary, N:32/integer>>),
                try riak_client:put(Obj, W, C) of
                    ok ->
                        Acc;
                    Other ->
                        [{N, Other} | Acc]
                catch
                    What:Why ->
                        [{N, {What, Why}} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

systest_read_binary(Node, Start, End, Bucket, R, CommonValBin, ExpectSiblings)
                                                    when is_binary(CommonValBin) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    lists:foreach(systest_read_fold_fun(C, Bucket, R, CommonValBin, ExpectSiblings),
                    lists:seq(Start, End)).

systest_read_fold_fun(C, Bucket, R, CommonValBin, ExpectSiblings) ->
    fun(N) ->
        RObj =
            case riak_client:get(Bucket, <<N:32/integer>>, R, C) of
                {ok, Obj} ->
                    Obj;
                {error, notfound} ->
                    lager:warning(
                        "nofound B=~w K=~w", [Bucket, N]
                    ),
                    notfound
            end,
        check_value(RObj, ExpectSiblings, N, CommonValBin)
    end.

check_value(Obj, ExpectSiblings, N, CommonValBin) ->
    Val = 
        case ExpectSiblings of
            false ->
                riak_object:get_value(Obj);
            true ->
                Contents = riak_object:get_contents(Obj),
                HaveDotFun =
                    fun({MD, V}, Acc) ->
                        {ok, _DV} = dict:find(<<"dot">>, MD),
                        LastMod = dict:fetch(<<"X-Riak-Last-Modified">>, MD),
                        [{LastMod, V}|Acc]
                    end,
                Vs = lists:foldl(HaveDotFun, [], Contents),
                [{_ModDate, FirstV}|_OtherVs] = lists:usort(Vs),
                FirstV
        end,
    ?assertEqual(<<CommonValBin/binary, N:32/integer>>, Val).

check_joined(Nodes) ->
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_no_pending_changes(Nodes)),
    ?assertEqual(ok, rt:wait_until_nodes_agree_about_ownership(Nodes)).