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
-module(verify_vclock).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

%% We've got a separate test for capability negotiation and other mechanisms, so the test here is fairly
%% straightforward: get a list of different versions of nodes and join them into a cluster, making sure that
%% each time our data has been replicated:
confirm() ->
    NTestItems    = 10,                                     %% How many test items to write/verify?
    TestMode      = false,                                  %% Set to false for "production tests", true if too slow.
    EncodingTypes = [default, encode_raw, encode_zlib],     %% Usually, you won't want to fiddle with these.

    lists:foreach(fun(EncodingType) -> run_test(TestMode, NTestItems, EncodingType) end, EncodingTypes),

    lager:info("Test verify_vclock passed."),
    pass.

run_test(TestMode, NTestItems, VClockEncoding) ->

    lager:info("Testing vclock (encoding: ~p)", [VClockEncoding]),

    %% This resets nodes, cleans up stale directories, etc.:
    lager:info("Cleaning up..."),
    rt:setup_harness(dummy, dummy),

    lager:info("Spinning up test nodes"),
    [RootNode, TestNode0, TestNode1] = Nodes = deploy_test_nodes(TestMode, 3),

    %% First, exercise the default setting, then force known encodings and see if we get our data back. 
    try_encoding(RootNode,  default,     NTestItems),
    try_encoding(TestNode0, encode_raw,  NTestItems),
    try_encoding(TestNode1, encode_zlib, NTestItems),

stopall(Nodes),
lager:info("Test verify_vclock passed."),
pass.

try_encoding(TestNode, Encoding, NTestItems) -> 

    rt:wait_for_service(TestNode, riak_kv),
    force_encoding(TestNode, Encoding),

    our_pbc_write(TestNode, NTestItems),

    Results = our_pbc_read(TestNode, NTestItems),

    ?assertEqual(0, length(Results)).

force_encoding(Node, EncodingMethod) ->
    case EncodingMethod of
        default -> lager:info("Using default encoding type."), true;   

        _       -> lager:info("Forcing encoding type to ~p.", [EncodingMethod]),
                   OverrideData = 
                    [
                      { riak_kv, 
                            [ 
                                { override_capability,
                                        [ 
                                          { vclock_data_encoding,
                                               [ 
                                                  {    use, EncodingMethod},
                                                  { prefer, EncodingMethod} 
                                                ]
                                          } 
                                        ]
                                }
                            ]
                      }
                    ],

                   rt:update_app_config(Node, OverrideData)

    end.

stopall(Nodes) ->
    lists:foreach(fun(N) -> rt:brutal_kill(N) end, Nodes).

%% Unfortunately, the rt module's systest write/read doesn't wind up triggering a vclock, so
%% we need our own version:
our_pbc_write(Node, Size) ->
    our_pbc_write(Node, 1, Size, <<"systest">>).

our_pbc_write(Node, Start, End, Bucket) ->
    PBC = rt:pbc(Node),
    F = fun(N, Acc) ->
                try rt:pbc_write(PBC, Bucket, <<N:32/integer>>, <<N:32/integer>>) of
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


our_pbc_read(Node, Size) -> 
    our_pbc_read(Node, 1, Size, <<"systest">>).

our_pbc_read(Node, Start, End, Bucket) ->
    PBC = rt:pbc(Node),

    %% Trundle along through the list, collecting mismatches:
    F = fun(N, Acc) ->
        KV = <<N:32/integer>>,

        ResultValue = riakc_pb_socket:get(PBC, Bucket, KV),
        case ResultValue of
                   {ok, Obj} ->
                                   ObjectValue = riakc_obj:get_value(Obj),
                                   case ObjectValue of
                                    KV -> 
                                            Acc;
                                    WrongVal -> 
                                            [{N, {wrong_val, WrongVal}} | Acc]
                                   end;

                   {error, timeout} ->
                                   lager:error("timeout");
                   {error, disconnected} ->
                                   lager:error("disconnected");

                   Other ->
                        [{N, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

%% For some testing purposes, making these limits smaller is helpful:
deploy_test_nodes(false, N) -> 
    rt:deploy_nodes(N);
deploy_test_nodes(true,  N) ->
    lager:info("NOTICE: Using turbo settings for testing."),
    Config = [{riak_core, [{forced_ownership_handoff, 8},
                           {handoff_concurrency, 8},
                           {vnode_inactivity_timeout, 1000},
                           {gossip_limit, {10000000, 60000}}]}],
    rt:deploy_nodes(N, Config).

