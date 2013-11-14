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
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2012, Basho Technologies
%%% @doc
%%% test for gh679, removing the backend data does not result in lost writes
%%% @end

-module(verify_frontier_vclock).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"gh679">>).
-define(KEY, ?BUCKET).

confirm() ->
    [N1, _N2, _N3]=Nodes = rt:build_cluster(3),

    %% Find out which node will be the primary for this data
    Preflist = get_preflist(N1),
    lager:info("Preflist for data is ~p", [Preflist]),

    %% Write some data to a primary with PW=3 (want to be sure that it is propogated)
    lager:info("Nodes are ~p", [Nodes]),

    set_allow_mult_true(Nodes),

    C = rt:pbc(N1),

    RObj = riakc_obj:new(?BUCKET, ?KEY, <<"1">>),
    ok = riakc_pb_socket:put(C, RObj, [{pw, 3}]),

    %% get the data, check it's vclock and be sure the write was co-ordinated as expected
    {ok, ROBj2} = riakc_pb_socket:get(C, ?BUCKET, ?KEY, [{pr, 3}]),
    VClock = riakc_obj:vclock(ROBj2),

    lager:info("Vclock ~p", [binary_to_term(zlib:unzip(VClock))]),

    %% repeat, for certitude
    RObj3 = riakc_obj:update_value(ROBj2, <<"2">>),

    ok = riakc_pb_socket:put(C, RObj3, [{pw, 3}]),
    {ok, ROBj4} = riakc_pb_socket:get(C, ?BUCKET, ?KEY, [{pr, 3}]),
    VClock2 = riakc_obj:vclock(ROBj4),

    lager:info("Vclock ~p", [binary_to_term(zlib:unzip(VClock2))]),

    Coordinator = get_coordinator(VClock2, Nodes),

    lager:info("Coordinator was ~p", [Coordinator]),
    %% Remove the bitcask directory for the data from the co-ordinating node
    remove_cordinator_bitask_dir(Preflist, Coordinator),

    C1 = rt:pbc(N1),

    %% Write some new data to the co-ordinating node
    RObj5 = riakc_obj:new(?BUCKET, ?KEY, <<"3">>),

    ok = riakc_pb_socket:put(C1, RObj5, [{pw, 3}]),

    %% Get it, it may have dissappeared.
    {ok, ROBj6} = riakc_pb_socket:get(C1, ?BUCKET, ?KEY, [{pr, 3}]),
    VClock3 = riakc_obj:vclock(ROBj6),

    lager:info("Vclock ~p", [binary_to_term(zlib:unzip(VClock3))]),
    lager:info("Value ~p", [riakc_obj:get_value(ROBj6)]),

    %% What do we expect? Well, since this last write was without a
    %% vclock and allow_mult is true, I expect 2 values, <<"2">> and
    %% <<"3">>, I don't expect a dropped write.
    ?assertEqual(2, riakc_obj:value_count(ROBj6)),
    ?assertEqual([<<"2">>, <<"3">>], lists:sort(riakc_obj:get_values(ROBj6))),

    pass.


get_preflist(N) ->
    DocIdx = rpc:call(N, riak_core_util, chash_key, [{?BUCKET, ?KEY}]),
    rpc:call(N, riak_core_apl, get_primary_apl, [DocIdx, 3, riak_kv]).

get_coordinator(VClock, Nodes) ->
    [{NodeId0, _}] = binary_to_term(zlib:unzip(VClock)),
    <<_Epoch:32/integer, DiEdon/binary>> =  binary:list_to_bin(
                                              lists:reverse(
                                                binary:bin_to_list(NodeId0))),
    NodeId =  binary:list_to_bin(
                lists:reverse(binary:bin_to_list(DiEdon))),
    IDs = [begin
               Id = erlang:crc32(term_to_binary(N)),
               <<Id:32/unsigned-integer>>
           end || N <- Nodes],
    lager:info("NodeId ~p", [NodeId]),
    NodeAndId = lists:zip(IDs, Nodes),
    lager:info("Nodes and their ids ~p", [NodeAndId]),
    proplists:get_value(NodeId, NodeAndId).

remove_cordinator_bitask_dir(Preflist, Coordinator) ->
    lager:info("ooh ~p ~p", [Preflist, Coordinator]),
    IdxNode = [IN || {IN, primary} <- Preflist],
    {Part, Coordinator} = lists:keyfind(Coordinator, 2, IdxNode),
    lager:info("need to delete ~p", [Part]),
    DataDir0 = rpc:call(Coordinator, app_helper, get_env, [riak_core, platform_data_dir]),
    DataDir = filename:join([DataDir0, "bitcask", integer_to_list(Part)]),
    {ok, Files} = rpc:call(Coordinator, file, list_dir, [DataDir]),
    lager:info("Need to delete ~p", [Files]),
    [ok = rpc:call(Coordinator, file, delete, [filename:join(DataDir, F)]) || F <- Files],
    rpc:call(Coordinator, file, del_dir, [DataDir]),
    rt:stop_and_wait(Coordinator),
    rt:start_and_wait(Coordinator).

set_allow_mult_true(Nodes) ->
    N1 = hd(Nodes),
    AllowMult = [{allow_mult, true}],
    lager:info("Setting bucket properties ~p for bucket ~p on node ~p",
               [AllowMult, ?BUCKET, N1]),
    rpc:call(N1, riak_core_bucket, set_bucket, [?BUCKET, AllowMult]),
    rt:wait_until_ring_converged(Nodes).


