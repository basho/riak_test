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
%% @doc Verfication of sibling merges
%% See - https://github.com/basho/riak_kv/issues/1707

-module(verify_siblingmerge_basic).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_RING_SIZE, 8).
-define(CFG(),
        [{riak_kv,
          [
           % Speedy AAE configuration
           {anti_entropy, {off, []}},
           {tictacaae_active, passive}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).
-define(NUM_NODES, 4).
-define(AMT_BUCKET, <<"amt_bucket">>).
-define(AMF_BUCKET, <<"amf_bucket">>).
-define(N_VAL, 3).

confirm() ->
    C0 = rt:build_cluster(?NUM_NODES, ?CFG()),
    ok = rt:wait_until_nodes_agree_about_ownership(C0),
    ok = verify_siblingmerge(C0),
    pass.


verify_siblingmerge(Cluster) ->
    lager:info("Select two different nodes that are primaries"),
    {Node1A, Node2A} =
        select_two_different_primarynodes(Cluster, ?AMT_BUCKET, to_key(1)),

    lager:info("Testing allow_mult = true"),
    AMTopts = [{allow_mult, true}],
    ok = set_bucket(Node1A, ?AMT_BUCKET, AMTopts),
    ok = rt:wait_until_bucket_props(Cluster, ?AMT_BUCKET, AMTopts),

    ok = blind_write_data(Node1A, ?AMT_BUCKET, to_key(1), <<1:8/integer>>, []),
    ok = blind_write_data(Node2A, ?AMT_BUCKET, to_key(1), <<2:8/integer>>, []),

    ExpectedValsBoth = [<<1:8/integer>>, <<2:8/integer>>],
    test_replicas(Node1A, ?AMT_BUCKET, to_key(1), ExpectedValsBoth),

    lager:info("Testing allow_mult = false"),
    {Node1B, Node2B} =
        select_two_different_primarynodes(Cluster, ?AMF_BUCKET, to_key(1)),
    ok = set_bucket(Node1B, ?AMF_BUCKET, [{allow_mult, false}]),

    ok = blind_write_data(Node1B, ?AMF_BUCKET, to_key(1), <<1:8/integer>>, []),
    ok = blind_write_data(Node2B, ?AMF_BUCKET, to_key(1), <<2:8/integer>>, []),

    ExpectedValsLast = [<<2:8/integer>>],
    test_replicas(Node1B, ?AMF_BUCKET, to_key(1), ExpectedValsLast),

    ok.

select_two_different_primarynodes(Cluster, Bucket, Key) ->
    Chash = rpc:call(hd(Cluster), riak_core_util, chash_key, [{Bucket, Key}]),
    Pl = rpc:call(hd(Cluster),
                    riak_core_apl,
                    get_primary_apl,
                    [Chash, 3, riak_kv]),
    {{_P1, Node1}, primary} = lists:nth(1, Pl),
    {{_P2, Node2}, primary} = lists:nth(2, Pl),
    {Node1, Node2}.



test_replicas(Node, Bucket, Key, ExpectedVals) ->
    {ok, RObj1} = get_replica(Node, Bucket, Key, 1, ?N_VAL),
    {ok, RObj2} = get_replica(Node, Bucket, Key, 2, ?N_VAL),
    {ok, RObj3} = get_replica(Node, Bucket, Key, 3, ?N_VAL),
    ?assertMatch(ExpectedVals, riak_object:get_values(RObj1)),
    ?assertMatch(ExpectedVals, riak_object:get_values(RObj2)),
    ?assertMatch(ExpectedVals, riak_object:get_values(RObj3)).



to_key(N) ->
    list_to_binary(io_lib:format("K~6..0B", [N])).


blind_write_data(Node, B, Key, Value, Opts) ->
    PB = rt:pbc(Node),
    Obj = riakc_obj:new(B, Key, Value),
    ?assertMatch(ok, riakc_pb_socket:put(PB, Obj, Opts)),
    riakc_pb_socket:stop(PB),
    ok.

set_bucket(Node, B, Props) ->
    PB = rt:pbc(Node),
    riakc_pb_socket:set_bucket(PB, B, Props),
    riakc_pb_socket:stop(PB),
    ok.
    
% @doc Reads a single replica of a value. This issues a get command directly
% to the vnode handling the Nth primary partition of the object's preflist.
get_replica(Node, Bucket, Key, I, N) ->
    BKey = {Bucket, Key},
    Chash = rpc:call(Node, riak_core_util, chash_key, [BKey]),
    Pl = rpc:call(Node, riak_core_apl, get_apl, [Chash, N, riak_kv]),
    {Partition, PNode} = lists:nth(I, Pl),
    Ref = Reqid = make_ref(),
    Sender = {raw, Ref, self()},
    rpc:call(PNode, riak_kv_vnode, get,
             [{Partition, PNode}, BKey, Ref, Sender]),
    receive
        {Ref, {r, Result, _, Reqid}} ->
            Result;
        {Ref, Reply} ->
            Reply
    after
        60000 ->
            lager:error("Replica ~p get for ~p/~p timed out",
                        [I, Bucket, Key]),
            ?assert(false)
    end.