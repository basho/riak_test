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
-module(verify_secondary_index_reformat).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

confirm() ->
    [Node] = rt_cluster:build_cluster([legacy]),
    rt:wait_until_nodes_ready([Node]),

    check_fixed_index_statuses(Node, undefined),

    TestBucket = <<"test">>,
    TestKey = <<"badindex">>,
    TestIndex = {integer_index, "foo"},
    TestIdxValue = 1362400142028,

    %% write key with index that old version of sext would encode improperly (not perserving
    %% sort order)
    lager:info("writing test key"),
    Client0 = rt_pb:pbc(Node),
    Obj0 = riakc_obj:new(TestBucket, TestKey, <<"somevalue">>),
    ObjMD0 = riakc_obj:get_update_metadata(Obj0),
    ObjMD1 = riakc_obj:set_secondary_index(ObjMD0,
                                           [{TestIndex, [TestIdxValue]}]),
    Obj1 = riakc_obj:update_metadata(Obj0, ObjMD1),
    ok = riakc_pb_socket:put(Client0, Obj1),

    %% upgrade node to version that supports reformatting
    rt:upgrade(Node, current),
    rt:wait_for_service(Node, riak_kv),

    %% some indexes have no data written and will be marked as fixed,
    %% others will not since there are invalid indexes
    check_fixed_index_statuses(Node, [true, false]),

    lager:info("reformatting indexes and verifying range query"),
    %% should rewrite 1 index (* n = 3), ignore 0 and have zero errors
    {3, 0, 0} = rpc:call(Node, riak_kv_util, fix_incorrect_index_entries, []),

    Client1 = rt_pb:pbc(Node),
    Results = riakc_pb_socket:get_index(Client1, TestBucket,
                                        TestIndex, 1000000000000,
                                        TestIdxValue),
    ?assertMatch({ok, #index_results_v1{}}, Results),
    {ok, ?INDEX_RESULTS{keys=ResultKeys}} = Results,
    lager:info("found keys: ~p", [ResultKeys]),
    ?assertEqual([TestKey], ResultKeys),

    check_fixed_index_statuses(Node, true),

    %% write some more data (make sure flag doesn't "roll back" on restart
    lager:info("writing some more data"),
    rt_systest:write(Node, 10, 1),

    lager:info("restarting node"),
    rt_node:stop_and_wait(Node),
    rt_node:start(Node),
    rt:wait_for_service(Node, riak_kv),

    check_fixed_index_statuses(Node, true),

    lager:info("rewriting indexes in old format to prepare for downgrade"),
    {3, 0, 0} = rpc:call(Node, riak_kv_util, fix_incorrect_index_entries, [[{downgrade, true}]]),

    check_fixed_index_statuses(Node, false),

    rt_node:stop_and_wait(Node),
    rt_node:start(Node),
    rt:wait_for_service(Node, riak_kv),
    check_fixed_index_statuses(Node, false),

    pass.

check_fixed_index_statuses(Node, E) when not is_list(E) ->
    check_fixed_index_statuses(Node, [E]);
check_fixed_index_statuses(Node, ExpectedStatuses) ->
    lager:info("Verifying fixed index status of ~p is one of ~p for all partitions",
               [Node, ExpectedStatuses]),
    Statuses = rpc:call(Node, riak_kv_status, vnode_status, []),
    BadIndexes = [{Idx, proplists:get_value(fixed_indexes, Status)} ||
                     {Idx, [{backend_status,_,Status}]} <- Statuses,
                     not fixed_index_status_ok(Status, ExpectedStatuses)],
    ?assertEqual([], BadIndexes),
    %% if we are checking for undefined then we are on old version that doesn't
    %% have riak_kv_status:fixed_index_status/0
    case lists:member(undefined, ExpectedStatuses) of
        false ->
            IncompleteIndexes = [Idx || {Idx, [{backend_status,_,Status}]} <- Statuses,
                                        fixed_index_status_ok(Status, [false])],
            RPCStatus = rpc:call(Node, riak_kv_status, fixed_index_status, []),
            case IncompleteIndexes of
                [] -> ?assert(RPCStatus);
                _ -> ?assertNot(RPCStatus)
            end;
        true ->
            ok
    end.

fixed_index_status_ok(Status, Expected) ->
    Found = proplists:get_value(fixed_indexes, Status),
    lists:member(Found, Expected).
