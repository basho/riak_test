%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
%%% @doc r_t to verify CRDTs and counter values can properly upgrade
%%%      1.4-2.0 verifies that 1.4 counters can work with types
%%%      Currently, this is a mainly placeholder for future 2.0+ tests
-module(verify_dt_upgrade).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(COUNTER_BUCKET, <<"cbucket">>).

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    Nodes = [Node1|_] = rt:build_cluster([OldVsn, OldVsn, OldVsn, OldVsn]),

    verify_counter_converge:set_allow_mult_true(Nodes, ?COUNTER_BUCKET),
    populate_counters(Node1),

    [begin
         verify_counters(Node),
         upgrade(Node, current)
     end || Node <- Nodes],

    verify_counters(Node1),
    pass.

%% @private

%% @doc populate a counter via http and pbc
populate_counters(Node) ->
    lager:info("Writing counters to ~p", [Node]),
    rt:wait_for_service(Node, riak_kv),
    ?assertEqual(ok, rt:wait_until_capability(Node, {riak_kv, crdt}, [pncounter])),

    RHC = rt:httpc(Node),
    ?assertMatch(ok, rhc:counter_incr(RHC, ?COUNTER_BUCKET, <<"httpkey">>, 2)),
    ?assertMatch({ok, 2}, rhc:counter_val(RHC, ?COUNTER_BUCKET, <<"httpkey">>)),

    PBC = rt:pbc(Node),
    ?assertEqual(ok, riakc_pb_socket:counter_incr(PBC, ?COUNTER_BUCKET, <<"pbkey">>, 4)),
    ?assertEqual({ok, 4}, riakc_pb_socket:counter_val(PBC, ?COUNTER_BUCKET, <<"pbkey">>)),
    ok.

%% @doc check that the counter values exist after upgrade, and
%%      check that you can get via default bucket
verify_counters(Node) ->
    lager:info("Verifying counters on ~p", [Node]),
    RHC = rt:httpc(Node),
    ?assertMatch({ok, 4}, rhc:counter_val(RHC, ?COUNTER_BUCKET, <<"pbkey">>)),

    PBC = rt:pbc(Node),
    ?assertEqual({ok, 2}, riakc_pb_socket:counter_val(PBC, ?COUNTER_BUCKET, <<"httpkey">>)),

    %% Check that 1.4 counters work with bucket types
    case catch rt:capability(Node, {riak_core, bucket_types}) of
        true ->
            ?assertEqual({ok, {counter, 4, 0}}, riakc_pb_socket:fetch_type(PBC, {<<"default">>, ?COUNTER_BUCKET}, <<"pbkey">>));
        _ ->
            ok
    end,
    ok.

upgrade(Node, NewVsn) ->
    lager:info("Upgrading ~p to ~p", [Node, NewVsn]),
    rt:upgrade(Node, NewVsn),
    rt:wait_for_service(Node, riak_kv),
    ok.
