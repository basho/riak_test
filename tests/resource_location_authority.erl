%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
-module(resource_location_authority).
-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(P1, <<"colac">>).
-define(P2a, <<"done">>).
-define(P2b, <<"nedo">>).
-define(URL1, <<?P1/binary,"/",?P2a/binary>>).
-define(URL2, <<?P1/binary,"/",?P2b/binary>>).
-define(EPLIST1, [{"8.2.8.2", 1973}, {"8.8.2.2", 1793}, {"VIP.5", 77777, tcp}]).
-define(EPLIST2, [{"8.8.2.2", 1793}, {"VIP.9", 77777, tcp}]).

-define(BACKEND, riak_core_metadata_rla_backend).
%% this is the only 'canonical' backend currently offered, and the
%% principal one intended for production.

-define(ATTEMPTS, 20).
-define(BETWEEN_MSEC, 200).
%% 200 msec is just small enough for a testing two-node cluster to
%% sometimes elicit the metadata gossip lagging behind: a metadata
%% update effected on node1 sometimes does not get propagated to node2
%% within 200 msec, triggering a faulty read on node2.

confirm() ->
    [N1, N2] = rt:build_cluster(2),

    assert_with_patience({N1, riak_net_rla, get_backends, []}, []),
    assert_with_patience({N1, riak_net_rla, register_backend, [riak_core_metadata_rla_backend]}, ok),
    assert_with_patience({N1, riak_net_rla, get_backends, []}, [riak_core_metadata_rla_backend]),

    assert_with_patience({N2, riak_net_rla, get_backends, []}, []),
    assert_with_patience({N2, riak_net_rla, register_backend, [riak_core_metadata_rla_backend]}, ok),
    assert_with_patience({N2, riak_net_rla, get_backends, []}, [riak_core_metadata_rla_backend]),

    %% test_Op1_Op(Node1, Node2) means Op1 is performed on Node1,
    %% Op2, on Node2
    ok = test_register_lookup(N1, N1),
    ok = test_register_lookup(N2, N1),
    ok = test_register_lookup(N2, N2),

    ok = test_register_deregister(N1, N1),
    ok = test_register_deregister(N2, N1),
    ok = test_register_deregister(N2, N2),

    ok = test_register_list(N1, N1),
    ok = test_register_list(N2, N1),
    ok = test_register_list(N2, N2),

    ok = test_register_purge(N1, N1),
    ok = test_register_purge(N2, N1),
    ok = test_register_purge(N2, N2),

    assert_with_patience({N1, riak_net_rla, unregister_backend, [riak_core_metadata_rla_backend]}, ok),
    assert_with_patience({N2, riak_net_rla, unregister_backend, [riak_core_metadata_rla_backend]}, ok),

    pass.

test_register_lookup(Na, Nb) ->
    assert_with_patience({Na, ?BACKEND, register, [?URL1, ?EPLIST1]}, ok),
    assert_with_patience({Nb, riak_net_rla, lookup, [?URL1]}, {ok, ?EPLIST1}),
    lager:info(" passed register-lookup cycle on nodes ~p, ~p", [Na, Nb]),
    ok.

test_register_deregister(Na, Nb) ->
    assert_with_patience({Na, ?BACKEND, register, [?URL1, ?EPLIST1]}, ok),
    assert_with_patience({Nb, ?BACKEND, deregister, [?URL1]}, ok),
    assert_with_patience({Nb, riak_net_rla, lookup, [?URL1]}, {error, not_found}),
    assert_with_patience({Na, riak_net_rla, lookup, [?URL1]}, {error, not_found}),
    lager:info(" passed register-deregister cycle on nodes ~p, ~p", [Na, Nb]),
    ok.

test_register_list(Na, Nb) ->
    assert_with_patience({Na, ?BACKEND, register, [?URL1, ?EPLIST1]}, ok),
    assert_with_patience({Na, ?BACKEND, register, [?URL2, ?EPLIST2]}, ok),
    assert_with_patience({Nb, riak_net_rla, list, [?P1]}, {ok, [?P2a, ?P2b]}),
    assert_with_patience({Nb, ?BACKEND, deregister, [?URL1]}, ok),
    assert_with_patience({Nb, riak_net_rla, list, [?P1]}, {ok, [?P2b]}),
    lager:info(" passed register-list cycle on nodes ~p, ~p", [Na, Nb]),
    ok.

test_register_purge(Na, Nb) ->
    assert_with_patience({Na, ?BACKEND, register, [?URL1, ?EPLIST1]}, ok),
    assert_with_patience({Na, ?BACKEND, register, [?URL2, ?EPLIST2]}, ok),
    assert_with_patience({Nb, riak_net_rla, list, [?P1]}, {ok, [?P2a, ?P2b]}),
    assert_with_patience({Nb, ?BACKEND, purge, []}, ok),
    assert_with_patience({Nb, riak_net_rla, lookup, [?URL1]}, {error, not_found}),
    assert_with_patience({Nb, riak_net_rla, lookup, [?URL2]}, {error, not_found}),
    assert_with_patience({Na, riak_net_rla, lookup, [?URL1]}, {error, not_found}),
    assert_with_patience({Na, riak_net_rla, lookup, [?URL2]}, {error, not_found}),
    assert_with_patience({Nb, riak_net_rla, list, [?P1]}, {ok, []}),
    assert_with_patience({Na, riak_net_rla, list, [?P1]}, {ok, []}),
    lager:info(" passed register-list cycle on nodes ~p, ~p", [Na, Nb]),
    ok.


%% The main spot of bother in this test is to catch, and tolerate,
%% the lag of propagation of changes between nodes due to metadata
%% gossip.  Hence these timed calls machinery.

assert_with_patience(RpcSig, Expect) ->
    assert_with_patience_(RpcSig, Expect, ?ATTEMPTS, ?BETWEEN_MSEC, 0).
assert_with_patience_(_, _, Times, _, Times) ->
    ?assert(false);
assert_with_patience_({Node, Mod, Fun, Args} = RpcSig, Expect, Times, Inter, Attempt) ->
    case rpc:call(Node, Mod, Fun, Args) of
        Expect ->
            if Attempt > 1 ->
                    lager:info("attempt ~b/~b: ~p(~p) -> ~p, got it right",
                               [Attempt, Times, Fun, Args, Expect]);
               el/=se -> pass
            end,
            Attempt;
        SomethingElse ->
            lager:info("attempt ~b/~b: ~p(~p) -> ~p, expecting ~p",
                       [Attempt, Times, Fun, Args, SomethingElse, Expect]),
            timer:sleep(Inter),
            assert_with_patience_(RpcSig, Expect, Times, Inter, Attempt + 1)
    end.