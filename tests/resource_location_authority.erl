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

-define(ATTEMPTS, 20).
-define(BETWEEN_MSEC, 200).

confirm() ->
    [N1, N2] = rt:build_cluster(2),

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

    pass.

test_register_lookup(Na, Nb) ->
    lager:info("testing RLA register-lookup cycle on nodes ~p, ~p", [Na, Nb]),
    assert_with_patience([Na, register, [?URL1, ?EPLIST1]], ok),
    assert_with_patience([Nb, lookup, [?URL1]], {ok, ?EPLIST1}),
    lager:info("RLA register-lookup cycle passed on nodes ~p, ~p", [Na, Nb]),
    ok.

test_register_deregister(Na, Nb) ->
    lager:info("testing RLA register-deregister cycle on nodes ~p, ~p", [Na, Nb]),
    assert_with_patience([Na, register, [?URL1, ?EPLIST1]], ok),
    assert_with_patience([Nb, deregister, [?URL1]], ok),
    assert_with_patience([Nb, lookup, [?URL1]], {error, not_found}),
    assert_with_patience([Na, lookup, [?URL1]], {error, not_found}),
    lager:info("RLA register-deregister cycle passed on nodes ~p, ~p", [Na, Nb]),
    ok.

test_register_list(Na, Nb) ->
    lager:info("testing RLA register-list cycle on nodes ~p, ~p", [Na, Nb]),
    assert_with_patience([Na, register, [?URL1, ?EPLIST1]], ok),
    assert_with_patience([Na, register, [?URL2, ?EPLIST2]], ok),
    assert_with_patience([Nb, list, [?P1]], {ok, [?P2a, ?P2b]}),
    assert_with_patience([Nb, deregister, [?URL1]], ok),
    assert_with_patience([Nb, list, [?P1]], {ok, [?P2b]}),
    lager:info("RLA register-list cycle passed on nodes ~p, ~p", [Na, Nb]),
    ok.

test_register_purge(Na, Nb) ->
    lager:info("testing RLA register-list cycle on nodes ~p, ~p", [Na, Nb]),
    assert_with_patience([Na, register, [?URL1, ?EPLIST1]], ok),
    assert_with_patience([Na, register, [?URL2, ?EPLIST2]], ok),
    assert_with_patience([Nb, list, [?P1]], {ok, [?P2a, ?P2b]}),
    assert_with_patience([Nb, purge, []], ok),
    assert_with_patience([Nb, lookup, [?URL1]], {error, not_found}),
    assert_with_patience([Nb, lookup, [?URL2]], {error, not_found}),
    assert_with_patience([Na, lookup, [?URL1]], {error, not_found}),
    assert_with_patience([Na, lookup, [?URL2]], {error, not_found}),
    assert_with_patience([Nb, list, [?P1]], {ok, []}),
    assert_with_patience([Na, list, [?P1]], {ok, []}),
    lager:info("RLA register-list cycle passed on nodes ~p, ~p", [Na, Nb]),
    ok.


%% The main spot of bother in this test is to catch the initial
%% latency between RLA coming up as a service, and the moment it
%% starts responding to client requests with proper operational values
%% other than not_ready.  Hence these timed calls machinery.

assert_with_patience(RpcSig, Expect) ->
    assert_with_patience_(RpcSig, Expect, ?ATTEMPTS, ?BETWEEN_MSEC, 0).
assert_with_patience_(_, _, Times, _, Times) ->
    ?assert(false);
assert_with_patience_([Node, Fun, Args] = RpcSig, Expect, Times, Inter, Attempt) ->
    case rpc:call(Node, riak_net_rla, Fun, Args) of
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
