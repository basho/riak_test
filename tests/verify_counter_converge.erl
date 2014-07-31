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
%%% riak_test for riak_dt counter convergence,
%%% @end

-module(verify_counter_converge).
-behavior(riak_test).
-export([confirm/0, set_allow_mult_true/1, set_allow_mult_true/2]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"test-counters">>).

confirm() ->
    Key = <<"a">>,

    [N1, N2, N3, N4]=Nodes = rt_cluster:build_cluster(4),
    [C1, C2, C3, C4]=Clients =  [ rt_http:httpc(N) || N <- Nodes ],

    set_allow_mult_true(Nodes),

    increment_counter(C1, Key),
    increment_counter(C2, Key, 10),

    [?assertEqual(11, get_counter(C, Key)) || C <- Clients],

    decrement_counter(C3, Key),
    decrement_counter(C4, Key, 2),

    [?assertEqual(8, get_counter(C, Key)) || C <- Clients],

    lager:info("Partition cluster in two."),

    PartInfo = rt_node:partition([N1, N2], [N3, N4]),

    %% increment one side
    increment_counter(C1, Key, 5),

    %% check value on one side is different from other
    [?assertEqual(13, get_counter(C, Key)) || C <- [C1, C2]],
    [?assertEqual(8, get_counter(C, Key)) || C <- [C3, C4]],

    %% decrement other side
    decrement_counter(C3, Key, 2),

    %% verify values differ
    [?assertEqual(13, get_counter(C, Key)) || C <- [C1, C2]],
    [?assertEqual(6, get_counter(C, Key)) || C <- [C3, C4]],

    %% heal
    lager:info("Heal and check merged values"),
    ok = rt_node:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, riak_kv),

    %% verify all nodes agree
    [?assertEqual(ok, rt:wait_until(fun() ->
                                            11 == get_counter(HP, Key)
                                    end)) ||  HP <- Clients ],

    pass.

set_allow_mult_true(Nodes) ->
    set_allow_mult_true(Nodes, ?BUCKET).

set_allow_mult_true(Nodes, Bucket) ->
    %% Counters REQUIRE allow_mult=true
    N1 = hd(Nodes),
    AllowMult = [{allow_mult, true}],
    lager:info("Setting bucket properties ~p for bucket ~p on node ~p",
               [AllowMult, Bucket, N1]),
    rpc:call(N1, riak_core_bucket, set_bucket, [Bucket, AllowMult]),
    rt:wait_until_ring_converged(Nodes).

%% Counter API
get_counter(Client, Key) ->
    {ok, Val} = rhc:counter_val(Client, ?BUCKET, Key),
    Val.

increment_counter(Client, Key) ->
    increment_counter(Client, Key, 1).

increment_counter(Client, Key, Amt) ->
    update_counter(Client, Key, Amt).

decrement_counter(Client, Key) ->
    decrement_counter(Client, Key, 1).

decrement_counter(Client, Key, Amt) ->
    update_counter(Client, Key, -Amt).

update_counter(Client, Key, Amt) ->
    rhc:counter_incr(Client, ?BUCKET, Key, Amt).
