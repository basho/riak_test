%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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

-module(ensemble_stats).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    NumNodes = 5,
    NVal = 5,
    Config = ensemble_util:fast_config(NVal),
    lager:info("Building cluster and waiting for ensemble to stablize"),
    Nodes = ensemble_util:build_cluster(NumNodes, Config, NVal),
    Node = hd(Nodes),

    lager:info("Creating/activating 'strong' bucket type"),
    rt:create_and_activate_bucket_type(Node, <<"strong">>,
                                       [{consistent, true}, {n_val, NVal}]),
    ensemble_util:wait_until_stable(Node, NVal),
    Bucket = {<<"strong">>, <<"test">>},
    Keys = [<<N:64/integer>> || N <- lists:seq(1,10)],

    PBC = rt:pbc(Node),

    lager:info("Read a key to verify 'notfound' doesn't get stats crashed"),
    rt:pbc_read_check(PBC, Bucket, hd(Keys), [{error, notfound}]),

    Stats0 = get_stats(Node),

    lager:info("Writing ~p consistent keys", [length(Keys)]),
    [ok = rt:pbc_write(PBC, Bucket, Key, Key) || Key <- Keys],

    lager:info("Read keys to verify they exist"),
    [rt:pbc_read(PBC, Bucket, Key) || Key <- Keys],

    Stats1 = get_stats(Node),

    lager:info("Verifying that the ensemble stats have valid values"),
    %% make sure the stats that were supposed to increment did
    verify_inc(Stats0, Stats1, [{<<"consistent_gets">>, 10},
                                {<<"consistent_gets_total">>, 10},
                                {<<"consistent_puts">>, 10},
                                {<<"consistent_puts_total">>, 10}]),

    %% verify that fsm times were tallied
    verify_nz(Stats1, [<<"consistent_get_time_mean">>,
                       <<"consistent_get_time_median">>,
                       <<"consistent_get_time_95">>,
                       <<"consistent_get_time_99">>,
                       <<"consistent_get_time_100">>,
                       <<"consistent_put_time_mean">>,
                       <<"consistent_put_time_median">>,
                       <<"consistent_put_time_95">>,
                       <<"consistent_put_time_99">>,
                       <<"consistent_put_time_100">>]),
    pass.

get_stats(Node) ->
    timer:sleep(10000),
    lager:info("Retrieving stats from node ~s", [Node]),
    StatsCommand = io_lib:format("curl -s -S ~s/stats", [rt:http_url(Node)]),
    lager:debug("Retrieving stats using command ~s", [StatsCommand]),
    StatString = os:cmd(StatsCommand),
    {struct, Stats} = mochijson2:decode(StatString),
    Stats.

verify_inc(Prev, Props, Keys) ->
    [begin
         Old = proplists:get_value(Key, Prev, 0),
         New = proplists:get_value(Key, Props, 0),
         lager:info("~s: ~p -> ~p (expected ~p)", [Key, Old, New, Old + Inc]),
         ?assertEqual(New, (Old + Inc))
     end || {Key, Inc} <- Keys].

verify_nz(Props, Keys) ->
    [?assertNotEqual(proplists:get_value(Key,Props,0), 0) || Key <- Keys].
