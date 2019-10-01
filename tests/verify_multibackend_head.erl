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
-module(verify_multibackend_head).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-define(DEFAULT_RING_SIZE, 16).

-define(CONF,
        [{riak_kv,
          [
           {anti_entropy, {off, []}}
          ]},
         {riak_core,
          [
           {ring_creation_size, ?DEFAULT_RING_SIZE}
          ]}]
       ).

confirm() ->
    lager:info("Overriding backend set in configuration"),
    lager:info("Multi backend with default settings (rt) to be used"),

    {Bucket, HeadSupport} = get_bucket(),

    rt:set_backend(riak_kv_multi_backend),
    [Node1] = rt:deploy_nodes(1, ?CONF),
    
    Stats1 = get_stats(Node1),
    
    C = rt:httpc(Node1),
    [rt:httpc_write(C, Bucket, <<X>>, <<"12345">>) || X <- lists:seq(1, 5)],
    [rt:httpc_read(C, Bucket, <<X>>) || X <- lists:seq(1, 5)],

    Stats2 = get_stats(Node1),

    ExpectedNodeStats = 
        case HeadSupport of
            true ->
                [{<<"node_gets">>, 10},
                    {<<"node_puts">>, 5},
                    {<<"node_gets_total">>, 10},
                    {<<"node_puts_total">>, 5},
                    {<<"vnode_gets">>, 5}, 
                        % The five PUTS will require only HEADs
                    {<<"vnode_heads">>, 30},
                        % There is no reduction in the count of HEADs
                        % as HEADS before GETs
                    {<<"vnode_puts">>, 15},
                    {<<"vnode_gets_total">>, 5},
                    {<<"vnode_heads_total">>, 30},
                    {<<"vnode_puts_total">>, 15}];
            false ->
                [{<<"node_gets">>, 10},
                    {<<"node_puts">>, 5},
                    {<<"node_gets_total">>, 10},
                    {<<"node_puts_total">>, 5},
                    {<<"vnode_gets">>, 30},
                    {<<"vnode_heads">>, 0},
                    {<<"vnode_puts">>, 15},
                    {<<"vnode_gets_total">>, 30},
                    {<<"vnode_heads_total">>, 0},
                    {<<"vnode_puts_total">>, 15}]
        end,

    %% make sure the stats that were supposed to increment did
    verify_inc(Stats1, Stats2, ExpectedNodeStats),

    pass.


get_bucket() ->
    Backend = proplists:get_value(backend, riak_test_runner:metadata()),
    lager:info("Running with backend ~p", [Backend]),
    {get_bucket(Backend), Backend == leveled}.

get_bucket(eleveldb) ->
    <<"eleveldb1">>;
get_bucket(bitcask) ->
    <<"bitcask1">>;
get_bucket(leveled) ->
    <<"leveled1">>;
get_bucket(memory) ->
    <<"memory1">>.

verify_inc(Prev, Props, Keys) ->
    [begin
         Old = proplists:get_value(Key, Prev, 0),
         New = proplists:get_value(Key, Props, 0),
         lager:info("~s: ~p -> ~p (expected ~p)", [Key, Old, New, Old + Inc]),
         ?assertEqual(New, (Old + Inc))
     end || {Key, Inc} <- Keys].

get_stats(Node) ->
    timer:sleep(10000),
    lager:info("Retrieving stats from node ~s", [Node]),
    StatsCommand = io_lib:format("curl -s -S ~s/stats", [rt:http_url(Node)]),
    lager:debug("Retrieving stats using command ~s", [StatsCommand]),
    StatString = os:cmd(StatsCommand),
    {struct, Stats} = mochijson2:decode(StatString),
    %%lager:debug(StatString),
    Stats.