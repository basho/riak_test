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
-module(verify_basic_upgrade).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(NUM_KEYS, 20000).

-define(CONFIG(RingSize, NVal), 
    [
        {riak_core,
            [
            {ring_creation_size, RingSize},
            {default_bucket_props,
                [
                    {n_val, NVal},
                    {allow_mult, true},
                    {dvv_enabled, true}
                ]}
            ]
        },
        {leveled,
            [
                {journal_objectcount, 2000} 
                % setting  low object count ensures we test rolled journal
                % files, not just active ones
            ]}
        ]).

confirm() ->
    
    TestMetaData = riak_test_runner:metadata(),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    [Nodes] = 
        rt:build_clusters([{4, OldVsn, ?CONFIG(8, 3)}]),
    [Node1|_] = Nodes,

    lager:info("Writing ~w keys to ~p", [?NUM_KEYS, Node1]),
    rt:systest_write(Node1, ?NUM_KEYS, 3),
    ?assertEqual([], rt:systest_read(Node1, ?NUM_KEYS, 1)),

    [upgrade(Node, current) || Node <- Nodes],

    %% Umm.. technically, it'd downgrade
    [upgrade(Node, OldVsn) || Node <- Nodes],
    pass.

upgrade(Node, NewVsn) ->
    lager:info("Upgrading ~p to ~p", [Node, NewVsn]),
    rt:upgrade(Node, NewVsn),
    rt:wait_for_service(Node, riak_kv),
    lager:info("Ensuring keys still exist"),
    ?assertEqual([], rt:systest_read(Node, ?NUM_KEYS, 1)),
    ok.
