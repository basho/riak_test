%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
-module(kv_vnode_requests_upgrade_downgrade).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-define(NUM_KEYS, 100).
-define(BUCKET, <<"ale">>).
-define(CLUSTER_SIZE, 5).
-define(CONFIG, []).

confirm() ->
    Cluster = [Node| _ ] = rt:build_cluster(lists:duplicate(?CLUSTER_SIZE, {lts, ?CONFIG})),
    Clients = [rt:pbc(N) || N <- Cluster],

    lager:info("Writing ~p keys", [?NUM_KEYS]),
    rt:systest_write(Node, ?NUM_KEYS, ?BUCKET),

    Before = count_keys(Clients, ?BUCKET),
    ExpectedCounts = lists:duplicate(?CLUSTER_SIZE, ?NUM_KEYS),
    ?assertEqual(Before, ExpectedCounts),

    perform_upgrade(Cluster, current, 3),

    After = count_keys(Clients, ?BUCKET),
    ?assertEqual(Before, After),
    pass.

count_keys(Clients, Bucket) when is_list(Clients) ->
    [count_keys(Client, Bucket) || Client <- Clients];
count_keys(Client, Bucket) ->
    {ok, Keys} = riakc_pb_socket:list_keys(Client, Bucket, 5000),
    length(Keys).

perform_upgrade(Node, Version) ->
    lager:info("Upgrading node ~p", [Node]),
    rt:upgrade(Node, Version),
    lager:info("Upgrade finished on node ~p", [Node]),
    rt:wait_for_service(Node, riak_kv).
perform_upgrade(Cluster, Version, TakeN) ->
    lists:foreach(fun(Node) -> perform_upgrade(Node, Version) end,
                  lists:sublist(Cluster, TakeN)).
