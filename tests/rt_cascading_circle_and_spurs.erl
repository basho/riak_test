%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
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
%% Topology for this cascading replication test:
%%                        +------------+
%%                        | north_spur |
%%                        +------------+
%%                               ^
%%                               |
%%                           +-------+
%%                     +---> | north | ---+
%%                     |     +-------+    |
%%                     |                  V
%% +-----------+    +------+           +------+    +-----------+
%% | west_spur | <- | west | <-------- | east | -> | east_spur |
%% +-----------+    +------+           +------+    +-----------+
%% -------------------------------------------------------------------
-module(rt_cascading_circle_and_spurs).
-behavior(riak_test).

%% API
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    State = circle_and_spurs_setup(),
    _ = circle_and_spurs_tests(State),
    pass.

circle_and_spurs_setup() ->
    Config = [
        {"north", 1, ["east", "north_spur"]},
        {"east", 1, ["west", "east_spur"]},
        {"west", 1, ["north", "west_spur"]},
        {"north_spur", 1},
        {"east_spur", 1},
        {"west_spur", 1}
    ],
    Clusters = rt_cascading:make_clusters(Config),
    lists:flatten([Nodes || {_, Nodes} <- Clusters]).

circle_and_spurs_tests(Nodes) ->

    Tests = [

        {"start at north", fun() ->
            [North | _Rest] = Nodes,
            Client = rt:pbc(North),
            Bin = <<"start at north">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            [begin
                 ?debugFmt("Checking ~p", [N]),
                 ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(N, Bucket, Bin))
             end || N <- Nodes, N =/= North]
                           end},

        {"Start at west", fun() ->
            [_North, _East, West | _Rest] = Nodes,
            Client = rt:pbc(West),
            Bin = <<"start at west">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            [begin
                 ?debugFmt("Checking ~p", [N]),
                 ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(N, Bucket, Bin))
             end || N <- Nodes, N =/= West]
                          end},

        {"spurs don't replicate back", fun() ->
            [_North, _East, _West, NorthSpur | _Rest] = Nodes,
            Client = rt:pbc(NorthSpur),
            Bin = <<"start at north_spur">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            [begin
                 ?debugFmt("Checking ~p", [N]),
                 ?assertEqual({error, notfound}, rt_cascading:maybe_eventually_exists(N, Bucket, Bin))
             end || N <- Nodes, N =/= NorthSpur]
                                       end},

        {"check pendings", fun() ->
            rt_cascading:wait_until_pending_count_zero(Nodes)
                           end}

    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== circle_and_spurs: ~s =====", [Name]),
        Eval()
                  end, Tests).

