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
%%        +-----+
%%        | top |
%%        +-----+
%%       /       \
%%      V         V
%% +------+   +-------+
%% | left |   | right |
%% +------+   +-------+
%%     |          |
%%     V          V
%% +-------+  +--------+
%% | left2 |  | right2 |
%% +-------+  +--------+
%% -------------------------------------------------------------------
-module(rt_cascading_pyramid).
-behavior(riak_test).

%% API
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    State = pyramid_setup(),
    _ = pyramid_tests(State),
    pass.

pyramid_setup() ->
    Conf = [{"top", 1}, {"left", 1}, {"left2", 1}, {"right", 1}, {"right2", 1}],
    Clusters = rt_cascading:make_clusters(Conf),
    GetPort = fun(Name) ->
        [Node] = proplists:get_value(Name, Clusters),
        rt_cascading:get_cluster_mgr_port(Node)
              end,
    [Top] = proplists:get_value("top", Clusters),
    [Left] = proplists:get_value("left", Clusters),
    [Right] = proplists:get_value("right", Clusters),
    rt_cascading:connect_rt(Top, GetPort("left"), "left"),
    rt_cascading:connect_rt(Left, GetPort("left2"), "left2"),
    rt_cascading:connect_rt(Top, GetPort("right"), "right"),
    rt_cascading:connect_rt(Right, GetPort("right2"), "right2"),
    lists:flatten([Nodes || {_, Nodes} <- Clusters]).

pyramid_tests(Nodes) ->
    Tests = [

        {"Cascade to both kids", fun() ->
            [Top | _] = Nodes,
            Client = rt:pbc(Top),
            Bucket = <<"objects">>,
            Bin = <<"pyramid_top">>,
            Obj = riakc_obj:new(Bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            lists:map(fun(N) ->
                ?debugFmt("Checking ~p", [N]),
                ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(N, Bucket, Bin))
                      end, Nodes)
                                 end},

        {"check pendings", fun() ->
            rt_cascading:wait_until_pending_count_zero(Nodes)
                           end}
    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== pyramid: ~s =====", [Name]),
        Eval()
                  end, Tests).
