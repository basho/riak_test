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
%%      +-----+
%%      | one |
%%      +-----+
%%      ^      \
%%     /        V
%% +-------+    +-----+
%% | three | <- | two |
%% +-------+    +-----+
%% -------------------------------------------------------------------
-module(rt_cascading_circle).
-behavior(riak_test).

%% API
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    State = circle_setup(),
    _ = circle_tests(State),
    pass.

circle_setup() ->
    Conf = [{"one", 1}, {"two", 1}, {"three", 1}],
    Clusters = rt_cascading:make_clusters(Conf),
    [[One], [Two], [Three]] = Unflattened = [ClusterNodes || {_Name, ClusterNodes} <- Clusters],

    Connections = [
        {One, Two, "two"},
        {Two, Three, "three"},
        {Three, One, "one"}
    ],
    ok = lists:foreach(fun({Node, ConnectNode, Name}) ->
        Port = rt_cascading:get_cluster_mgr_port(ConnectNode),
        rt_cascading:connect_rt(Node, Port, Name)
                       end, Connections),
    lists:flatten(Unflattened).

circle_tests(Nodes) ->
    %      +-----+
    %      | one |
    %      +-----+
    %      ^      \
    %     /        V
    % +-------+    +-----+
    % | three | <- | two |
    % +-------+    +-----+
    Tests = [

        {"cascade all the way to the other end, but no further", fun() ->
            Client = rt:pbc(hd(Nodes)),
            Bin = <<"cascading">>,
            Obj = riakc_obj:new(<<"objects">>, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(lists:last(Nodes), <<"objects">>, Bin)),
            % we want to ensure there's not a cascade back to the beginning, so
            % there's no event we can properly wait for. All we can do is wait
            % and make sure we didn't update/write the object.
            timer:sleep(1000),
            Status = rpc:call(hd(Nodes), riak_repl2_rt, status, []),
            [SinkData] = proplists:get_value(sinks, Status, [[]]),
            ?assertEqual(undefined, proplists:get_value(expect_seq, SinkData))
                                                                 end},

        {"cascade starting at a different point", fun() ->
            [One, Two | _] = Nodes,
            Client = rt:pbc(Two),
            Bin = <<"start_at_two">>,
            Obj = riakc_obj:new(<<"objects">>, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(One, <<"objects">>, Bin)),
            timer:sleep(1000),
            Status = rpc:call(Two, riak_repl2_rt, status, []),
            [SinkData] = proplists:get_value(sinks, Status, [[]]),
            ?assertEqual(2, proplists:get_value(expect_seq, SinkData))
                                                  end},

        {"check pendings", fun() ->
            rt_cascading:wait_until_pending_count_zero(Nodes)
                           end}
    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== circle: ~s =====", [Name]),
        Eval()
                  end, Tests).
