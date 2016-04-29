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
%%                      +-----+
%%     +--------------->| top |
%%     | loop added     +-----+
%%     |               /       \
%%     |              V         V
%%     |    +---------+         +----------+
%%     ^    | midleft |         | midright |
%%     |    +---------+         +----------+
%%     |               \       /
%%     |                V     V
%%     |               +--------+
%%     +-------<-------| bottom |
%%                     +--------+
%% -------------------------------------------------------------------
-module(rt_cascading_diamond).
-behavior(riak_test).

%% API
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    State = diamond_setup(),
    _ = diamond_tests(State),
    pass.

diamond_setup() ->
    Clusters = rt_cascading:make_clusters([{"top", 1}, {"midleft", 1}, {"midright", 1}, {"bottom", 1}]),
    GetNode = fun(Name) ->
        [N] = proplists:get_value(Name, Clusters),
        N
              end,
    GetPort = fun(Name) ->
        rt_cascading:get_cluster_mgr_port(GetNode(Name))
              end,
    rt_cascading:connect_rt(GetNode("top"), GetPort("midleft"), "midleft"),
    rt_cascading:connect_rt(GetNode("midleft"), GetPort("bottom"), "bottom"),
    rt_cascading:connect_rt(GetNode("midright"), GetPort("bottom"), "bottom"),
    rt_cascading:connect_rt(GetNode("top"), GetPort("midright"), "midright"),
    lists:flatten([Nodes || {_, Nodes} <- Clusters]).

diamond_tests(Nodes) ->
    Tests = [

        {"unfortunate double write", fun() ->
            [Top, MidLeft, MidRight, Bottom] = Nodes,
            Client = rt:pbc(Top),
            Bin = <<"start_at_top">>,
            Obj = riakc_obj:new(<<"objects">>, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            timer:sleep(100000),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(MidLeft, <<"objects">>, Bin)),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(MidRight, <<"objects">>, Bin)),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(Bottom, <<"objects">>, Bin)),
            %timer:sleep(1000),
            Status = rpc:call(Bottom, riak_repl2_rt, status, []),
            [SinkOne, SinkTwo] = proplists:get_value(sinks, Status, [[], []]),
            ?assertEqual(proplists:get_value(expect_seq, SinkOne), proplists:get_value(expect_seq, SinkTwo))
                                     end},

        {"connect bottom to top", fun() ->
            [Top, _MidLeft, _MidRight, Bottom] = Nodes,
            Port = rt_cascading:get_cluster_mgr_port(Top),
            rt_cascading:connect_rt(Bottom, Port, "top"),
            WaitFun = fun(N) ->
                Status = rpc:call(N, riak_repl2_rt, status, []),
                Sinks = proplists:get_value(sinks, Status, []),
                length(Sinks) == 1
                      end,
            ?assertEqual(ok, rt:wait_until(Top, WaitFun))
                                  end},

        {"start at midright", fun() ->
            [Top, MidLeft, MidRight, Bottom] = Nodes,
            % To ensure a write doesn't happen to MidRight when it originated
            % on midright, we're going to compare the expect_seq before and
            % after.
            Status = rpc:call(MidRight, riak_repl2_rt, status, []),
            [Sink] = proplists:get_value(sinks, Status, [[]]),
            ExpectSeq = proplists:get_value(expect_seq, Sink),

            Client = rt:pbc(MidRight),
            Bin = <<"start at midright">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            [begin
                 ?debugFmt("Checking ~p", [N]),
                 ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(N, Bucket, Bin))
             end || N <- [Bottom, Top, MidLeft]],

            Status2 = rpc:call(MidRight, riak_repl2_rt, status, []),
            [Sink2] = proplists:get_value(sinks, Status2, [[]]),
            GotSeq = proplists:get_value(expect_seq, Sink2),
            ?assertEqual(ExpectSeq, GotSeq)
                              end},

        {"check pendings", fun() ->
            rt_cascading:wait_until_pending_count_zero(Nodes)
                           end}

    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== diamond: ~s =====", [Name]),
        Eval()
                  end, Tests).
