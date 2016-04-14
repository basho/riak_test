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
%% +-----------+    +--------+    +-----+
%% | beginning | -> | middle | -> | end |
%% +-----------+    +--------+    +-----+
%% -------------------------------------------------------------------

-module(rt_cascading_simple).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-record(simple_state, {
    beginning :: node(),
    middle :: node(),
    ending :: node()
}).

-define(bucket, <<"objects">>).

confirm() ->
    State = simple_setup(),
    simple_tests(State),
    pass.

simple_setup() ->
    [BeginCluster, MidCluster, EndCluster] = rt_cascading:make_clusters([
        {"beginning", 1},
        {"middle", 1},
        {"end", 1}
    ]),
    {_, [BeginNode]} = BeginCluster,
    {_, [MidNode]} = MidCluster,
    {_, [EndNode]} = EndCluster,
    #simple_state{beginning = BeginNode, middle = MidNode, ending = EndNode}.


simple_tests(State) ->
    Tests = [

        {"connecting Beginning to Middle", fun() ->
            Port = rt_cascading:get_cluster_mgr_port(State#simple_state.middle),
            repl_util:connect_cluster(State#simple_state.beginning, "127.0.0.1", Port),
            repl_util:enable_realtime(State#simple_state.beginning, "middle"),
            repl_util:start_realtime(State#simple_state.beginning, "middle")
                                           end},

        {"connection Middle to End", fun() ->
            Port = rt_cascading:get_cluster_mgr_port(State#simple_state.ending),
            repl_util:connect_cluster(State#simple_state.middle, "127.0.0.1", Port),
            repl_util:enable_realtime(State#simple_state.middle, "end"),
            repl_util:start_realtime(State#simple_state.middle, "end")
                                     end},

        {"cascade a put from beginning down to ending", fun() ->
            BeginningClient = rt:pbc(State#simple_state.beginning),
            Bin = <<"cascading realtime">>,
            Obj = riakc_obj:new(<<"objects">>, Bin, Bin),
            riakc_pb_socket:put(BeginningClient, Obj, [{w,1}]),
            riakc_pb_socket:stop(BeginningClient),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(State#simple_state.middle, <<"objects">>, Bin)),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(State#simple_state.ending, <<"objects">>, Bin))
                                                        end},

        {"disable cascading on middle", fun() ->
            rpc:call(State#simple_state.middle, riak_repl_console, realtime_cascades, [["never"]]),
            Bin = <<"disabled cascading">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            Client = rt:pbc(State#simple_state.beginning),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(State#simple_state.middle, ?bucket, Bin)),
            ?assertEqual({error, notfound}, rt_cascading:maybe_eventually_exists(State#simple_state.ending, ?bucket, Bin))

                                        end},

        {"re-enable cascading", fun() ->
            rpc:call(State#simple_state.middle, riak_repl_console, realtime_cascades, [["always"]]),
            Bin = <<"cascading re-enabled">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            Client = rt:pbc(State#simple_state.beginning),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(State#simple_state.middle, ?bucket, Bin)),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(State#simple_state.ending, ?bucket, Bin))
                                end},

        {"check pendings", fun() ->
            rt_cascading:wait_until_pending_count_zero([State#simple_state.middle,
                State#simple_state.beginning,
                State#simple_state.ending])
                           end}
    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== simple: ~s =====", [Name]),
        Eval()
                  end, Tests).
