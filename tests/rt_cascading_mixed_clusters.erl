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
%%      | n12 |
%%      +-----+
%%      ^      \
%%     /        V
%% +-----+    +-----+
%% | n56 | <- | n34 |
%% +-----+    +-----+
%%
%% This test is configurable for 1.3 versions of Riak, but off by default.
%% place the following config in ~/.riak_test_config to run:
%%
%% {run_rt_cascading_1_3_tests, true}
%% -------------------------------------------------------------------
-module(rt_cascading_mixed_clusters).
-behavior(riak_test).

%% API
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(bucket, <<"objects">>).

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    case rt_config:config_or_os_env(run_rt_cascading_1_3_tests, false) of
        false ->
            lager:info("mixed_version_clusters_test_ not configured to run!");
        _ ->
            State = mixed_version_clusters_setup(),
            _ = mixed_version_clusters_tests(State)
    end,
    pass.

mixed_version_clusters_setup() ->
    Conf = rt_cascading:conf(),
    DeployConfs = [{previous, Conf} || _ <- lists:seq(1,6)],
    Nodes = rt:deploy_nodes(DeployConfs),
    [N1, N2, N3, N4, N5, N6] =  Nodes,
    case rpc:call(N1, application, get_key, [riak_core, vsn]) of
        % this is meant to test upgrading from early BNW aka
        % Brave New World aka Advanced Repl aka version 3 repl to
        % a cascading realtime repl. Other tests handle going from pre
        % repl 3 to repl 3.
        {ok, Vsn} when Vsn < "1.3.0" ->
            {too_old, Nodes};
        _ ->
            N12 = [N1, N2],
            N34 = [N3, N4],
            N56 = [N5, N6],
            repl_util:make_cluster(N12),
            repl_util:make_cluster(N34),
            repl_util:make_cluster(N56),
            repl_util:name_cluster(N1, "n12"),
            repl_util:name_cluster(N3, "n34"),
            repl_util:name_cluster(N5, "n56"),
            [repl_util:wait_until_leader_converge(Cluster) || Cluster <- [N12, N34, N56]],
            rt_cascading:connect_rt(N1, rt_cascading:get_cluster_mgr_port(N3), "n34"),
            rt_cascading:connect_rt(N3, rt_cascading:get_cluster_mgr_port(N5), "n56"),
            rt_cascading:connect_rt(N5, rt_cascading:get_cluster_mgr_port(N1), "n12"),
            Nodes
    end.

mixed_version_clusters_tests({too_old, _Nodes}) ->
    ok;

mixed_version_clusters_tests(Nodes) ->

    [N1, N2, N3, N4, N5, N6] = Nodes,
    Tests = [

        {"no cascading at first 1", fun() ->
            Client = rt:pbc(N1),
            Bin = <<"no cascade yet">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 2}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual({error, notfound}, rt_cascading:maybe_eventually_exists([N5, N6], ?bucket, Bin)),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists([N3, N4], ?bucket, Bin))
                                    end},

        {"no cascading at first 2", fun() ->
            Client = rt:pbc(N2),
            Bin = <<"no cascade yet 2">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 2}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual({error, notfound}, rt_cascading:maybe_eventually_exists([N5, N6], ?bucket, Bin)),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists([N3, N4], ?bucket, Bin))
                                    end},

        {"mixed source can send (setup)", fun() ->
            rt:upgrade(N1, current),
            repl_util:wait_until_leader_converge([N1, N2]),
            Running = fun(Node) ->
                RTStatus = rpc:call(Node, riak_repl2_rt, status, []),
                if
                    is_list(RTStatus) ->
                        SourcesList = proplists:get_value(sources, RTStatus, []),
                        Sources = [S || S <- SourcesList,
                            is_list(S),
                            proplists:get_value(connected, S, false),
                            proplists:get_value(source, S) =:= "n34"
                        ],
                        length(Sources) >= 1;
                    true ->
                        false
                end
                      end,
            ?assertEqual(ok, rt:wait_until(N1, Running)),
            % give the node further time to settle
            StatsNotEmpty = fun(Node) ->
                case rpc:call(Node, riak_repl_stats, get_stats, []) of
                    [] ->
                        false;
                    Stats ->
                        is_list(Stats)
                end
                            end,
            ?assertEqual(ok, rt:wait_until(N1, StatsNotEmpty))
                                          end},

        {"node1 put", fun() ->
            Client = rt:pbc(N1),
            Bin = <<"rt after upgrade">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 2}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(N3, ?bucket, Bin, rt_cascading:timeout(100))),
            ?assertEqual({error, notfound}, rt_cascading:maybe_eventually_exists(N5, ?bucket, Bin, 100000))
                      end},

        {"node2 put", fun() ->
            Client = rt:pbc(N2),
            Bin = <<"rt after upgrade 2">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 2}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual({error, notfound}, rt_cascading:maybe_eventually_exists(N5, ?bucket, Bin)),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists([N3,N4], ?bucket, Bin))
                      end},

        {"upgrade the world, cascade starts working", fun() ->
            [N1 | NotUpgraded] = Nodes,
            [rt:upgrade(Node, current) || Node <- NotUpgraded],
            repl_util:wait_until_leader_converge([N1, N2]),
            repl_util:wait_until_leader_converge([N3, N4]),
            repl_util:wait_until_leader_converge([N5, N6]),
            ClusterMgrUp = fun(Node) ->
                case rpc:call(Node, erlang, whereis, [riak_core_cluster_manager]) of
                    P when is_pid(P) ->
                        true;
                    _ ->
                        fail
                end
                           end,
            [rt:wait_until(N, ClusterMgrUp) || N <- Nodes],
            rt_cascading:maybe_reconnect_rt(N1, rt_cascading:get_cluster_mgr_port(N3), "n34"),
            rt_cascading:maybe_reconnect_rt(N3, rt_cascading:get_cluster_mgr_port(N5), "n56"),
            rt_cascading:maybe_reconnect_rt(N5, rt_cascading:get_cluster_mgr_port(N1), "n12"),

            ToB = fun
                      (Atom) when is_atom(Atom) ->
                          list_to_binary(atom_to_list(Atom));
                      (N) when is_integer(N) ->
                          list_to_binary(integer_to_list(N))
                  end,
            ExistsEverywhere = fun(Key, LookupOrder) ->
                Reses = [rt_cascading:maybe_eventually_exists(Node, ?bucket, Key) || Node <- LookupOrder],
                ?debugFmt("Node and it's res:~n~p", [lists:zip(LookupOrder,
                    Reses)]),
                lists:all(fun(E) -> E =:= Key end, Reses)
                               end,
            MakeTest = fun(Node, N) ->
                Name = "writing " ++ atom_to_list(Node) ++ "-write-" ++ integer_to_list(N),
                {NewTail, NewHead} = lists:splitwith(fun(E) ->
                    E =/= Node
                                                     end, Nodes),
                ExistsLookup = NewHead ++ NewTail,
                Test = fun() ->
                    ?debugFmt("Running test ~p", [Name]),
                    Client = rt:pbc(Node),
                    Key = <<(ToB(Node))/binary, "-write-", (ToB(N))/binary>>,
                    Obj = riakc_obj:new(?bucket, Key, Key),
                    riakc_pb_socket:put(Client, Obj, [{w, 2}]),
                    riakc_pb_socket:stop(Client),
                    ?assert(ExistsEverywhere(Key, ExistsLookup))
                       end,
                {Name, Test}
                       end,
            NodeTests = [MakeTest(Node, N) || Node <- Nodes, N <- lists:seq(1, 3)],
            lists:foreach(fun({Name, Eval}) ->
                lager:info("===== mixed version cluster: upgrade world: ~s =====", [Name]),
                Eval()
                          end, NodeTests)
                                                      end},

        {"check pendings", fun() ->
            rt_cascading:wait_until_pending_count_zero(Nodes)
                           end}

    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== mixed version cluster: ~p =====", [Name]),
        Eval()
                  end, Tests).
