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
%%      +------+
%%      | New1 |
%%      +------+
%%      ^       \
%%     /         V
%% +------+    +------+
%% | New3 | <- | Old2 |
%% +------+    +------+
%%
%% This test is configurable for 1.3 versions of Riak, but off by default.
%% place the following config in ~/.riak_test_config to run:
%%
%% {run_rt_cascading_1_3_tests, true}
%% -------------------------------------------------------------------
-module(rt_cascading_new_to_old).
-behavior(riak_test).

%% API
-export([confirm/0]).

-define(bucket, <<"objects">>).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    case rt_config:config_or_os_env(run_rt_cascading_1_3_tests, false) of
        false ->
            lager:info("new_to_old_test_ not configured to run.");
        _ ->
            lager:info("new_to_old_test_ configured to run for 1.3"),
            State = new_to_old_setup(),
            _ = new_to_old_tests(State)
    end,
    pass.

new_to_old_setup() ->
    Conf = rt_cascading:conf(),
    DeployConfs = [{current, Conf}, {previous, Conf}, {current, Conf}],
    [New1, Old2, New3] = Nodes = rt:deploy_nodes(DeployConfs),
    case rpc:call(Old2, application, get_key, [riak_core, vsn]) of
        % this is meant to test upgrading from early BNW aka
        % Brave New World aka Advanced Repl aka version 3 repl to
        % a cascading realtime repl. Other tests handle going from pre
        % repl 3 to repl 3.
        {ok, Vsn} when Vsn < "1.3.0" ->
            {too_old, Nodes};
        _ ->
            [repl_util:make_cluster([N]) || N <- Nodes],
            Names = ["new1", "old2", "new3"],
            [repl_util:name_cluster(Node, Name) || {Node, Name} <- lists:zip(Nodes, Names)],
            [repl_util:wait_until_is_leader(N) || N <- Nodes],
            rt_cascading:connect_rt(New1, 10026, "old2"),
            rt_cascading:connect_rt(Old2, 10036, "new3"),
            rt_cascading:connect_rt(New3, 10016, "new1"),
            Nodes
    end.

new_to_old_tests(Nodes) ->
    %      +------+
    %      | New1 |
    %      +------+
    %      ^       \
    %     /         V
    % +------+    +------+
    % | New3 | <- | Old2 |
    % +------+    +------+
    %
    % This test is configurable for 1.3 versions of Riak, but off by default.
    % place the following config in ~/.riak_test_config to run:
    %
    % {run_rt_cascading_1_3_tests, true}
    [New1, Old2, New3] = Nodes,
    Tests = [

        {"From new1 to old2", fun() ->
            Client = rt:pbc(New1),
            Bin = <<"new1 to old2">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(Old2, ?bucket, Bin)),
            ?assertEqual({error, notfound}, rt_cascading:maybe_eventually_exists(New3, ?bucket, Bin))
                              end},

        {"old2 does not cascade at all", fun() ->
            Client = rt:pbc(New1),
            Bin = <<"old2 no cascade">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(Old2, ?bucket, Bin)),
            ?assertEqual({error, notfound}, rt_cascading:maybe_eventually_exists(New3, ?bucket, Bin))
                                         end},

        {"from new3 to old2", fun() ->
            Client = rt:pbc(New3),
            Bin = <<"new3 to old2">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(New1, ?bucket, Bin)),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(Old2, ?bucket, Bin))
                              end},

        {"from old2 to new3 no cascade", fun() ->
            % in the future, cascading may be able to occur even if it starts
            % from an older source cluster/node. It is prevented for now by
            % having no easy/good way to get the name of the source cluster,
            % thus preventing complete information on the routed clusters.
            Client = rt:pbc(Old2),
            Bin = <<"old2 to new3">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(New3, ?bucket, Bin)),
            ?assertEqual({error, notfound}, rt_cascading:maybe_eventually_exists(New1, ?bucket, Bin))
                                         end},

        {"check pendings", fun() ->
            rt_cascading:wait_until_pending_count_zero(["new1", "old2", "new3"])
                           end}

    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== new to old: ~s =====", [Name]),
        Eval()
                  end, Tests).

