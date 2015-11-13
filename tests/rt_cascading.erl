%% These tests were written using the following riak versions:
%% current: 1.4
%% previous: 1.3.1
%% legacy: 1.2.1
%%
%% uses the following configs with given defaults:
%%
%% ## default_timeout = 1000 :: timeout()
%%
%% Base timeout value; some tests will use a larger value (multiple of).
%%
%% ## run_rt_cascading_1_3_tests = false :: any()
%%
%% Some tests (new_to_old and mixed_version_clusters) only make sense to
%% run if one is testing the version before cascading was introduced and
%% the version it was added; eg current being riak 1.4 and previous being
%% riak 1.3. If this is set to anything (other than 'false') those tests
%% are run. They will not function properly unless the correct versions
%% for riak are avialable. The tests check if the versions under test are
%% too old to be valid however.
%%
%% With this set to default, the tests that depend on this option will
%% emit a log message saying they are not configured to run.
%%

-module(rt_cascading).
-compile(export_all).
-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-define(bucket, <<"objects">>).

-export([confirm/0]).
-export([new_to_old/0, mixed_version_clusters/0]).

% cluster_mgr port = 10006 + 10n where n is devN


confirm() ->
    %% test requires allow_mult=false b/c of rt:systest_read
    rt:set_conf(all, [{"buckets.default.allow_mult", "false"}]),

    simple(),
    big_circle(),
    circle(),
    pyramid(),
    diamond(),
    circle_and_spurs(),
    mixed_version_clusters(),

    case eunit:test(?MODULE, [verbose]) of
        ok ->
            pass;
        error ->
            % at the time this is written, the return value isn't acutally
            % checked, the only way to fail is to crash the process.
            % i leave the fail here in hopes a future version will actually
            % do what the documentation says.
            exit(error),
            fail
    end.

-record(simple_state, {
    beginning :: node(),
    middle :: node(),
    ending :: node()
}).

simple() ->
    State = simple_setup(),
    simple_tests(State),
    simple_teardown(State).

simple_setup() ->
    [BeginCluster, MidCluster, EndCluster] = make_clusters([
        {"beginning", 1},
        {"middle", 1},
        {"end", 1}
    ]),
    {_, [BeginNode]} = BeginCluster,
    {_, [MidNode]} = MidCluster,
    {_, [EndNode]} = EndCluster,
    #simple_state{beginning = BeginNode, middle = MidNode, ending = EndNode}.

simple_teardown(State) ->
    [simple_state | Nodes] = tuple_to_list(State),
    rt:clean_cluster(Nodes).


simple_tests(State) ->
    Tests = [

        {"connecting Beginning to Middle", fun() ->
            Port = get_cluster_mgr_port(State#simple_state.middle),
            repl_util:connect_cluster(State#simple_state.beginning, "127.0.0.1", Port),
            repl_util:enable_realtime(State#simple_state.beginning, "middle"),
            repl_util:start_realtime(State#simple_state.beginning, "middle")
        end},

        {"connection Middle to End", fun() ->
            Port = get_cluster_mgr_port(State#simple_state.ending),
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
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.middle, <<"objects">>, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.ending, <<"objects">>, Bin))
        end},

        {"disable cascading on middle", fun() ->
            rpc:call(State#simple_state.middle, riak_repl_console, realtime_cascades, [["never"]]),
            Bin = <<"disabled cascading">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            Client = rt:pbc(State#simple_state.beginning),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.middle, ?bucket, Bin)),
            ?assertEqual({error, notfound}, maybe_eventually_exists(State#simple_state.ending, ?bucket, Bin))

        end},

        {"re-enable cascading", fun() ->
            rpc:call(State#simple_state.middle, riak_repl_console, realtime_cascades, [["always"]]),
            Bin = <<"cascading re-enabled">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            Client = rt:pbc(State#simple_state.beginning),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.middle, ?bucket, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.ending, ?bucket, Bin))
        end},

        {"check pendings", fun() ->
            wait_until_pending_count_zero([State#simple_state.middle,
                                           State#simple_state.beginning,
                                           State#simple_state.ending])
        end}
    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== simple: ~s =====", [Name]),
        Eval()
    end, Tests).

big_circle() ->
    State = big_circle_setup(),
    _ = big_circle_tests(State),
    big_circle_teardown(State).

big_circle_setup() ->
    Conf = lists:map(fun(I) ->
        {integer_to_list(I), 1}
    end, lists:seq(1, 6)),
    NamesAndNodes = make_clusters(Conf),
    Nodes = lists:flatten([ClusterNodes || {_Name, ClusterNodes} <- NamesAndNodes]),
    Names = [ClusterName || {ClusterName, _} <- Conf],
    [NameHd | NameTail] = Names,
    ConnectTo = NameTail ++ [NameHd],
    ClustersAndConnectTo = lists:zip(NamesAndNodes, ConnectTo),
    ok = lists:foreach(fun({SourceCluster, SinkName}) ->
        {_SourceName, [Node]} = SourceCluster,
        [SinkNode] = proplists:get_value(SinkName, NamesAndNodes),
        Port = get_cluster_mgr_port(SinkNode),
        connect_rt(Node, Port, SinkName)
    end, ClustersAndConnectTo),
    Nodes.

big_circle_teardown(Nodes) ->
    rt:clean_cluster(Nodes).

big_circle_tests(Nodes) ->
    % Initally just 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 1, but then 2 way is
    % added later.
    %     +---+
    %     | 1 |
    %     +---+
    %     ^   ^
    %    /     \
    %   V       V
    % +---+   +---+
    % | 6 |   | 2 |
    % +---+   +---+
    %   ^       ^
    %   |       |
    %   V       V
    % +---+   +---+
    % | 5 |   | 3 |
    % +---+   +---+
    %     ^   ^
    %      \ /
    %       V
    %     +---+
    %     | 4 |
    %     +---+
    Tests = [

        {"circle it", fun() ->
            [One | _] = Nodes,
            C = rt:pbc(One),
            Bin = <<"goober">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Bin, Bin),
            riakc_pb_socket:put(C, Obj, [{w,1}]),
            riakc_pb_socket:stop(C),
            [begin
                ?debugFmt("Checking ~p", [Node]),
                ?assertEqual(Bin, maybe_eventually_exists(Node, Bucket, Bin))
            end || Node <- Nodes]
        end},

        {"2 way repl, and circle it", fun() ->
            ConnectTo = ["6", "1", "2", "3", "4", "5"],
            Connect = fun({Node, ConnectToName}) ->
                Nth = list_to_integer(ConnectToName),
                ConnectNode = lists:nth(Nth, Nodes),
                Port = get_cluster_mgr_port(ConnectNode),
                connect_rt(Node, Port, ConnectToName)
            end,
            lists:map(Connect, lists:zip(Nodes, ConnectTo)),
            C = rt:pbc(hd(Nodes)),
            Bin = <<"2 way repl">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Bin, Bin),
            riakc_pb_socket:put(C, Obj, [{w,1}]),
            lists:map(fun(N) ->
                ?debugFmt("Testing ~p", [N]),
                ?assertEqual(Bin, maybe_eventually_exists(N, Bucket, Bin))
            end, Nodes)
            % there will be duplicate writes, but due to size of the circle,
            % there's not going to be a lot. Also, it's very difficult to
            % determine when/where a duplicate may start/occur.
            % a full breakdown:
            % "1" forwards to "2" and "6", noting its local forwards.
            % so we have two flows going. Assuming both sides flow at the same
            % rate:
            %     1
            %    / \
            %   6   2:   6 has [1, 2, 6]; 2 has [1, 2, 6]
            %   5   3:   5 has [1,2,5,6]; 3 has [1,2,3,6]
            %   4   4:   4 has [1,2,4,5,6]; 4 has [1,2,3,4,6] ! double write
            %   3   5:   3 has [1,2,3,4,5,6]; 5 has [1,2,3,4,5,6] ! double write
            %
            % let's explore the flow with 10 clusters:
            %      1
            %     / \
            %    10  2  10: [1,2,10]; 2: [1,2,10]
            %    9   3  9: [1,2,9,10]; 3: [1,2,3,10]
            %    8   4  8: [1,2,8,9,10]; 4: [1,2,3,4,10]
            %    7   5  7: [1,2,7,8,9,10]; 5: [1,2,3,4,5,10]
            %    6   6  6: [1,2,6,7,8,9,10]; 6: [1,2,3,4,5,6,10] !!
            %    5   7  5: [1,2,5..10]; 7: [1..7,10] !!
            %    4   8  4: [1,2,4..10]; 8: [1..8,10] !!
            %    3   9  3: [1..10]; 9: [1..10] !!
            % so, by adding 4 clusters, we've added 2 overlaps.
            % best guess based on what's above is:
            %  NumDuplicateWrites = ceil(NumClusters/2 - 1.5)
        end},

        {"check pendings", fun() ->
            wait_until_pending_count_zero(Nodes)
        end}
    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== big circle: ~s =====", [Name]),
        Eval()
    end, Tests).

circle() ->
    State = circle_setup(),
    _ = circle_tests(State),
    circle_teardown(State).

circle_setup() ->
    Conf = [{"one", 1}, {"two", 1}, {"three", 1}],
    Clusters = make_clusters(Conf),
    [[One], [Two], [Three]] = Unflattened = [ClusterNodes || {_Name, ClusterNodes} <- Clusters],

    Connections = [
        {One, Two, "two"},
        {Two, Three, "three"},
        {Three, One, "one"}
    ],
    ok = lists:foreach(fun({Node, ConnectNode, Name}) ->
        Port = get_cluster_mgr_port(ConnectNode),
        connect_rt(Node, Port, Name)
    end, Connections),
    lists:flatten(Unflattened).

circle_teardown(Nodes) ->
    rt:clean_cluster(Nodes).

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
            ?assertEqual(Bin, maybe_eventually_exists(lists:last(Nodes), <<"objects">>, Bin)),
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
            ?assertEqual(Bin, maybe_eventually_exists(One, <<"objects">>, Bin)),
            timer:sleep(1000),
            Status = rpc:call(Two, riak_repl2_rt, status, []),
            [SinkData] = proplists:get_value(sinks, Status, [[]]),
            ?assertEqual(2, proplists:get_value(expect_seq, SinkData))
        end},

        {"check pendings", fun() ->
            wait_until_pending_count_zero(Nodes)
        end}
    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== cirle: ~s =====", [Name]),
        Eval()
    end, Tests).

pyramid() ->
    State = pyramid_setup(),
    _ = pyramid_tests(State),
    pyramid_teardown(State).

pyramid_setup() ->
    Conf = [{"top", 1}, {"left", 1}, {"left2", 1}, {"right", 1}, {"right2", 1}],
    Clusters = make_clusters(Conf),
    GetPort = fun(Name) ->
        [Node] = proplists:get_value(Name, Clusters),
        get_cluster_mgr_port(Node)
    end,
    [Top] = proplists:get_value("top", Clusters),
    [Left] = proplists:get_value("left", Clusters),
    [Right] = proplists:get_value("right", Clusters),
    connect_rt(Top, GetPort("left"), "left"),
    connect_rt(Left, GetPort("left2"), "left2"),
    connect_rt(Top, GetPort("right"), "right"),
    connect_rt(Right, GetPort("right2"), "right2"),
    lists:flatten([Nodes || {_, Nodes} <- Clusters]).

pyramid_teardown(Nodes) ->
    rt:clean_cluster(Nodes).

pyramid_tests(Nodes) ->
    %        +-----+
    %        | top |
    %        +-----+
    %       /       \
    %      V         V
    % +------+   +-------+
    % | left |   | right |
    % +------+   +-------+
    %     |          |
    %     V          V
    % +-------+  +--------+
    % | left2 |  | right2 |
    % +-------+  +--------+

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
                ?assertEqual(Bin, maybe_eventually_exists(N, Bucket, Bin))
            end, Nodes)
        end},

        {"check pendings", fun() ->
            wait_until_pending_count_zero(Nodes)
         end}
    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== pyramid: ~s =====", [Name]),
        Eval()
    end, Tests).

diamond() ->
    State = diamond_setup(),
    _ = diamond_tests(State),
    diamond_teardown(State).

diamond_setup() ->
    Clusters = make_clusters([{"top", 1}, {"midleft", 1}, {"midright", 1}, {"bottom", 1}]),
    GetNode = fun(Name) ->
        [N] = proplists:get_value(Name, Clusters),
        N
    end,
    GetPort = fun(Name) ->
        get_cluster_mgr_port(GetNode(Name))
    end,
    connect_rt(GetNode("top"), GetPort("midleft"), "midleft"),
    connect_rt(GetNode("midleft"), GetPort("bottom"), "bottom"),
    connect_rt(GetNode("midright"), GetPort("bottom"), "bottom"),
    connect_rt(GetNode("top"), GetPort("midright"), "midright"),
    lists:flatten([Nodes || {_, Nodes} <- Clusters]).

diamond_teardown(Nodes) ->
    rt:clean_cluster(Nodes).

diamond_tests(Nodes) ->
    % A pretty cluster of clusters:
    %                      +-----+
    %     +--------------->| top |
    %     | loop added     +-----+
    %     |               /       \
    %     |              V         V
    %     |    +---------+         +----------+
    %     ^    | midleft |         | midright |
    %     |    +---------+         +----------+
    %     |               \       /
    %     |                V     V
    %     |               +--------+
    %     +-------<-------| bottom |
    %                     +--------+
    Tests = [

        {"unfortunate double write", fun() ->
            [Top, MidLeft, MidRight, Bottom] = Nodes,
            Client = rt:pbc(Top),
            Bin = <<"start_at_top">>,
            Obj = riakc_obj:new(<<"objects">>, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            timer:sleep(100000),
            ?assertEqual(Bin, maybe_eventually_exists(MidLeft, <<"objects">>, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists(MidRight, <<"objects">>, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists(Bottom, <<"objects">>, Bin)),
            %timer:sleep(1000),
            Status = rpc:call(Bottom, riak_repl2_rt, status, []),
            [SinkOne, SinkTwo] = proplists:get_value(sinks, Status, [[], []]),
            ?assertEqual(proplists:get_value(expect_seq, SinkOne), proplists:get_value(expect_seq, SinkTwo))
        end},

        {"connect bottom to top", fun() ->
            [Top, _MidLeft, _MidRight, Bottom] = Nodes,
            Port = get_cluster_mgr_port(Top),
            connect_rt(Bottom, Port, "top"),
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
                ?assertEqual(Bin, maybe_eventually_exists(N, Bucket, Bin))
            end || N <- [Bottom, Top, MidLeft]],

            Status2 = rpc:call(MidRight, riak_repl2_rt, status, []),
            [Sink2] = proplists:get_value(sinks, Status2, [[]]),
            GotSeq = proplists:get_value(expect_seq, Sink2),
            ?assertEqual(ExpectSeq, GotSeq)
        end},

        {"check pendings", fun() ->
            wait_until_pending_count_zero(Nodes)
        end}

    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== diamond: ~s =====", [Name]),
        Eval()
    end, Tests).

circle_and_spurs() ->
    State = circle_and_spurs_setup(),
    _ = circle_and_spurs_tests(State),
    circle_and_spurs_teardown(State).

circle_and_spurs_setup() ->
    Config = [
        {"north", 1, ["east", "north_spur"]},
        {"east", 1, ["west", "east_spur"]},
        {"west", 1, ["north", "west_spur"]},
        {"north_spur", 1},
        {"east_spur", 1},
        {"west_spur", 1}
    ],
    Clusters = make_clusters(Config),
    lists:flatten([Nodes || {_, Nodes} <- Clusters]).

circle_and_spurs_teardown(Nodes) ->
    rt:clean_cluster(Nodes).

circle_and_spurs_tests(Nodes) ->
    %                        +------------+
    %                        | north_spur |
    %                        +------------+
    %                               ^
    %                               |
    %                           +-------+
    %                     +---> | north | ---+
    %                     |     +-------+    |
    %                     |                  V
    % +-----------+    +------+           +------+    +-----------+
    % | west_spur | <- | west | <-------- | east | -> | east_spur |
    % +-----------+    +------+           +------+    +-----------+
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
                ?assertEqual(Bin, maybe_eventually_exists(N, Bucket, Bin))
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
                ?assertEqual(Bin, maybe_eventually_exists(N, Bucket, Bin))
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
                ?assertEqual({error, notfound}, maybe_eventually_exists(N, Bucket, Bin))
            end || N <- Nodes, N =/= NorthSpur]
        end},

        {"check pendings", fun() ->
            wait_until_pending_count_zero(Nodes)
        end}

    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== cirlce_and_spurs: ~s =====", [Name]),
        Eval()
    end, Tests).

mixed_version_clusters() ->
    case rt_config:config_or_os_env(run_rt_cascading_1_3_tests, false) of
        false -> 
            lager:info("mixed_version_clusters_test_ not configured to run!");
        _ ->
            State = mixed_version_clusters_setup(),
            _ = mixed_version_clusters_tests(State),
            mixed_version_clusters_teardown(State)
    end.

mixed_version_clusters_setup() ->
    Conf = conf(),
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
            connect_rt(N1, get_cluster_mgr_port(N3), "n34"),
            connect_rt(N3, get_cluster_mgr_port(N5), "n56"),
            connect_rt(N5, get_cluster_mgr_port(N1), "n12"),
            Nodes
    end.

mixed_version_clusters_teardown({too_old, Nodes}) ->
    mixed_version_clusters_teardown(Nodes);

mixed_version_clusters_teardown(Nodes) ->
    rt:clean_cluster(Nodes).

mixed_version_clusters_tests({too_old, _Nodes}) ->
    ok;

mixed_version_clusters_tests(Nodes) ->
    %      +-----+
    %      | n12 |
    %      +-----+
    %      ^      \
    %     /        V
    % +-----+    +-----+
    % | n56 | <- | n34 |
    % +-----+    +-----+
    % 
    % This test is configurable for 1.3 versions of Riak, but off by default.
    % place the following config in ~/.riak_test_config to run:
    % 
    % {run_rt_cascading_1_3_tests, true}
    [N1, N2, N3, N4, N5, N6] = Nodes,
    Tests = [

        {"no cascading at first 1", fun() ->
            Client = rt:pbc(N1),
            Bin = <<"no cascade yet">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 2}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual({error, notfound}, maybe_eventually_exists([N5, N6], ?bucket, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists([N3, N4], ?bucket, Bin))
        end},

        {"no cascading at first 2", fun() ->
            Client = rt:pbc(N2),
            Bin = <<"no cascade yet 2">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 2}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual({error, notfound}, maybe_eventually_exists([N5, N6], ?bucket, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists([N3, N4], ?bucket, Bin))
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
            ?assertEqual(Bin, maybe_eventually_exists(N3, ?bucket, Bin, timeout(100))),
            ?assertEqual({error, notfound}, maybe_eventually_exists(N5, ?bucket, Bin, 100000))
        end},

        {"node2 put", fun() ->
            Client = rt:pbc(N2),
            Bin = <<"rt after upgrade 2">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 2}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual({error, notfound}, maybe_eventually_exists(N5, ?bucket, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists([N3,N4], ?bucket, Bin))
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
            maybe_reconnect_rt(N1, get_cluster_mgr_port(N3), "n34"),
            maybe_reconnect_rt(N3, get_cluster_mgr_port(N5), "n56"),
            maybe_reconnect_rt(N5, get_cluster_mgr_port(N1), "n12"),

            ToB = fun
                (Atom) when is_atom(Atom) ->
                    list_to_binary(atom_to_list(Atom));
                (N) when is_integer(N) ->
                    list_to_binary(integer_to_list(N))
            end,
            ExistsEverywhere = fun(Key, LookupOrder) ->
                Reses = [maybe_eventually_exists(Node, ?bucket, Key) || Node <- LookupOrder],
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
            wait_until_pending_count_zero(Nodes)
        end}

    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== mixed version cluster: ~p =====", [Name]),
        Eval()
    end, Tests).

new_to_old() ->
    case eunit:test(?MODULE:new_to_old_test_(), [verbose]) of
        ok ->
            pass;
        error ->
            % at the time this is written, the return value isn't acutally
            % checked, the only way to fail is to crash the process.
            % i leave the fail here in hopes a future version will actually
            % do what the documentation says.
            exit(error),
            fail
    end.

new_to_old_test_() ->
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
    case rt_config:config_or_os_env(run_rt_cascading_1_3_tests, false) of
        false -> 
            lager:info("new_to_old_test_ not configured to run!"),
            [];
        _ ->
            lager:info("new_to_old_test_ configured to run for 1.3"),
            new_to_old_test_dep()
    end.

new_to_old_test_dep() ->
    {timeout, timeout(105), {setup, fun() ->
        Conf = conf(),
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
                connect_rt(New1, 10026, "old2"),
                connect_rt(Old2, 10036, "new3"),
                connect_rt(New3, 10016, "new1"),
                Nodes
        end
    end,
    fun(MaybeNodes) ->
        Nodes = case MaybeNodes of
            {too_old, Ns} -> Ns;
            _ -> MaybeNodes
        end,
        rt:clean_cluster(Nodes)
    end,
    fun({too_old, _}) -> [];
       ([New1, Old2, New3]) -> [

        {"From new1 to old2", timeout, timeout(25), fun() ->
            Client = rt:pbc(New1),
            Bin = <<"new1 to old2">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(Old2, ?bucket, Bin)),
            ?assertEqual({error, notfound}, maybe_eventually_exists(New3, ?bucket, Bin))
        end},

        {"old2 does not cascade at all", timeout, timeout(25), fun() ->
            Client = rt:pbc(New1),
            Bin = <<"old2 no cascade">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(Old2, ?bucket, Bin)),
            ?assertEqual({error, notfound}, maybe_eventually_exists(New3, ?bucket, Bin))
        end},

        {"from new3 to old2", timeout, timeout(25), fun() ->
            Client = rt:pbc(New3),
            Bin = <<"new3 to old2">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(New1, ?bucket, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists(Old2, ?bucket, Bin))
        end},

        {"from old2 to new3 no cascade", timeout, timeout(25), fun() ->
            % in the future, cascading may be able to occur even if it starts
            % from an older source cluster/node. It is prevented for now by
            % having no easy/good way to get the name of the source cluster,
            % thus preventing complete information on the routed clusters.
            Client = rt:pbc(Old2),
            Bin = <<"old2 to new3">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(New3, ?bucket, Bin)),
            ?assertEqual({error, notfound}, maybe_eventually_exists(New1, ?bucket, Bin))
        end},
        {"check pendings", fun() ->
            wait_until_pending_count_zero(["new1", "old2", "new3"])
        end}
    ] end}}.

ensure_ack_test_() ->
    {timeout, timeout(130), {setup, fun() ->
        Conf = conf(),
        [LeaderA, LeaderB] = Nodes = rt:deploy_nodes(2, Conf),
        [repl_util:make_cluster([N]) || N <- Nodes],
        [repl_util:wait_until_is_leader(N) || N <- Nodes],
        Names = ["A", "B"],
        [repl_util:name_cluster(Node, Name) || {Node, Name} <- lists:zip(Nodes, Names)],
        %repl_util:name_cluster(LeaderA, "A"),
        %repl_util:name_cluster(LeaderB, "B"), 
        lager:info("made it past naming"), 
        Port = get_cluster_mgr_port(LeaderB),
        lager:info("made it past port"), 
        connect_rt(LeaderA, Port, "B"),
        lager:info("made it past connect"), 
        [LeaderA, LeaderB]
    end,
    fun(Nodes) ->
        rt:clean_cluster(Nodes)
    end,

    fun([LeaderA, LeaderB] = _Nodes) -> [
        {"ensure acks", timeout, timeout(65), fun() ->
            lager:info("Nodes:~p, ~p", [LeaderA, LeaderB]),
            TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
            TestBucket = <<TestHash/binary, "-rt_test_a">>,

            %% Write some objects to the source cluster (A),
            lager:info("Writing 1 key to ~p, which should RT repl to ~p",
            [LeaderA, LeaderB]),
            ?assertEqual([], repl_util:do_write(LeaderA, 1, 1, TestBucket, 2)),

            %% verify data is replicated to B
            lager:info("Reading 1 key written from ~p", [LeaderB]),
            ?assertEqual(0, repl_util:wait_for_reads(LeaderB, 1, 1, TestBucket, 2)),

            RTQStatus = rpc:call(LeaderA, riak_repl2_rtq, status, []),

            Consumers = proplists:get_value(consumers, RTQStatus),
            case proplists:get_value("B", Consumers) of
                 undefined ->
                    [];
                 Consumer ->
                    Unacked = proplists:get_value(unacked, Consumer, 0),
                    lager:info("unacked: ~p", [Unacked]),
                    ?assertEqual(0, Unacked)
            end

        end}
    ]
    end}}.

ensure_unacked_and_queue() ->
    eunit(ensure_unacked_and_queue_test_()).

ensure_unacked_and_queue_test_() ->
    {timeout, timeout(2300), {setup, fun() ->
        Nodes = rt:deploy_nodes(6, conf()),
        {N123, N456} = lists:split(3, Nodes),
        repl_util:make_cluster(N123),
        repl_util:make_cluster(N456),
        repl_util:wait_until_leader_converge(N123),
        repl_util:wait_until_leader_converge(N456),
        repl_util:name_cluster(hd(N123), "n123"),
        repl_util:name_cluster(hd(N456), "n456"),
        N456Port = get_cluster_mgr_port(hd(N456)),
        connect_rt(hd(N123), N456Port, "n456"),
        N123Port = get_cluster_mgr_port(hd(N123)),
        connect_rt(hd(N456), N123Port, "n123"),
        {N123, N456}
    end,
    maybe_skip_teardown(fun({N123, N456}) ->
        rt:clean_cluster(N123),
        rt:clean_cluster(N456)
    end),
    fun({N123, N456}) -> [

        {"unacked does not increase when there are skips", timeout, timeout(100), fun() ->
            N123Leader = hd(N123),
            N456Leader = hd(N456),

            write_n_keys(N123Leader, N456Leader, 1, 10000),

            write_n_keys(N456Leader, N123Leader, 10001, 20000),

            Res = rt:wait_until(fun() ->
                RTQStatus = rpc:call(N123Leader, riak_repl2_rtq, status, []),

                Consumers = proplists:get_value(consumers, RTQStatus),
                Data = proplists:get_value("n456", Consumers),
                Unacked = proplists:get_value(unacked, Data),
                ?debugFmt("unacked: ~p", [Unacked]),
                0 == Unacked
            end),
            ?assertEqual(ok, Res)
        end},

        {"after acks, queues are empty", fun() ->
            Nodes = N123 ++ N456,
            Got = lists:map(fun(Node) ->
                rpc:call(Node, riak_repl2_rtq, all_queues_empty, [])
            end, Nodes),
            Expected = [true || _ <- lists:seq(1, length(Nodes))],
            ?assertEqual(Expected, Got)
        end},

        {"after acks, queues truly are empty. Truly", fun() ->
            Nodes = N123 ++ N456,
            Gots = lists:map(fun(Node) ->
                {Node, rpc:call(Node, riak_repl2_rtq, dumpq, [])}
            end, Nodes),
            lists:map(fun({Node, Got}) ->
                ?debugFmt("Checking data from ~p", [Node]),
                ?assertEqual([], Got)
            end, Gots)
        end},

        {"dual loads keeps unacked satisfied", timeout, timeout(100), fun() ->
            N123Leader = hd(N123),
            N456Leader = hd(N456),
            LoadN123Pid = spawn(fun() ->
                {Time, Val} = timer:tc(fun write_n_keys/4, [N123Leader, N456Leader, 20001, 30000]),
                ?debugFmt("loading 123 to 456 took ~p to get ~p", [Time, Val]),
                Val
            end),
            LoadN456Pid = spawn(fun() ->
                {Time, Val} = timer:tc(fun write_n_keys/4, [N456Leader, N123Leader, 30001, 40000]),
                ?debugFmt("loading 456 to 123 took ~p to get ~p", [Time, Val]),
                Val
            end),
            Exits = wait_exit([LoadN123Pid, LoadN456Pid], infinity),
            ?assert(lists:all(fun(E) -> E == normal end, Exits)),

            StatusDig = fun(SinkName, Node) ->
                Status = rpc:call(Node, riak_repl2_rtq, status, []),
                Consumers = proplists:get_value(consumers, Status, []),
                ConsumerStats = proplists:get_value(SinkName, Consumers, []),
                proplists:get_value(unacked, ConsumerStats)
            end,

            N123UnackedRes = rt:wait_until(fun() ->
                Unacked = StatusDig("n456", N123Leader),
                ?debugFmt("Unacked: ~p", [Unacked]),
                0 == Unacked
            end),
            ?assertEqual(ok, N123UnackedRes),

            N456Unacked = StatusDig("n123", N456Leader),
            case N456Unacked of
                0 ->
                    ?assert(true);
                _ ->
                    N456Unacked2 = StatusDig("n123", N456Leader),
                    ?debugFmt("Not 0, are they at least decreasing?~n"
                        "    ~p, ~p", [N456Unacked2, N456Unacked]),
                    ?assert(N456Unacked2 < N456Unacked)
            end
        end},

        {"after dual load acks, queues are empty", fun() ->
            Nodes = N123 ++ N456,
            Got = lists:map(fun(Node) ->
                rpc:call(Node, riak_repl2_rtq, all_queues_empty, [])
            end, Nodes),
            Expected = [true || _ <- lists:seq(1, length(Nodes))],
            ?assertEqual(Expected, Got)
        end},

        {"after dual load acks, queues truly are empty. Truly", fun() ->
            Nodes = N123 ++ N456,
            Gots = lists:map(fun(Node) ->
                {Node, rpc:call(Node, riak_repl2_rtq, dumpq, [])}
            end, Nodes),
            lists:map(fun({Node, Got}) ->
                ?debugFmt("Checking data from ~p", [Node]),
                ?assertEqual([], Got)
            end, Gots)
        end},

        {"no negative pendings", fun() ->
            Nodes = N123 ++ N456,
            GetPending = fun({sink_stats, SinkStats}) ->
                ConnTo = proplists:get_value(rt_sink_connected_to, SinkStats),
                proplists:get_value(pending, ConnTo)
            end,
            lists:map(fun(Node) ->
                ?debugFmt("Checking node ~p", [Node]),
                Status = rpc:call(Node, riak_repl_console, status, [quiet]),
                Sinks = proplists:get_value(sinks, Status),
                lists:map(fun(SStats) ->
                    Pending = GetPending(SStats),
                    ?assertEqual(0, Pending)
                end, Sinks)
            end, Nodes)
        end}

    ] end}}.

%% =====
%% utility functions for teh happy
%% ====

wait_exit(Pids, Timeout) ->
    Mons = [{erlang:monitor(process, Pid), Pid} || Pid <- Pids],
    lists:map(fun({Mon, Pid}) ->
        receive
            {'DOWN', Mon, process, Pid, Cause} ->
                Cause
        after Timeout ->
            timeout
        end
    end, Mons).

make_clusters(UnNormalClusterConfs) ->
    ClusterConfs = lists:map(fun
        ({Name, Size}) ->
            {Name, Size, []};
        ({_Name, _Size, _ConnectsTo} = E) ->
            E
    end, UnNormalClusterConfs),
    DeployList = lists:foldl(fun
        ({_Name, Size, _ConnectTo}, ConfAcc) ->
            Conf = conf(),
            AddToAcc = lists:duplicate(Size, {current, Conf}),
            ConfAcc ++ AddToAcc
    end, [], ClusterConfs),
    Nodes = rt:deploy_nodes(DeployList),
    lager:info("nodes deployed: ~p", [Nodes]),
    {NamesAndNodes, []} = lists:foldl(fun
        ({Name, Size, _ConnectTo}, {Clusters, NodesLeft}) ->
            {ForCluster, NewNodesLeft} = lists:split(Size, NodesLeft),
            {Clusters ++ [{Name, ForCluster}], NewNodesLeft}
    end, {[], Nodes}, ClusterConfs),
    NamesAndNodes = lists:map(fun({Name, ForClusterNodes}) ->
        {Name, make_cluster(Name, ForClusterNodes)}
    end, NamesAndNodes),
    ok = lists:foreach(fun({Name, _Size, ConnectsTo}) ->
        lists:foreach(fun(ConnectToName) ->
            connect_rt(get_node(Name, NamesAndNodes), get_port(ConnectToName, NamesAndNodes), ConnectToName)
        end, ConnectsTo)
    end, ClusterConfs),
    NamesAndNodes.

make_cluster(Name, Nodes) ->
    repl_util:make_cluster(Nodes),
    _ = [repl_util:wait_until_is_leader(N) || N <- Nodes],
    [ANode | _] = Nodes,
    repl_util:name_cluster(ANode, Name),
    Nodes.

get_node(Name, NamesAndNodes) ->
    [Node | _] = proplists:get_value(Name, NamesAndNodes),
    Node.

get_port(Name, NamesAndNodes) ->
    get_cluster_mgr_port(get_node(Name, NamesAndNodes)).

conf() ->
    [{lager, [
        {handlers, [
            {lager_console_backend,info},
            {lager_file_backend, [
                {"./log/error.log",error,10485760,"$D0",5},
                {"./log/console.log",info,10485760,"$D0",5},
                {"./log/debug.log",debug,10485760,"$D0",5}
            ]}
        ]},
        {crash_log,"./log/crash.log"},
        {crash_log_msg_size,65536},
        {crash_log_size,10485760},
        {crash_log_date,"$D0"},
        {crash_log_count,5},
        {error_logger_redirect,true}
    ]},
    {riak_repl, [
        {fullsync_on_connect, false},
        {fullsync_interval, disabled},
        {diff_batch_size, 10},
        {rt_heartbeat_interval, undefined}
    ]}].

get_cluster_mgr_port(Node) ->
    {ok, {_Ip, Port}} = rpc:call(Node, application, get_env, [riak_core, cluster_mgr]),
    Port.

maybe_reconnect_rt(SourceNode, SinkPort, SinkName) ->
    case repl_util:wait_for_connection(SourceNode, SinkName) of
        fail ->
            connect_rt(SourceNode, SinkPort, SinkName);
        Oot ->
            Oot
    end.

connect_rt(SourceNode, SinkPort, SinkName) ->
    repl_util:connect_cluster(SourceNode, "127.0.0.1", SinkPort),
    repl_util:wait_for_connection(SourceNode, SinkName),
    repl_util:enable_realtime(SourceNode, SinkName),
    repl_util:start_realtime(SourceNode, SinkName).

exists(Nodes, Bucket, Key) ->
    exists({error, notfound}, Nodes, Bucket, Key).

exists(Got, [], _Bucket, _Key) ->
    Got;
exists({error, notfound}, [Node | Tail], Bucket, Key) ->
    Pid = rt:pbc(Node),
    Got = riakc_pb_socket:get(Pid, Bucket, Key),
    riakc_pb_socket:stop(Pid),
    exists(Got, Tail, Bucket, Key);
exists(Got, _Nodes, _Bucket, _Key) ->
    Got.

maybe_eventually_exists(Node, Bucket, Key) ->
    Timeout = timeout(10),
    WaitTime = rt_config:get(default_wait_time, 1000),
    maybe_eventually_exists(Node, Bucket, Key, Timeout, WaitTime).

maybe_eventually_exists(Node, Bucket, Key, Timeout) ->
    WaitTime = rt_config:get(default_wait_time, 1000),
    maybe_eventually_exists(Node, Bucket, Key, Timeout, WaitTime).

maybe_eventually_exists(Node, Bucket, Key, Timeout, WaitMs) when is_atom(Node) ->
    maybe_eventually_exists([Node], Bucket, Key, Timeout, WaitMs);

maybe_eventually_exists(Nodes, Bucket, Key, Timeout, WaitMs) ->
    Got = exists(Nodes, Bucket, Key),
    maybe_eventually_exists(Got, Nodes, Bucket, Key, Timeout, WaitMs).

maybe_eventually_exists({error, notfound}, Nodes, Bucket, Key, Timeout, WaitMs) when Timeout > 0 ->
    timer:sleep(WaitMs),
    Got = exists(Nodes, Bucket, Key),
    Timeout2 = case Timeout of
        infinity ->
            infinity;
        _ ->
            Timeout - WaitMs
    end,
    maybe_eventually_exists(Got, Nodes, Bucket, Key, Timeout2, WaitMs);

maybe_eventually_exists({ok, RiakObj}, _Nodes, _Bucket, _Key, _Timeout, _WaitMs) ->
    riakc_obj:get_value(RiakObj);

maybe_eventually_exists(Got, _Nodes, _Bucket, _Key, _Timeout, _WaitMs) ->
    Got.

wait_for_rt_started(Node, ToName) ->
    Fun = fun(_) ->
        Status = rpc:call(Node, riak_repl2_rt, status, []),
        Started = proplists:get_value(started, Status, []),
        lists:member(ToName, Started)
    end,
    rt:wait_until(Node, Fun).

write_n_keys(Source, Destination, M, N) ->
    TestHash =  list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                <<X>> <= erlang:md5(term_to_binary(os:timestamp()))]),
    TestBucket = <<TestHash/binary, "-rt_test_a">>,
    First = M,
    Last = N,

    %% Write some objects to the source cluster (A),
    lager:info("Writing ~p keys to ~p, which should RT repl to ~p",
               [Last-First+1, Source, Destination]),
    ?assertEqual([], repl_util:do_write(Source, First, Last, TestBucket, 2)),

    %% verify data is replicated to B
    lager:info("Reading ~p keys written from ~p", [Last-First+1, Destination]),
    ?assertEqual(0, repl_util:wait_for_reads(Destination, First, Last, TestBucket, 2)).

timeout(MultiplyBy) ->
    case rt_config:get(default_timeout, 1000) of
        infinity ->
            infinity;
        N ->
            N * MultiplyBy
    end.

eunit(TestDef) ->
    case eunit:test(TestDef, [verbose]) of
        ok ->
            pass;
        error ->
            exit(error),
            fail
    end.

maybe_skip_teardown(TearDownFun) ->
    fun(Arg) ->
        case rt_config:config_or_os_env(skip_teardowns, undefined) of
            undefined ->
                TearDownFun(Arg);
            _ ->
                ok
        end
    end.

wait_until_pending_count_zero(Nodes) ->
    WaitFun = fun() ->
        {Statuses, _} =  rpc:multicall(Nodes, riak_repl2_rtq, status, []),
        Out = [check_status(S) || S <- Statuses],
        not lists:member(false, Out)	      
    end,
    ?assertEqual(ok, rt:wait_until(WaitFun)),
    ok.

check_status(Status) ->            
    case proplists:get_all_values(consumers, Status) of
        undefined ->
	    true;
	[] ->
	    true;
	Cs ->
	    PendingList = [proplists:lookup_all(pending, C) || {_, C} <- lists:flatten(Cs)],
            PendingCount = lists:sum(proplists:get_all_values(pending, lists:flatten(PendingList))),
	    ?debugFmt("RTQ status pending on test node:~p", [PendingCount]),
	    PendingCount == 0
    end.
