%% These tests were written using the following riak versions:
%% current: 1.4
%% previous: 1.3.1
%% legacy: 1.2.1

-module(rt_cascading).
-compile(export_all).
-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-define(bucket, <<"objects">>).

-export([confirm/0]).

% cluster_mgr port = 10006 + 10n where n is devN

confirm() ->
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
    beginning = [] :: [node()],
    middle = [] :: [node()],
    ending = [] :: [node()]
}).

simple_test_() ->
    % +-----------+    +--------+    +-----+
    % | beginning | -> | middle | -> | end |
    % +-----------+    +--------+    +-----+
    {timeout, 60000, {setup, fun() ->
        Conf = conf(),
        [BeginNode, MiddleNode, EndNode] = Nodes = rt:deploy_nodes(3, Conf),
        repl_util:make_cluster([BeginNode]),
        repl_util:make_cluster([MiddleNode]),
        repl_util:make_cluster([EndNode]),
        repl_util:name_cluster(BeginNode, "beginning"),
        [repl_util:wait_until_is_leader(N) || N <- Nodes],
        repl_util:name_cluster(MiddleNode, "middle"),
        repl_util:name_cluster(EndNode, "end"),
        #simple_state{beginning = BeginNode, middle = MiddleNode,
            ending = EndNode}
    end,
    fun(State) ->
        Nodes = [State#simple_state.beginning, State#simple_state.middle,
            State#simple_state.ending],
        rt:clean_cluster(Nodes)
    end,
    fun(State) -> [

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

        {"cascade a put from beginning down to ending", timeout, 30000, fun() ->
            BeginningClient = rt:pbc(State#simple_state.beginning),
            Bin = <<"cascading realtime">>,
            Obj = riakc_obj:new(<<"objects">>, Bin, Bin),
            riakc_pb_socket:put(BeginningClient, Obj, [{w,1}]),
            riakc_pb_socket:stop(BeginningClient),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.middle, <<"objects">>, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.ending, <<"objects">>, Bin))
        end},

        {"disable cascading on middle", timeout, 30000, fun() ->
            rpc:call(State#simple_state.middle, riak_repl_console, realtime_cascades, [["never"]]),
            Bin = <<"disabled cascading">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            Client = rt:pbc(State#simple_state.beginning),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.middle, ?bucket, Bin)),
            ?assertEqual({error, notfound}, maybe_eventually_exists(State#simple_state.ending, ?bucket, Bin))
            
        end},

        {"re-enable cascading", timeout, 30000, fun() ->
            rpc:call(State#simple_state.middle, riak_repl_console, realtime_cascades, [["always"]]),
            Bin = <<"cascading re-enabled">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            Client = rt:pbc(State#simple_state.beginning),
            riakc_pb_socket:put(Client, Obj, [{w,1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.middle, ?bucket, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists(State#simple_state.ending, ?bucket, Bin))
        end}

    ] end}}.

big_circle_test_() ->
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
    {timeout, 10000, {setup, fun() ->
        Conf = conf(),
        Nodes = rt:deploy_nodes(6, Conf),
        [repl_util:make_cluster([N]) || N <- Nodes],
        [repl_util:wait_until_is_leader(N) || N <- Nodes],
        Names = ["1", "2", "3", "4", "5", "6"],
        [repl_util:name_cluster(Node, Name) || {Node, Name} <- lists:zip(Nodes, Names)],
        [NameHd | NameTail] = Names,
        ConnectTo = NameTail ++ [NameHd],
        NamePortMap = lists:map(fun({Node, Name}) ->
            Port = get_cluster_mgr_port(Node),
            {Name, Port}
        end, lists:zip(Nodes, Names)),
        Connect = fun({Node, ConnectToName}) ->
            Port = proplists:get_value(ConnectToName, NamePortMap),
            connect_rt(Node, Port, ConnectToName)
        end,
        Res = lists:map(Connect, lists:zip(Nodes, ConnectTo)),
        ?debugFmt("der res: ~p", [Res]),
        Nodes
    end,
    fun(Nodes) ->
        rt:clean_cluster(Nodes)
    end,
    fun(Nodes) -> [

        {"circle it", timeout, 10000, fun() ->
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

        {"2 way repl, and circle it", timeout, 10000, fun() ->
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
        end}

    ] end}}.

circle_test_() ->
    %      +-----+
    %      | one |
    %      +-----+
    %      ^      \
    %     /        V
    % +-------+    +-----+
    % | three | <- | two |
    % +-------+    +-----+
    {timeout, 10000, {setup, fun() ->
        Conf = conf(),
        [One, Two, Three] = Nodes = rt:deploy_nodes(3, Conf),
        [repl_util:make_cluster([N]) || N <- Nodes],
        [repl_util:wait_until_is_leader(N) || N <- Nodes],
        Names = ["one", "two", "three"],
        [repl_util:name_cluster(Node, Name) || {Node, Name} <- lists:zip(Nodes, Names)],

        Connections = [
            {One, Two, "two"},
            {Two, Three, "three"},
            {Three, One, "one"}
        ],
        lists:map(fun({Node, ConnectNode, Name}) ->
            Port = get_cluster_mgr_port(ConnectNode),
            connect_rt(Node, Port, Name)
        end, Connections),
        Nodes
    end,
    fun(Nodes) ->
        rt:clean_cluster(Nodes)
    end,
    fun(Nodes) -> [

        {"cascade all the way to the other end, but no further", timeout, 6000, fun() ->
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

        {"cascade starting at a different point", timeout, 6000, fun() ->
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
        end}

    ] end}}.

pyramid_test_() ->
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

    {timeout, 60000, {setup, fun() ->
        Conf = conf(),
        [Top, Left, Left2, Right, Right2] = Nodes = rt:deploy_nodes(5, Conf),
        [repl_util:make_cluster([N]) || N <- Nodes],
        [repl_util:wait_until_is_leader(N) || N <- Nodes],
        Names = ["top", "left", "left2", "right", "right2"],
        [repl_util:name_cluster(Node, Name) || {Node, Name} <- lists:zip(Nodes, Names)],
        Ports = lists:map(fun(Node) ->
            Port = get_cluster_mgr_port(Node),
            {Node, Port}
        end, Nodes),
        connect_rt(Top, proplists:get_value(Left, Ports), "left"),
        connect_rt(Left, proplists:get_value(Left2, Ports), "left2"),
        connect_rt(Top, proplists:get_value(Right, Ports), "right"),
        connect_rt(Right, proplists:get_value(Right2, Ports), "right2"),
        Nodes
    end,
    fun(Nodes) ->
        rt:clean_cluster(Nodes)
    end,
    fun(Nodes) -> [

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
        end}

    ] end}}.

diamond_test_() ->
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
    {timeout, 60000, {setup, fun() ->
        Conf = conf(),
        [Top, MidLeft, MidRight, Bottom] = Nodes = rt:deploy_nodes(4, Conf),
        [repl_util:make_cluster([N]) || N <- Nodes],
        Names = ["top", "midleft", "midright", "bottom"],
        [repl_util:name_cluster(Node, Name) || {Node, Name} <- lists:zip(Nodes, Names)],
        [repl_util:wait_until_is_leader(N) || N <- Nodes],
        PortMap = lists:map(fun(Node) ->
            Port = get_cluster_mgr_port(Node),
            {Node, Port}
        end, Nodes),
        connect_rt(Top, proplists:get_value(MidLeft, PortMap), "midleft"),
        connect_rt(MidLeft, proplists:get_value(Bottom, PortMap), "bottom"),
        connect_rt(MidRight, proplists:get_value(Bottom, PortMap), "bottom"),
        connect_rt(Top, proplists:get_value(MidRight, PortMap), "midright"),
        Nodes
    end,
    fun(Nodes) ->
        rt:clean_cluster(Nodes)
    end,
    fun(Nodes) -> [

        {"unfortunate double write", timeout, 10000, fun() ->
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

        {"start at midright", timeout, 10000, fun() ->
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
            %?assertEqual(Bin, maybe_eventually_exists(MidLeft, Bucket, Bin)),

            Status2 = rpc:call(MidRight, riak_repl2_rt, status, []),
            [Sink2] = proplists:get_value(sinks, Status2, [[]]),
            GotSeq = proplists:get_value(expect_seq, Sink2),
            ?assertEqual(ExpectSeq, GotSeq)
        end}

    ] end}}.

circle_and_spurs_test_() ->
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
    {timeout, 60000, {setup, fun() ->
        Conf = conf(),
        [North, East, West, NorthSpur, EastSpur, WestSpur] = Nodes = rt:deploy_nodes(6, Conf),
        [repl_util:make_cluster([N]) || N <- Nodes],
        Names = ["north", "east", "west", "north_spur", "east_spur", "west_spur"],
        [repl_util:name_cluster(Node, Name) || {Node, Name} <- lists:zip(Nodes, Names)],
        [repl_util:wait_until_is_leader(N) || N <- Nodes],
        connect_rt(North, get_cluster_mgr_port(East), "east"),
        connect_rt(East, get_cluster_mgr_port(West), "west"),
        connect_rt(West, get_cluster_mgr_port(North), "north"),
        connect_rt(North, get_cluster_mgr_port(NorthSpur), "north_spur"),
        connect_rt(East, get_cluster_mgr_port(EastSpur), "east_spur"),
        connect_rt(West, get_cluster_mgr_port(WestSpur), "west_spur"),
        Nodes
    end,
    fun(Nodes) ->
        rt:clean_cluster(Nodes)
    end,
    fun(Nodes) -> [

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

        {"spurs don't replicate back", timeout, 30000, fun() ->
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
        end}

    ] end}}.

mixed_version_clusters_test_() ->
    %      +-----+
    %      | n12 |
    %      +-----+
    %      ^      \
    %     /        V
    % +-----+    +-----+
    % | n56 | <- | n34 |
    % +-----+    +-----+
    {timeout, 60000, {setup, fun() ->
        Conf = conf(),
        DeployConfs = [{previous, Conf} || _ <- lists:seq(1,6)],
        Nodes = rt:deploy_nodes(DeployConfs),
        [N1, N2, N3, N4, N5, N6] =  Nodes,
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
    end,
    fun(Nodes) ->
        rt:clean_cluster(Nodes)
    end,
    fun([N1, N2, N3, N4, N5, N6] = Nodes) -> [

        {"no cascading at first", timeout, 20000, [
            {timeout, 10000, fun() ->
                Client = rt:pbc(N1),
                Bin = <<"no cascade yet">>,
                Obj = riakc_obj:new(?bucket, Bin, Bin),
                riakc_pb_socket:put(Client, Obj, [{w, 2}]),
                riakc_pb_socket:stop(Client),
                ?assertEqual({error, notfound}, maybe_eventually_exists([N5, N6], ?bucket, Bin)),
                ?assertEqual(Bin, maybe_eventually_exists([N3, N4], ?bucket, Bin))
            end},

            {timeout, 10000, fun() ->
                Client = rt:pbc(N2),
                Bin = <<"no cascade yet 2">>,
                Obj = riakc_obj:new(?bucket, Bin, Bin),
                riakc_pb_socket:put(Client, Obj, [{w, 2}]),
                riakc_pb_socket:stop(Client),
                ?assertEqual({error, notfound}, maybe_eventually_exists([N5, N6], ?bucket, Bin)),
                ?assertEqual(Bin, maybe_eventually_exists([N3, N4], ?bucket, Bin))
            end}
        ]},

        {"mixed source can send", timeout, 20000, {setup,
            fun() ->
                rt:upgrade(N1, current)
            end,
            fun(_) -> [

                {"node1 put", timeout, 15000, fun() ->
                    Client = rt:pbc(N1),
                    Bin = <<"rt after upgrade">>,
                    Obj = riakc_obj:new(?bucket, Bin, Bin),
                    riakc_pb_socket:put(Client, Obj, [{w, 2}]),
                    riakc_pb_socket:stop(Client),
                    ?assertEqual({error, notfound}, maybe_eventually_exists(N5, ?bucket, Bin, 100, 1000)),
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
                    ?assertEqual(Bin, maybe_eventually_exists(N3, ?bucket, Bin, 100, 1000))
                end},

                {"node2 put", timeout, 10000, fun() ->
                    Client = rt:pbc(N2),
                    Bin = <<"rt after upgrade 2">>,
                    Obj = riakc_obj:new(?bucket, Bin, Bin),
                    riakc_pb_socket:put(Client, Obj, [{w, 2}]),
                    riakc_pb_socket:stop(Client),
                    ?assertEqual({error, notfound}, maybe_eventually_exists(N5, ?bucket, Bin)),
                    ?assertEqual(Bin, maybe_eventually_exists([N3,N4], ?bucket, Bin))
                end}
            ] end
        }},

        {"upgrade the world, cascade starts working", timeout, 30000, {setup,
            fun() ->
                [N1 | NotUpgraded] = Nodes,
                [rt:upgrade(Node, current) || Node <- NotUpgraded],
                repl_util:wait_until_leader_converge([N1, N2]),
                repl_util:wait_until_leader_converge([N3, N4]),
                repl_util:wait_until_leader_converge([N5, N6]),
                maybe_reconnect_rt(N1, get_cluster_mgr_port(N3), "n34"),
                maybe_reconnect_rt(N3, get_cluster_mgr_port(N5), "n56"),
                maybe_reconnect_rt(N5, get_cluster_mgr_port(N1), "n12"),
                ok
            end,
            fun(_) ->
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
                    {Name, timeout, 10000, Test}
                end,
                [MakeTest(Node, N) || Node <- Nodes, N <- lists:seq(1, 3)]
            end
        }}

    ] end}}.

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
    {timeout, 60000, {setup, fun() ->
        Conf = conf(),
        DeployConfs = [{current, Conf}, {previous, Conf}, {current, Conf}],
        [New1, Old2, New3] = Nodes = rt:deploy_nodes(DeployConfs),
        [repl_util:make_cluster([N]) || N <- Nodes],
        Names = ["new1", "old2", "new3"],
        [repl_util:name_cluster(Node, Name) || {Node, Name} <- lists:zip(Nodes, Names)],
        [repl_util:wait_until_is_leader(N) || N <- Nodes],
        connect_rt(New1, 10026, "old2"),
        connect_rt(Old2, 10036, "new3"),
        connect_rt(New3, 10016, "new1"),
        Nodes
    end,
    fun(Nodes) ->
        rt:clean_cluster(Nodes)
    end,
    fun([New1, Old2, New3]) -> [

        {"From new1 to old2", timeout, 30000, fun() ->
            Client = rt:pbc(New1),
            Bin = <<"new1 to old2">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(Old2, ?bucket, Bin)),
            ?assertEqual({error, notfound}, maybe_eventually_exists(New3, ?bucket, Bin))
        end},

        {"old2 does not cascade at all", timeout, 30000, fun() ->
            Client = rt:pbc(New1),
            Bin = <<"old2 no cascade">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(Old2, ?bucket, Bin)),
            ?assertEqual({error, notfound}, maybe_eventually_exists(New3, ?bucket, Bin))
        end},

        {"from new3 to old2", timeout, 30000, fun() ->
            Client = rt:pbc(New3),
            Bin = <<"new3 to old2">>,
            Obj = riakc_obj:new(?bucket, Bin, Bin),
            riakc_pb_socket:put(Client, Obj, [{w, 1}]),
            riakc_pb_socket:stop(Client),
            ?assertEqual(Bin, maybe_eventually_exists(New1, ?bucket, Bin)),
            ?assertEqual(Bin, maybe_eventually_exists(Old2, ?bucket, Bin))
        end},

        {"from old2 to new3 no cascade", timeout, 30000, fun() ->
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
        end}

    ] end}}.

%% =====
%% utility functions for teh happy
%% ====

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
        {diff_batch_size, 10}
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
    maybe_eventually_exists(Node, Bucket, Key, 10, 1000).

maybe_eventually_exists(Node, Bucket, Key, MaxTries, WaitMs) when is_atom(Node) ->
    maybe_eventually_exists([Node], Bucket, Key, MaxTries, WaitMs);

maybe_eventually_exists(Nodes, Bucket, Key, MaxTries, WaitMs) ->
    Got = exists(Nodes, Bucket, Key),
    maybe_eventually_exists(Got, Nodes, Bucket, Key, MaxTries - 1, WaitMs).

maybe_eventually_exists({error, notfound}, Nodes, Bucket, Key, MaxTries, WaitMs) when MaxTries > 0 ->
    timer:sleep(WaitMs),
    Got = exists(Nodes, Bucket, Key),
    maybe_eventually_exists(Got, Nodes, Bucket, Key, MaxTries - 1, WaitMs);

maybe_eventually_exists({ok, RiakObj}, _Nodes, _Bucket, _Key, _MaxTries, _WaitMs) ->
    riakc_obj:get_value(RiakObj);

maybe_eventually_exists(Got, _Nodes, _Bucket, _Key, _MaxTries, _WaitMs) ->
    Got.

wait_for_rt_started(Node, ToName) ->
    Fun = fun(_) ->
        Status = rpc:call(Node, riak_repl2_rt, status, []),
        Started = proplists:get_value(started, Status, []),
        lists:member(ToName, Started)
    end,
    rt:wait_until(Node, Fun).

