%%%-------------------------------------------------------------------
%%% @author doug
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Apr 2016 2:13 PM
%%%-------------------------------------------------------------------
-module(rt_cascading_big_circle).
-author("doug").

%% API
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    State = big_circle_setup(),
    _ = big_circle_tests(State),
    pass.

big_circle_setup() ->
    Conf = lists:map(fun(I) ->
        {integer_to_list(I), 1}
                     end, lists:seq(1, 6)),
    NamesAndNodes = rt_cascading:make_clusters(Conf),
    Nodes = lists:flatten([ClusterNodes || {_Name, ClusterNodes} <- NamesAndNodes]),
    Names = [ClusterName || {ClusterName, _} <- Conf],
    [NameHd | NameTail] = Names,
    ConnectTo = NameTail ++ [NameHd],
    ClustersAndConnectTo = lists:zip(NamesAndNodes, ConnectTo),
    ok = lists:foreach(fun({SourceCluster, SinkName}) ->
        {_SourceName, [Node]} = SourceCluster,
        [SinkNode] = proplists:get_value(SinkName, NamesAndNodes),
        Port = rt_cascading:get_cluster_mgr_port(SinkNode),
        rt_cascading:connect_rt(Node, Port, SinkName)
                       end, ClustersAndConnectTo),
    Nodes.


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
                 ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(Node, Bucket, Bin))
             end || Node <- Nodes]
                      end},

        {"2 way repl, and circle it", fun() ->
            ConnectTo = ["6", "1", "2", "3", "4", "5"],
            Connect = fun({Node, ConnectToName}) ->
                Nth = list_to_integer(ConnectToName),
                ConnectNode = lists:nth(Nth, Nodes),
                Port = rt_cascading:get_cluster_mgr_port(ConnectNode),
                rt_cascading:connect_rt(Node, Port, ConnectToName)
                      end,
            lists:map(Connect, lists:zip(Nodes, ConnectTo)),
            C = rt:pbc(hd(Nodes)),
            Bin = <<"2 way repl">>,
            Bucket = <<"objects">>,
            Obj = riakc_obj:new(Bucket, Bin, Bin),
            riakc_pb_socket:put(C, Obj, [{w,1}]),
            lists:map(fun(N) ->
                ?debugFmt("Testing ~p", [N]),
                ?assertEqual(Bin, rt_cascading:maybe_eventually_exists(N, Bucket, Bin))
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
            rt_cascading:wait_until_pending_count_zero(Nodes)
                           end}
    ],
    lists:foreach(fun({Name, Eval}) ->
        lager:info("===== big circle: ~s =====", [Name]),
        Eval()
                  end, Tests).

