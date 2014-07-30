%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
%% -------------------------------------------------------------------
-module(cluster_meta_rmr).
-behavior(riak_test).
-export([confirm/0]).

-define(CM_PREFIX, {test, cm}).

confirm() ->
    rt_config:set_conf(all, [{"ring_size", "128"}]),
    Seed = erlang:now(),
    lager:info("SEED: ~p", [Seed]),
    random:seed(Seed),
%    run([10,20,40], 10, 50, 10).
    run([5], 1, 5, 1),
    pass.

run(NumNodes, SuperRounds, NumRounds, StableRounds) when is_list(NumNodes) ->
    [begin
         [begin
              lager:info("starting super round ~p: ~p nodes ~p total rounds ~p stable rounds",
                         [S, N, NumRounds, StableRounds]),
              run(N, NumRounds, StableRounds)
          end || S <- lists:seq(1, SuperRounds)]
     end || N <- NumNodes].

run(NumNodes, NumRounds, StableRounds) ->
    {ok, Pid} = cluster_meta_proxy_server:start_link(),
    unlink(Pid),
    AllNodes = setup_nodes(NumNodes),
    %% ensures any gossip messages being proxied during initial cluster setup
    %% are drained before we proceed
%    wait_until_no_messages(),
%    {ok, R} = rpc:call(hd(AllNodes), riak_core_ring_manager, get_my_ring, []),
%    Nodes = riak_core_ring:active_members(R),
%    lager:info("GOSSIP TREE: ~p", [riak_core_util:build_tree(2, Nodes, [cycles])]),
    lager:info("running ~p broadcast rounds on nodes: ~p", [NumRounds, AllNodes]),
    DownNodes = run_rounds(NumRounds, StableRounds, fun broadcast/2, fun wait_until_broadcast_consistent/2, AllNodes, []),
%    lager:info("running ~p gossip rounds on nodes: ~p", [NumRounds, AllNodes]),
%    run_rounds(NumRounds, fun gossip/2, fun wait_until_gossip_consistent/2, AllNodes),
    calc_stuff(AllNodes, NumNodes, NumRounds),
    exit(Pid, kill),
    %% start all the down nodes so we can clean them :(
    [rt:start(Node) || Node <- DownNodes],
    rt_cluster:clean_cluster(AllNodes).

setup_nodes(NumNodes) ->
    Nodes = rt_cluster:build_cluster(NumNodes),
    [begin
         ok = rpc:call(Node, application, set_env, [riak_core, broadcast_exchange_timer, 4294967295]),
         ok = rpc:call(Node, application, set_env, [riak_core, gossip_limit, {10000000, 4294967295}]),
         rt_intercept:add(Node, {riak_core_broadcast, [{{send,2}, global_send}]})
     end || Node <- Nodes],
    Nodes.

run_rounds(0, _, _, _, _, DownNodes) ->
    DownNodes;
run_rounds(_, _, _, _, [_SenderNode], DownNodes) ->
    lager:info("ran out of nodes to shut down"),
    DownNodes;
run_rounds(Round, 0, SendFun, ConsistentFun, [SenderNode | OtherNodes]=UpNodes, DownNodes) ->
    lager:info("round ~p (unstable): starting", [Round]),
    %% get down nodes too just so it prints nicer, debug_get_tree handles them being down
    Tree = rpc:call(SenderNode, riak_core_broadcast, debug_get_tree, [SenderNode, UpNodes ++ DownNodes]),
    lager:info("round ~p (unstable): tree before sending ~p", [Round, Tree]),
    {FailedNode, RemainingNodes} = fail_node(Round, OtherNodes),
    NewUpNodes = [SenderNode | RemainingNodes],
    SendFun(SenderNode, Round),
    lager:info("round: ~p (unstable): waiting until updates have reached all running nodes", [Round]),
    try ConsistentFun(NewUpNodes, Round) of
        _ ->
            run_rounds(Round - 1, 0, SendFun, ConsistentFun, NewUpNodes, [FailedNode | DownNodes])
    catch
        _:_ ->
            NumDown = length([FailedNode | DownNodes]),
            NumUp = length(NewUpNodes),
            Total = NumDown + NumUp,
            lager:error("round ~p (unstable): consistency check failed w/ ~p down ~p up, total ~p",
                        [Round, NumDown, NumUp, Total]),
            [FailedNode | DownNodes]
    end;
run_rounds(Round, StableRound, SendFun, ConsistentFun, [SenderNode | _]=UpNodes, DownNodes) ->
    lager:info("round ~p (stable): starting", [Round]),
    SendFun(SenderNode, Round),
    lager:info("round ~p (stable): waiting until there are no messages left", [Round]),
    wait_until_no_messages(),
    lager:info("round ~p (stable): waiting until updates have reached all running nodes", [Round]),
    ConsistentFun(UpNodes, Round),
    run_rounds(Round - 1, StableRound - 1, SendFun, ConsistentFun, UpNodes, DownNodes).

fail_node(Round, OtherNodes) ->
    Failed = lists:nth(random:uniform(length(OtherNodes)), OtherNodes),
    lager:info("round: ~p (unstable): shutting down ~p", [Round, Failed]),
    rt_node:stop(Failed),
    {Failed, lists:delete(Failed, OtherNodes)}.

calc_stuff(AllNodes, NumNodes, NumRounds) ->
    History = cluster_meta_proxy_server:history(),
    %% GossipHistory = [{From, To, element(2, riak_core_ring:get_meta(round, R))} ||
    %%                     {From, To, {reconcile_ring, R}} <- History, riak_core_ring:get_meta(round, R) =/= undefined],
    %% lager:info("GOSSIP HISTORY:"),
    %% [lager:info("~p", [X]) || X <- GossipHistory],
    ResultsDict = calc_stuff(AllNodes, History),
    ResultsList = lists:reverse(orddict:to_list(ResultsDict)),
    ResultsFileName = io_lib:format("results-~p.csv", [NumNodes]),
    {ok, ResultsFile} = file:open(ResultsFileName, [write]),
    io:format(ResultsFile, "round,broadcastrmr,gossiprmr,broadcastldh,gossipldh~n", []),
    [io:format(ResultsFile, "~p,~p,~p,~p,~p~n", [abs(Round - NumRounds), BRMR, GRMR, BLDH, GLDH])
     || {{round, Round}, {{BRMR, GRMR}, {BLDH, GLDH}}} <- ResultsList],
    lager:info("NumNodes: ~p NumRounds: ~p RESULTS: ~p", [NumNodes, NumRounds, ResultsList]).

calc_stuff(AllNodes, History) ->
    CountDict = lists:foldl(fun process_message/2, orddict:new(), History),
    RMRN = length(AllNodes) - 1,
    orddict:fold(fun(TestRound, Info, AccDict) ->
                         {BroadcastCount, BroadcastLDH} = proplists:get_value(broadcast, Info),
                         {GossipCount, GossipLDH} = proplists:get_value(gossip, Info, {0, 0}),
                         BroadcastRMR = (BroadcastCount / RMRN) - 1,
                         GossipRMR = (GossipCount / RMRN) - 1,
                         %% add 1 to LDHs since rounds are zero based
                         orddict:store(TestRound, {{BroadcastRMR, GossipRMR}, {BroadcastLDH+1, GossipLDH+1}}, AccDict)
                 end, orddict:new(), CountDict).


process_message({_From, _To,
                 {broadcast, {{?CM_PREFIX, {round, TestRound}}, _Ctx}, _Payload, _Mod, BCastRound, _Root, _From}},
                ResultsDict) ->
    case orddict:find({round, TestRound}, ResultsDict) of
        error ->
            orddict:store({round, TestRound}, [{broadcast, {1, BCastRound}}], ResultsDict);
        {ok, Info} ->
            {CurrentCount, LastBCastRound} = proplists:get_value(broadcast, Info, {1, BCastRound}),
            NewInfo = lists:keystore(broadcast, 1, Info, {broadcast, {CurrentCount+1, max(LastBCastRound, BCastRound)}}),
            orddict:store({round, TestRound}, NewInfo, ResultsDict)
    end;
%process_message({_From, _To,
%                 {reconcile_ring, OutRing}}, {CountDict, LDHDict}) ->
%    CountDict1 = case riak_core_ring:get_meta(round, OutRing) of
%                     undefined -> CountDict; %% not a gossip message we sent (e.g. owneship change building cluster)
%                     {ok, Round} -> orddict:update_counter({gossip, Round}, 1, CountDict)
%                 end,
%    {CountDict1, LDHDict};
process_message({_From, _To,
                 {reconcile_ring, GossipRound, OutRing}}, ResultsDict) ->
    case riak_core_ring:get_meta(round, OutRing) of
        undefined -> ResultsDict;
        {ok, TestRound} ->
        case orddict:find({round, TestRound}, ResultsDict) of
            error ->
                orddict:store({round, TestRound}, [{gossip, {1, GossipRound}}], ResultsDict);
            {ok, Info} ->
                {CurrentCount, LastGossipRound} = proplists:get_value(gossip, Info, {1, GossipRound}),
                NewInfo = lists:keystore(gossip, 1, Info, {gossip, {CurrentCount+1, max(LastGossipRound, GossipRound)}}),
                orddict:store({round, TestRound}, NewInfo, ResultsDict)
        end
    end;
process_message(_Msg, Acc) ->
    Acc.

broadcast(SenderNode, Round) ->
    %% TODO: don't use metadata manager?
    Key = mk_key(Round),
    Value = mk_value(Round),
    ok = rpc:call(SenderNode, riak_core_metadata, put, [?CM_PREFIX, Key, Value]).

%gossip(SenderNode, Round) ->
%    Value = mk_value(Round),
%    {ok, _} = rpc:call(SenderNode, riak_core_ring, update_round, [Value]).

wait_until_no_messages() ->
    F = fun() ->
                cluster_meta_proxy_server:is_empty(5)
        end,
    ok = rt:wait_until(F).

wait_until_broadcast_consistent(Nodes, Round) ->
    Key = mk_key(Round),
    Value = mk_value(Round),
    wait_until_metadata_value(Nodes,Key, Value).

wait_until_metadata_value(Nodes, Key, Val) when is_list(Nodes) ->
    [wait_until_metadata_value(Node, Key, Val) || Node <- Nodes];
wait_until_metadata_value(Node, Key, Val) ->
    F = fun() ->
                %% no need to resolve b/c we use a single sender
                Val =:= metadata_get(Node, Key)
        end,
    ok = rt:wait_until(F, 10, 500).

metadata_get(Node, Key) ->
    rpc:call(Node, riak_core_metadata, get, [?CM_PREFIX, Key]).

%wait_until_gossip_consistent(Nodes, Round) ->
%    Value = mk_value(Round),
%    wait_until_bucket_value(Nodes, Value).

%wait_until_bucket_value(Nodes, Val) when is_list(Nodes) ->
%    [wait_until_bucket_value(Node, Val) || Node <- Nodes];
%wait_until_bucket_value(Node, Val) ->
%    F = fun() ->
%                {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
%                {ok, Val} =:= riak_core_ring:get_meta(round, Ring)
%        end,
%    ok = rt:wait_until(F).

mk_key(Round) ->
    {round, Round}.

mk_value(Round) ->
    Round.
