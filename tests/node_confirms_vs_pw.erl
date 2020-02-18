%% -------------------------------------------------------------------
%%% @copyright (C) 2017, NHS Digital
%%% @doc
%%% riak_test for pw vs node_confirms behaviour.
%%%
%%% when using w=3, pw=2 in the attempt to prevent data loss by writing 
%%% primary nodes only to ensure the write goes to more than one physical 
%%% node, one ends up rejecting writes in the case of more than one node
%%% going down.
%%% node_confirms solves this issue by writing to both primary and
%%% fallback nodes, ensuring that the writes are to different physical nodes.
%%%
%%% This test demonstrates that of writing to a bucket with pw=2 when 2 nodes
%%% from the preflist are down will be rejected, whereas the same situation with
%%% node_confirms=2 returns a successful write.
%%% Finally, it demonstrates that write to a bucket with a node_confirms value 
%%% that cannot be met will be rejected.
%%%
%%% @end

-module(node_confirms_vs_pw).
-behavior(riak_test).
-compile([export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"bucket">>).
-define(KEY, <<"key">>).

confirm() ->
    Conf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [{allow_mult, true},
                                                 {dvv_enabled, true},
                                                 {ring_creation_size, 8},
                                                 {vnode_management_timer, 1000},
                                                 {handoff_concurrency, 100},
                                                 {vnode_inactivity_timeout, 1000}]}]},
            {bitcask, [{sync_strategy, o_sync}, {io_mode, nif}]}],

    [Node1|_] = rt:build_cluster(5, Conf),
    
    % Get preflist, we need to find two primary nodes to stop for this bucket/key
    PL = rt:get_preflist(Node1, ?BUCKET, ?KEY),
    lager:info("Got preflist"),
    lager:info("Preflist ~p~n", [PL]),
    [FirstNode | OtherPrimaries] = [Node || {{_Idx, Node}, Type} <- PL,
                                    Type == primary],
    lager:info("Other Primaries ~p~n", [OtherPrimaries]),

    [rt:stop_and_wait(N) || N <- OtherPrimaries],
    lager:info("Killed 2 primaries"),
    %% Wait for prelist to change to 1 primary, 2 fallbacks
    wait_for_new_preflist(FirstNode, 1, 2),

    Client = rt:httpc(FirstNode),

    %% Now write test for pw=2, node_confirms=0. Should fail, as only one primary available
    lager:info("Change bucket properties to pw:2 node_confirms:0"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 2}, {'node_confirms', 0}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 2}, {'node_confirms', 0}]),
    lager:info("Attempting to write key"),
    %% Write key and confirm error pw=2 unsatisfied
    ?assertMatch({error, {ok,"503",_,<<"PW-value unsatisfied: 1/2\n">>}},
                 rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>)),

    %% Now write test for pw=0, node_confirms=2. Should pass, as three physical nodes available
    lager:info("Change bucket properties to pw:0 node_confirms:2"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 0}, {'node_confirms', 2}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 0}, {'node_confirms', 2}]),
    %% Write key
    lager:info("Attempting to write key"),
    %% write key and confirm success
    ?assertMatch(ok, rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>)),
    lager:info("Read back key with node_confirms = 2"),
    {ok, ObjNC2} = rhc:get(Client, ?BUCKET, ?KEY),
    ?assertMatch(<<"12345">>, riakc_obj:get_value(ObjNC2)),

    %% Negative tests
    %% Now write test for pw=0, node_confirms=4. Should fail, as node_confirms should not be greater than n_val (3)
    lager:info("Change bucket properties to pw:0 node_confirms:4"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 0}, {'node_confirms', 4}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 0}, {'node_confirms', 4}]),
    %% Write key
    lager:info("Attempting to write key"),
    %% Write key and confirm error invalid pw/node_confirms
    ?assertMatch({error, {ok,"400",_,<<"Specified w/dw/pw/node_confirms values invalid for bucket n value of 3\n">>}},
                 rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>)),

    %% Now stop another node and write test for pw=0, node_confirms=3. Should fail, as only two physical nodes available
    PL2 = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
    lager:info("Got preflist"),
    lager:info("Preflist ~p~n", [PL2]),

    lager:info("Change bucket properties to pw:0 node_confirms:3"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 0}, {'node_confirms', 3}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 0}, {'node_confirms', 3}]),
    Others = [Node || {{_Idx, Node}, _Type} <- PL2, Node /= FirstNode],
    OtherFailedNode = lists:last(Others),
    rt:stop_and_wait(OtherFailedNode),
    wait_for_new_preflist(FirstNode, PL2),
    PL3 = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
    lager:info("Preflist ~p~n", [PL3]),

    %% How many nodes are up?
    RemainingUpNodes = [UpNode2 || {{_Idx, UpNode2}, _Type} <- PL3],
    UpNodeCount = length(lists:usort(RemainingUpNodes)),
    {PriCount3, _FallCount3} = primary_and_fallback_counts(PL3),
    lager:info("Left with ~w nodes ~w and ~w primary vnodes",
                [UpNodeCount, RemainingUpNodes, PriCount3]),

    lager:info("Attempting to write key"),
    %% Write key and confirm error node_confirms=3 unsatisfied
    ?assertMatch({error, {ok,"503",_,<<"node_confirms-value unsatisfied: 2/3\n">>}},
                 rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12346">>)),

    lager:info("Setting pw and node_confirms to reflect current status"),
    rpc:call(FirstNode,
                riak_core_bucket, set_bucket,
                [?BUCKET,
                    [{'pw', PriCount3}, {'node_confirms', UpNodeCount}]]),
    rt:wait_until_bucket_props([FirstNode],
                                ?BUCKET,
                                    [{'pw', PriCount3},
                                        {'node_confirms', UpNodeCount}]),
    
    lager:info("Read back key"),
    lager:info("Although previous put failed, it will have been co-ordinated"),
    lager:info("We can expect to see siblings - both PUTs should be present"),
    lager:info("Success occurs as enough nodes and primaries are up"),
    {ok, ObjS2} = rhc:get(Client, ?BUCKET, ?KEY),
    ?assertMatch([<<"12345">>, <<"12346">>],
                    lists:usort(riakc_obj:get_values(ObjS2))),
    lager:info("Incrementing node_confirms to cause failure"),
    rpc:call(FirstNode,
                riak_core_bucket, set_bucket,
                [?BUCKET,
                    [{'node_confirms', UpNodeCount + 1}]]),
    rt:wait_until_bucket_props([FirstNode],
                                ?BUCKET,
                                    [{'node_confirms', UpNodeCount + 1}]),
    {error, {ok, "500", _Headers, Error}} = rhc:get(Client, ?BUCKET, ?KEY),
    lager:info("Error ~p now returned", [Error]),
    ExpectedError =
        list_to_binary(
            io_lib:format("Error:\n{insufficient_nodes,~w,need,~w}\n",
                            [UpNodeCount, UpNodeCount + 1])),
    ?assertMatch(ExpectedError, Error),
    
    pass.


primary_and_fallback_counts(PL) ->
    lists:foldl(fun({{_, _}, primary}, {P, F}) ->
                        {P+1, F};
                   ({{_, _}, fallback}, {P, F}) ->
                        {P, F+1}
                end,
                {0, 0},
PL).

partition_compare(OldPL, NewPL) ->
    Old = [Part || {{_, Part}, _} <- OldPL],
    New = [Part || {{_, Part}, _} <- NewPL],
    Old == New.

wait_for_new_preflist(FirstNode, Primaries, Fallbacks) ->
    rt:wait_until(fun() ->
                          NewPL = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
                          primary_and_fallback_counts(NewPL) == {Primaries, Fallbacks}
    end).

wait_for_new_preflist(FirstNode, OldPL) ->
    rt:wait_until(fun() ->
                          NewPL = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
                          not partition_compare(OldPL, NewPL)
    end).

