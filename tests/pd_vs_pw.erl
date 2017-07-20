%% -------------------------------------------------------------------
%%% @copyright (C) 2017, NHS Digital
%%% @doc
%%% riak_test for pw vs pd behaviour.
%%%
%%% when using w=3, pw=2 in the attempt to prevent data loss by writing 
%%% primary nodes only to ensure the write goes to more than one physical 
%%% node, one ends up rejecting writes in the case of more than one node
%%% going down.
%%% pd (physical diversity) solves this issue by writing to both primary and
%%% fallback nodes, ensuring that the writes are to different physical nodes.
%%%
%%% This test demonstrates that of writing to a bucket with pw=2 when 2 nodes
%%% from the preflist are down will be rejected, whereas the same situation with
%%% pd=2 returns a successful write.
%%% Finally, it demonstrates that write to a bucket with a pd value that cannot
%%% be met will be rejected.
%%%
%%% @end

-module(pd_vs_pw).
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

    %% Now write test for pw=2, pd=0. Should fail, as only one primary available
    lager:info("Change bucket properties to pw:2 pd:0"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 2}, {'pd', 0}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 2}, {'pd', 0}]),
    lager:info("Attempting to write key"),
    %% Write key and confirm error pw=2 unsatisfied
    ?assertMatch({error, {ok,"503",_,<<"PW-value unsatisfied: 1/2\n">>}},
                 rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>)),

    %% Now write test for pw=0, pd=2. Should pass, as three physical nodes available
    lager:info("Change bucket properties to pw:0 pd:2"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 0}, {'pd', 2}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 0}, {'pd', 2}]),
    %% Write key
    lager:info("Attempting to write key"),
    %% write key and confirm success
    ?assertMatch(ok, rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>)),

    %% Negative tests
    %% Now write test for pw=0, pd=4. Should fail, as pd should not be greater than n_val (3)
    lager:info("Change bucket properties to pw:0 pd:4"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 0}, {'pd', 4}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 0}, {'pd', 4}]),
    %% Write key
    lager:info("Attempting to write key"),
    %% Write key and confirm error invalid pw/pd
    ?assertMatch({error, {ok,"400",_,<<"Specified w/dw/pw/pd values invalid for bucket n value of 3\n">>}},
                 rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>)),

    %% Now stop another node and write test for pw=0, pd=3. Should fail, as only two physical nodes available
    PL2 = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
    lager:info("Got preflist"),
    lager:info("Preflist ~p~n", [PL2]),

    lager:info("Change bucket properties to pw:0 pd:3"),
    rpc:call(FirstNode,riak_core_bucket, set_bucket, [?BUCKET, [{'pw', 0}, {'pd', 3}]]),
    rt:wait_until_bucket_props([FirstNode],?BUCKET,[{'pw', 0}, {'pd', 3}]),
    Others = [Node || {{_Idx, Node}, _Type} <- PL2, Node /= FirstNode],
    rt:stop_and_wait(lists:last(Others)),
    wait_for_new_preflist(FirstNode, PL2),
    PL3 = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
    lager:info("Preflist ~p~n", [PL3]),

    lager:info("Attempting to write key"),
    %% Write key and confirm error pd=3 unsatisfied
    ?assertMatch({error, {ok,"503",_,<<"PD-value unsatisfied: 2/3\n">>}},
                 rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>)),
    
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
