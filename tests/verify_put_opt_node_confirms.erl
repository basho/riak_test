%% -------------------------------------------------------------------
%%% @copyright (C) 2017, NHS Digital
%%% @doc
%%% riak_test for pw vs node_confirms behaviour. Via clients.
%%%
%%% when using w=3, pw=2 in the attempt to prevent data loss by
%%% writing primary nodes only to ensure the write goes to more than
%%% one physical node, one ends up rejecting writes in the case of
%%% more than one node going down.  node_confirms solves this issue by
%%% writing to both primary and fallback nodes, ensuring that the
%%% writes are to different physical nodes.
%%%
%%% This test demonstrates that of writing to a bucket with pw=2 when
%%% 2 nodes from the preflist are down will be rejected, whereas the
%%% same situation with node_confirms=2 returns a successful write.
%%% Finally, it demonstrates that write to a bucket with a
%%% node_confirms value that cannot be met will be rejected.
%%%
%%% @end

-module(verify_put_opt_node_confirms).
-behavior(riak_test).
-compile([export_all, nowarn_export_all]).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"bucket">>).
-define(KEY, <<"key">>).
-define(RING_SIZE, 8).

confirm() ->
    Conf = [
            {riak_kv, [{anti_entropy, {off, []}}]},
            {riak_core, [{default_bucket_props, [{allow_mult, true},
                                                 {dvv_enabled, true},
                                                 {ring_creation_size, ?RING_SIZE},
                                                 {vnode_management_timer, 1000},
                                                 {handoff_concurrency, 100},
                                                 {vnode_inactivity_timeout, 1000}]}]},
            {bitcask, [{sync_strategy, o_sync}, {io_mode, nif}]}],

    [Node1|_] = rt:build_cluster(5, Conf),

    %% you can't chash a bucket type to get a preflist unless it
    %% exists, who knew?
    rt:create_and_activate_bucket_type(Node1, <<"sets">>, [{datatype, set}]),

    %% Get preflist, we need to find two primary nodes to stop for
    %% these bucket/keys
    {BucketTypes, PL} = find_common_preflist(Node1, [?BUCKET,
                                                     {<<"sets">>, <<"set">>},
                                                     %% 1.4 counter
                                                     <<"old_counter">>
                                                    ]),

    lager:info("Got preflist"),
    lager:info("Preflist ~p~n", [PL]),
    [FirstNode | OtherPrimaries] = [Node || {{_Idx, Node}, Type} <- PL,
                                            Type == primary],
    lager:info("Other Primaries ~p~n", [OtherPrimaries]),

    [rt:stop_and_wait(N) || N <- OtherPrimaries],
    lager:info("Killed 2 primaries"),
    %% Wait for prelist to change to 1 primary, 2 fallbacks
    wait_for_new_preflist(FirstNode, hd(BucketTypes), 1, 2),

    HttpClient = rt:httpc(FirstNode),
    PBClient = rt:pbc(FirstNode),

    lager:info("Attempting to write key"),

    %% Write key and confirm error pw=2 unsatisfied
    write_http(HttpClient, BucketTypes, {error, "503", <<"PW-value unsatisfied: 1/2\n">>}, [{pw, 2}]),
    write_pb(PBClient, BucketTypes, {error, <<"{pw_val_unsatisfied,2,1}">>}, [{pw, 2}]),

    %% Now write test for pw=0, node_confirms=2. Should pass, as three physical nodes available
    %% Write key
    lager:info("Attempting to write key"),
    %% write key and confirm success
    write_http(HttpClient, BucketTypes, ok, [{node_confirms, 2}]),
    write_pb(PBClient, BucketTypes, ok, [{node_confirms, 2}]),

    %% Negative tests

    %% Write key
    lager:info("Attempting to write key"),
    %% Write key and confirm error invalid pw/node_confirms
    write_http(HttpClient, BucketTypes,
               {error, "400", <<"Specified w/dw/pw/node_confirms values invalid for bucket n value of 3\n">>},
               [{node_confirms, 4}]),

    write_pb(PBClient, BucketTypes, {error, <<"{n_val_violation,3}">>}, [{node_confirms, 4}]),

    %% Now stop another node and write test for pw=0, node_confirms=3. Should fail, as only two physical nodes available
    PL2 = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
    lager:info("Got preflist"),
    lager:info("Preflist ~p~n", [PL2]),

    Others = [Node || {{_Idx, Node}, _Type} <- PL2, Node /= FirstNode],
    rt:stop_and_wait(lists:last(Others)),
    wait_for_new_preflist(FirstNode, hd(BucketTypes), PL2),
    PL3 = rt:get_preflist(FirstNode, ?BUCKET, ?KEY),
    lager:info("Preflist ~p~n", [PL3]),

    lager:info("Attempting to write key"),
    %% Write key and confirm error node_confirms=3 unsatisfied
    write_http(HttpClient, BucketTypes, {error, "503", <<"node_confirms-value unsatisfied: 2/3\n">>},
               [{node_confirms, 3}]),

    write_pb(PBClient, BucketTypes, {error, <<"{node_confirms_val_unsatisfied,3,2}">>}, [{node_confirms, 3}]),

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

wait_for_new_preflist(FirstNode, BT, Primaries, Fallbacks) ->
    rt:wait_until(fun() ->
                          NewPL = rt:get_preflist(FirstNode, BT, ?KEY),
                          lager:info("new pl ~p", [NewPL]),
                          primary_and_fallback_counts(NewPL) == {Primaries, Fallbacks}
    end).

wait_for_new_preflist(FirstNode, BT, OldPL) ->
    rt:wait_until(fun() ->
                          NewPL = rt:get_preflist(FirstNode, BT, ?KEY),
                          not partition_compare(OldPL, NewPL)
    end).

write_http(_Client, [], _, _) ->
    ok;
write_http(Client, [BT | Rest], Expected, Options) ->
    lager:info("http checking ~p", [BT]),
    write_http2(Client, BT, Expected,  Options),
    write_http(Client, Rest, Expected, Options).

write_http2(Client, {<<"sets">>, _B}=BT, Expected0, Options) ->
    Op0 = riakc_set:new(),
    Op1 = riakc_set:add_element(<<"test">>, Op0),
    Res = rhc:update_type(Client, BT, ?KEY, riakc_set:to_op(Op1), Options),
    DTExpected = get_http_dt_expected(Expected0),
    assert_match(DTExpected, Res);
write_http2(Client, <<"old_counter", _Rest/binary>>=B, Expected, Options) ->
    Res = rhc:counter_incr(Client, B, ?KEY, 1, Options),
    assert_match(Expected, Res);
write_http2(Client, ?BUCKET, Expected, Options) ->
    Res = rt:httpc_write(Client, ?BUCKET, ?KEY, <<"12345">>, Options),
    assert_match(Expected, Res).

%% because consistency is overated??
get_http_dt_expected(ok) ->
    ok;
%% wat? come on rhc?!?
get_http_dt_expected({error, _Code,
                      <<"Specified w/dw/pw/node_confirms values invalid for bucket n value of 3\n">>=Msg}) ->
    {error, {bad_request, Msg}};
get_http_dt_expected({error, _Code, Msg}) ->
    {error, Msg}.

write_pb(_Client, [], _Expected, _Options) ->
    ok;
write_pb(Client, [BT | Rest], Expected, Options) ->
    lager:info("pbc checking ~p", [BT]),
    Res = write_pb2(Client, BT, Options),
    assert_match(Expected, Res),
    write_pb(Client, Rest, Expected, Options).

write_pb2(Client, {<<"sets">>, _B}=BT, Options) ->
    Op0 = riakc_set:new(),
    Op1 = riakc_set:add_element(<<"test">>, Op0),
    riakc_pb_socket:update_type(Client, BT, ?KEY, riakc_set:to_op(Op1), Options);
write_pb2(Client, <<"old_counter", _Rest/binary>>=B, Options) ->
    riakc_pb_socket:counter_incr(Client, B, ?KEY, 1, Options);
write_pb2(Client, ?BUCKET, Options) ->
    rt:pbc_write(Client, ?BUCKET, ?KEY, <<"12345">>, <<"bin">>,Options).

assert_match({error, Code, Msg}, Res) ->
    ?assertMatch({error, {ok, Code, _, Msg}}, Res);
assert_match(Other, Res) ->
    ?assertMatch(Other, Res).

%% bear in mind that there are only ring_size primary preflists. Given
%% some {{T, B}, K} on average we should need ring_size trials to find
%% another {{T, B}, K}' that shares the same preflist. That is the
%% purpose of these functions. If we want to test CRDTs, and old style
%% counters, and riak_objects for node_confirms we can cut and paste
%% the same test or we can do it all at once, but we need a common
%% preflist for that to work. TODO maybe cap the number of trials?
-spec find_common_preflist(node(), [{binary(), binary()}]) ->
                                  {[{binary(), binary()}], riak_core_apl:preflist_ann()}.
find_common_preflist(Node, [Hd|BTs]) when is_list(BTs) ->
    find_common_preflist(Node, BTs, preflist(Node, Hd), [Hd]).

find_common_preflist(_Node, [], PL, BTs) ->
    {lists:reverse(BTs), PL};
find_common_preflist(Node, [BT | BTs], PL, BTsAcc) ->
    BT2 = get_matching_preflist(Node, BT, PL),
    find_common_preflist(Node, BTs, PL, [BT2 | BTsAcc]).

preflist(Node, BT) ->
    rt:get_preflist(Node, BT, ?KEY).

get_matching_preflist(Node, BT, PL) ->
    get_matching_preflist(Node, BT, preflist(Node, BT), PL).

get_matching_preflist(_Node, BT, PL, PL) ->
    BT;
get_matching_preflist(Node, BT, _NonMatch, TargetPL) ->
    NewBT = new_bucket(BT),
    get_matching_preflist(Node, NewBT, TargetPL).

new_bucket({Type, Bucket}) ->
    {Type, new_bucket(Bucket)};
new_bucket(Bucket) when is_binary(Bucket) ->
    Tail = random_char(),
    <<Bucket/binary, Tail/binary>>.

%% string like random bytes
random_char() ->
    <<(crypto:rand_uniform(65, 90))>>.
