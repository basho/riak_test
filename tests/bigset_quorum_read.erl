%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% Checks that reads across a quorum merge as expected. In order to
%%% do that we disable handoff, update partitioned nodes, then heal
%%% and read from a quorum to ensure merge works.
%%% @end
%%% Created :  7 June 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_quorum_read).

-export([confirm/0]).

-define(SET, <<"test_set">>).

-include_lib("eunit/include/eunit.hrl").

-define(ELEM_BASE, "test").

confirm() ->
    lager:info("Testing Quorum reads"),

    lager:info("Start cluster"),


    Config = [{riak_core, [ {ring_creation_size, 16},
                            {vnode_management_timer, 1000},
                            {handoff_concurrency, 0} %% turn OFF handoff!
                          ]}],

    [N1, N2]=Nodes = rt:deploy_nodes(2, Config, [bigset]),
    rt:join_cluster(Nodes),
    N1Client = bigset_client:new(N1),
    N2Client = bigset_client:new(N2),

    Elements = [ make_element(I)  || I <- lists:seq(1, 50)],

    ok = bigset_client:update(?SET, Elements, N1Client),

    lager:info("elements added"),

    {ok, {ctx, <<>>}, {elems, E1}} = bigset_client:read(?SET, [], N1Client),
    {ok, {ctx, <<>>}, {elems, E2}} = bigset_client:read(?SET, [], N2Client),

    ?assertEqual(E1, E2),
    ?assertEqual(50, length(E1)),

    lager:info("Partitioning"),

    PartInfo = rt:partition([N1], [N2]),

    lager:info("update ~p", [N1]),

    RemElem1 = make_element(1),
    RemElem2 = make_element(2),

    %% add and remove from one side
    ToRem1 = lists:keyfind(RemElem1, 1, E1), %% contains the ctx
    ToRem2 = lists:keyfind(RemElem2, 1, E1),

    ?assertNotEqual(false, ToRem1),
    ?assertNotEqual(false, ToRem2),

    NewElem = make_element(1000),
    ok = bigset_client:update(?SET, [NewElem], [ToRem1, ToRem2], [], N1Client),

    PRead = read(N1Client),

    ?assertMatch({_, _}, lists:keyfind(NewElem, 1, PRead)),
    ?assertEqual(false, lists:keyfind(RemElem1, 1, PRead)),
    ?assertEqual(false, lists:keyfind(RemElem2, 1, PRead)),


    %% concurrently add RemElem1 on otherside
    ok = bigset_client:update(?SET, [RemElem1], N1Client),

    %% Heal
    lager:info("Healing"),

    ok = rt:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, bigset),

    %% Since no handoff, a quorum read is the only way to get a
    %% convergent/correct view of the set

    C1Res  = read(N1Client),
    C2Res  = read(N2Client),

    ?assertEqual(C1Res, C2Res),

    %% really removed
    ?assertEqual(false, lists:member(ToRem2, C1Res)),
    assert_elem_absent(RemElem2, C1Res),

    %% Still present
    assert_elem_present(RemElem1, C1Res),
    {RemElem1, NewCtx} = lists:keyfind(RemElem1, 1, C1Res),
    {RemElem1, OldCtx} = ToRem1,
    ?assertNotEqual(OldCtx, NewCtx),

    %% New element present
    assert_elem_present(NewElem, C1Res),

    pass.

make_element(I) when is_integer(I) ->
    list_to_binary(?ELEM_BASE ++ integer_to_list(I)).

read(Client) ->
    {ok, {ctx, <<>>}, {elems, E1}} = bigset_client:read(?SET, [], Client),
    E1.

assert_elem_present(Elem, ElemList) ->
    ?assertMatch({_, _}, lists:keyfind(Elem, 1, ElemList)).

assert_elem_absent(Elem, ElemList) ->
    ?assertEqual(false,  lists:keyfind(Elem, 1, ElemList)).
