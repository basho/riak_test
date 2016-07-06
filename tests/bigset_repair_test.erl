%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% read repair for a subset query
%%% @end
%%% Created :  7 June 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_repair_test).

-export([confirm/0]).

-define(SET, <<"test_set">>).

-include_lib("eunit/include/eunit.hrl").

-define(ELEM_BASE, "test").

confirm() ->
    lager:info("Testing Subset Query Read Repair"),

    lager:info("Start cluster"),


    Config = [{riak_core, [ {ring_creation_size, 16},
                            {vnode_management_timer, 1000},
                            {handoff_concurrency, 0} %% turn OFF handoff!
                          ]}],


    [N1, N2]=Nodes = rt:deploy_nodes(2, Config, [bigset]),
    rt:join_cluster(Nodes),
    N1Client = bigset_client:new(N1),
    N2Client = bigset_client:new(N2),

    Elements = [ make_element(I)  || I <- lists:seq(1, 25)],

    ok = bigset_client:update(?SET, Elements, N1Client),

    [E1, E3, E25, E1000, E1001, E3000] = [make_element(N) || N <- [1, 3, 25, 1000, 1001, 3000]],

    {ok, Member} = bigset_client:is_member(?SET, E25, [], N2Client),


    {ok, Subset} = bigset_client:is_subset(?SET, [E1, E3, E1000], [], N1Client),

    ?assertEqual({ok, []}, bigset_client:is_subset(?SET, [E1000, E3000], [], N1Client)),
    ?assertEqual({ok, []}, bigset_client:is_member(?SET, E1001, [], N1Client)),

    ?assertEqual(2, length(Subset)),
    assert_elem_present(E1, Subset),
    assert_elem_present(E3, Subset),
    assert_elem_absent(E1000, Subset),

    ?assertEqual(1, length(Member)),
    assert_elem_present(E25, Member),

    %% partition and change on both sides so that sets are divergent

    lager:info("Partitioning"),

    PartInfo = rt:partition([N1], [N2]),

    lager:info("update ~p", [N1]),

    %% add and remove from one side
    ToRem1 = lists:keyfind(E1, 1, Subset), %% contains the ctx
    ToRem2 = lists:keyfind(E3, 1, Subset),

    ?assertNotEqual(false, ToRem1),
    ?assertNotEqual(false, ToRem2),

    NewElem = make_element(4000),
    ok = bigset_client:update(?SET, [NewElem], [ToRem1, ToRem2], [], N1Client),

    PRead = read(N1Client),

    ?assertMatch({_, _}, lists:keyfind(NewElem, 1, PRead)),
    ?assertEqual(false, lists:keyfind(E1, 1, PRead)),
    ?assertEqual(false, lists:keyfind(E3, 1, PRead)),


    %% concurrently re-add E1  on otherside
    ok = bigset_client:update(?SET, [E1], N1Client),

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
    assert_elem_absent(E3, C1Res),

    %% Still present
    assert_elem_present(E1, C1Res),
    {RemElem1, NewCtx} = lists:keyfind(E1, 1, C1Res),
    {RemElem1, OldCtx} = ToRem1,
    ?assertNotEqual(OldCtx, NewCtx),

    %% New element present
    assert_elem_present(NewElem, C1Res),

    %% now partition again and check that both sides are equal (with
    %% no handoff, read repair must be the only explanation)

    lager:info("Partitioning"),

    PartInfo = rt:partition([N1], [N2]),

    C1PartRead= read(N1Client),
    C2PartRead  = read(N2Client),

    ?assertEqual(C1PartRead, C2PartRead),

    pass.

make_element(I) when is_integer(I) ->
    list_to_binary(?ELEM_BASE ++ integer_to_list(I)).

assert_elem_present(Elem, ElemList) ->
    ?assertMatch({_, _}, lists:keyfind(Elem, 1, ElemList)).

assert_elem_absent(Elem, ElemList) ->
    ?assertEqual(false,  lists:keyfind(Elem, 1, ElemList)).

read(Client) ->
    {ok, {ctx, _Ctx}, {elems, E1}} = bigset_client:read(?SET, [], Client),
    E1.

