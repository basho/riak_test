%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% Checks that subset queries across a quorum merge as expected. In
%%% order to do that we disable handoff, update partitioned nodes,
%%% then heal and read from a quorum to ensure merge works.
%%% @end
%%% Created :  7 June 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_subset_query).

-export([confirm/0]).

-define(SET, <<"test_set">>).

-include_lib("eunit/include/eunit.hrl").

-define(ELEM_BASE, "test").

confirm() ->
    lager:info("Testing Subset Reads"),

    lager:info("Start cluster"),


    Config = [{riak_core, [ {ring_creation_size, 16},
                            {vnode_management_timer, 1000}
                          ]}],

    [N1, N2]=Nodes = rt:deploy_nodes(2, Config, [bigset]),
    rt:join_cluster(Nodes),
    N1Client = bigset_client:new(N1),
    N2Client = bigset_client:new(N2),

    Elements = [ make_element(I)  || I <- lists:seq(1, 50)],

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

    pass.

make_element(I) when is_integer(I) ->
    list_to_binary(?ELEM_BASE ++ integer_to_list(I)).

assert_elem_present(Elem, ElemList) ->
    ?assertMatch({_, _}, lists:keyfind(Elem, 1, ElemList)).

assert_elem_absent(Elem, ElemList) ->
    ?assertEqual(false,  lists:keyfind(Elem, 1, ElemList)).
