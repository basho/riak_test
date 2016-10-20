%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% Checks that range queries work
%%% @end
%%% Created :  7 June 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_range_query).

-export([confirm/0]).

-define(SET, <<"test_set">>).

-include_lib("eunit/include/eunit.hrl").


confirm() ->
    lager:info("Testing Range Reads"),

    lager:info("Start cluster"),


    Config = [{riak_core, [ {ring_creation_size, 16},
                            {vnode_management_timer, 1000}
                          ]}],

    [N1, N2]=Nodes = rt:deploy_nodes(2, Config, [bigset]),
    rt:join_cluster(Nodes),
    N1Client = bigset_client:new(N1),
    N2Client = bigset_client:new(N2),

    Elements = make_range(1, 100),

    ok = bigset_client:update(?SET, Elements, N1Client),

    %% range start - range end
    {ok, _, {_, Res}} = bigset_client:read(?SET, [{range_start, make_element(30)},
                                                  {range_end, make_element(50)},
                                                  {end_inclusive, true}],
                                           N2Client),

    assertRange({30, 50}, Res),
    %% set start - range end
    %% range start - set end
    %% range start - range end (where range end > set end)
    %% range start - range end (where start < set start)
    %% range start > range end

    pass.

make_element(I) when is_integer(I) ->
    <<I:32/big-unsigned-integer>>.

%% assert_elem_present(Elem, ElemList) ->
%%     ?assertMatch({_, _}, lists:keyfind(Elem, 1, ElemList)).

%% assert_elem_absent(Elem, ElemList) ->
%%     ?assertEqual(false,  lists:keyfind(Elem, 1, ElemList)).

make_range(Start, Finish) ->
    [ make_element(I)  || I <- lists:seq(Start, Finish)].

assertRange({Start, Finish}, Res) ->
    {Elems, _Ctx} = lists:unzip(Res),
    Expected = make_range(Start, Finish),
    ?assertEqual(Expected, Elems).
