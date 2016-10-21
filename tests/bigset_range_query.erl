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
                                                  {range_end, make_element(50)}],
                                           N2Client),

    assertRange({30, 50}, Res),

    {ok, _, {_, Res2}} = bigset_client:read(?SET, [{range_start, make_element(30)},
                                                   {range_end, make_element(50)},
                                                   {end_inclusive, false},
                                                   {start_inclusive, false}],
                                            N2Client),

    assertRange({31, 49}, Res2),

    %% set start - range end
    {ok, _, {_, Res3}} = bigset_client:read(?SET, [{range_end, make_element(50)},
                                                   {end_inclusive, false}],
                                            N2Client),

    assertRange({1, 49}, Res3),
    %% range start - set end
    {ok, _, {_, Res4}} = bigset_client:read(?SET, [{range_start, make_element(50)},
                                                   {start_inclusive, false}],
                                            N2Client),

    assertRange({51, 100}, Res4),
    %% range start - range end (where range end > set end)
    {ok, _, {_, Res4}} = bigset_client:read(?SET, [{range_start, make_element(50)},
                                                   {start_inclusive, false},
                                                   {range_end, make_element(10000)}],
                                            N2Client),

    assertRange({51, 100}, Res4),
    %% range start - range end (where start < set start)
    {ok, _, {_, Res5}} = bigset_client:read(?SET, [{range_start, make_element(0)},
                                                   {range_end, make_element(10)}],
                                            N2Client),

    assertRange({1, 10}, Res5),
    %% range start > range end
    try
        NotErr = bigset_client:read(?SET, [{range_start, make_element(50)},
                                           {range_end, make_element(1)}],
                                    N2Client),
        ?assertEqual("Expected an error", NotErr)
    catch throw:E ->
            ?assertMatch({error,invalid_options,[{invalid_range_options, _, _}], _}, E),
            lager:info("Expected error: ~p", [E])
    end,
    pass.

make_element(I) when is_integer(I) ->
    <<I:32/big-unsigned-integer>>.

make_range(Start, Finish) ->
    [ make_element(I)  || I <- lists:seq(Start, Finish)].

assertRange({Start, Finish}, Res) ->
    {Elems, _Ctx} = lists:unzip(Res),
    Expected = make_range(Start, Finish),
    ?assertEqual(Expected, Elems).
