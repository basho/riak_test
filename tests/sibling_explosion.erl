-module(sibling_explosion).
-include_lib("eunit/include/eunit.hrl").
-export([confirm/0]).
-compile(export_all).

-define(B, <<"b">>).
-define(K, <<"k">>).

confirm() ->
    Conf = [{riak_core, [{default_bucket_props, [{allow_mult, true}]}]}],
    [Node1] = rt:deploy_nodes(1, Conf),
    N = 100,

    lager:info("Put new object in ~p via PBC.", [Node1]),
    PB = rt:pbc(Node1),

    A0 = riakc_obj:new(<<"b">>, <<"k">>, sets:from_list([0])),
    B0 = riakc_obj:new(<<"b">>, <<"k">>, sets:from_list([1])),

    _ = explode(PB, {A0, B0}, N),

    {ok, SibCheck1} = riakc_pb_socket:get(PB, <<"b">>, <<"k">>),
    %% there should now be only two siblings
    ?assertEqual(2, riakc_obj:value_count(SibCheck1)),
    %% siblings should merge to include all writes
    assert_sibling_values(riakc_obj:get_values(SibCheck1), N),
    pass.

assert_sibling_values(Values, N) ->
    V = resolve(Values, sets:new()),
    L = lists:sort(sets:to_list(V)),
    Expected = lists:seq(0, N-2),
    ?assertEqual(Expected, L).

explode(_PB, {A, B}, 1) ->
    {A, B};
explode(PB, Objs, Cnt) ->
    Elem = (Cnt rem 2) +1,
    Obj = resolve_mutate_store(PB, Cnt, element(Elem, Objs)),
    explode(PB, setelement(Elem, Objs, Obj), Cnt-1).

resolve_mutate_store(PB, N, Obj0) ->
    Obj = resolve_update(Obj0, N),
    {ok, Obj2} = riakc_pb_socket:put(PB, Obj, [return_body]),
    Obj2.

resolve_update(Obj, N) ->
    case riakc_obj:get_values(Obj) of
        [] -> Obj;
        Values ->
            Value0 = resolve(Values, sets:new()),
            Value = sets:add_element(N, Value0),
            lager:info("Storing ~p", [N]),
            riakc_obj:update_metadata(riakc_obj:update_value(Obj, Value), dict:new())
    end.

resolve([], Acc) ->
    Acc;
resolve([V0 | Rest], Acc) ->
    V = binary_to_term(V0),
    resolve(Rest, sets:union(V, Acc)).
