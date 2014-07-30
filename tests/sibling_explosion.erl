-module(sibling_explosion).
-include_lib("eunit/include/eunit.hrl").
-export([confirm/0]).
-compile(export_all).

-define(B, <<"b">>).
-define(K, <<"k">>).

%% This tests provokes a sibling explosion. It does so with a single
%% node and a single client. All it does to acheive the explosion is
%% interleave fetch / resolve / writes. It works like this:
%%  - The aim of the clients is to write a single set of intergers from 0-99
%%  - one client gets odds, the other evens
%% - The test interleaves writes from the clients
%% - Each client, in turn:
%% -- fetchs the key from riak
%% -- resolves the sibling values (by performing a set union on the values in all siblings)
%% -- adds a new value to the set
%% -- Puts the new value back to riak
%% This results in 99 siblings, each a subset of the following sibling [0] | [0, 1] | [0, 1, 2],  [0, 1, 2, 3] etc
confirm() ->
    Conf = [{riak_core, [{default_bucket_props, [{allow_mult, true},
                                                 {dvv_enabled, true}]}]}],
    [Node1] = rt_cluster:deploy_nodes(1, Conf),
    N = 100,

    lager:info("Put new object in ~p via PBC.", [Node1]),
    PB = rt_pb:pbc(Node1),

    A0 = riakc_obj:new(<<"b">>, <<"k">>, sets:from_list([0])),
    B0 = riakc_obj:new(<<"b">>, <<"k">>, sets:from_list([1])),

    _ = explode(PB, {A0, B0}, N),

    {ok, SibCheck1} = riakc_pb_socket:get(PB, <<"b">>, <<"k">>),
    %% there should now be only two siblings
    ?assertEqual(2, riakc_obj:value_count(SibCheck1)),
    %% siblings should merge to include all writes
    assert_sibling_values(riakc_obj:get_values(SibCheck1), N),
    pass.

%% Check that everywrite was wrote.
assert_sibling_values(Values, N) ->
    V = resolve(Values, sets:new()),
    L = lists:sort(sets:to_list(V)),
    Expected = lists:seq(0, N-2),
    ?assertEqual(Expected, L).

%% Pick one of the two objects, and perform a fetch, resolve, update
%% cycle with it. The point is that the two object's writes are
%% interleaved. First A is updated, then B. Each object already has a
%% "latest" vclock returned from it's last put. This simulates the
%% case where a client fetches a vclock before PUT, but another write
%% lands at the vnode after the vclock is returned and before the next
%% PUT. Each PUT sees all but one write that Riak as seen, meaning
%% there is a perpetual race / sibling. Without DVV that means ALL
%% writes are added to the sibling set. With DVV, we correctly capture
%% the resolution of seen writes.
explode(_PB, {A, B}, 1) ->
    {A, B};
explode(PB, {A0, B}, Cnt) when Cnt rem 2 == 0 ->
    A = resolve_mutate_store(PB, Cnt, A0),
    explode(PB, {A, B}, Cnt-1);
explode(PB, {A, B0}, Cnt) when Cnt rem 2 /= 0  ->
    B = resolve_mutate_store(PB, Cnt, B0),
    explode(PB, {A, B}, Cnt-1).

%% resolve the fetch, and put a new value
resolve_mutate_store(PB, N, Obj0) ->
    Obj = resolve_update(Obj0, N),
    {ok, Obj2} = riakc_pb_socket:put(PB, Obj, [return_body]),
    Obj2.

%% simply union the values, and add a new one for this operation
resolve_update(Obj, N) ->
    case riakc_obj:get_values(Obj) of
        [] -> Obj;
        Values ->
            Value0 = resolve(Values, sets:new()),
            Value = sets:add_element(N, Value0),
            lager:info("Storing ~p", [N]),
            riakc_obj:update_metadata(riakc_obj:update_value(Obj, Value), dict:new())
    end.

%% Set union on each value
resolve([], Acc) ->
    Acc;
resolve([V0 | Rest], Acc) ->
    V = binary_to_term(V0),
    resolve(Rest, sets:union(V, Acc)).
