-module(redirect_siblings_kv1188).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

confirm() ->
    %% It is possible for a slow node to create siblings on put when a the coordinator
    %% times out and the next node selected to coordinate is a fallback - in that case,
    %% the fallback node doesn't know that the originating node has already tried to
    %% forward the request to the node which timed out, and will calculate the preflist
    %% and try to forward the request again (because it doesn't yet know it's responsible
    %% as a fall-back). If it happens to pick the same, slow node as the original put_fsm,
    %% it _too_ will time out, but so will the original put_fsm that redirected to it.
    %% In this case, the original put_fsm will redirect _again_, while the fallback
    %% node will recalculate the preflist and realize it can, in fact, coordinate the write.
    %% Each will then write, causing a sibling.
    %% See https://github.com/basho/riak_kv/issues/1188 for more detail

    %% Build a 5-node cluster to make sure we don't have overlap in coverage
    Nodes = rt:build_cluster(5),
    %% Install intercepts
    lists:foreach(fun(Node) ->
        rpc:call(Node, random_intercepts, unstick_random, []),
        rt_intercept:add(Node, {random, [{{uniform_s, 2}, last_for_uniform_s}]})
        end, Nodes),
    Bucket = <<"SibBucket">>,
    Key = <<"SibKey">>,
    NVal = 3,
    {ok, PL} = rt:get_primary_preflist(hd(Nodes), Bucket, Key, NVal),
    PrefListNodes = get_nodes_from_preflist(PL),
    %% Because we always choose the last node in the preflist due to the intercept above,
    %% make the last node in the primary preflist slow to induce the issue.
    DeadNode = lists:nth(3, PrefListNodes),
    %% Install slow failed startlink in dead node
    rt_intercept:add(DeadNode, {riak_kv_put_fsm, [{{start_link, 3}, really_slow_failed_start_link}]}),
    %% Pick a node _not_ in the preflist to make sure we redirect the request
    NonPlNode = get_non_preflist_node(Nodes, PrefListNodes),
    Client = rt:pbc(NonPlNode),
    rt:pbc_set_bucket_prop(Client, Bucket, [{allow_mult, true}]),
    lager:info("Putting object", []),
    riakc_pb_socket:put(Client, riakc_obj:new(Bucket, Key, <<"value">>)),
    lager:info("Get object and check for siblings", []),
    {ok, Obj} = riakc_pb_socket:get(Client, Bucket, Key),
    lager:info("Obj : ~p", [Obj]),
    ?assertEqual(1, length(riakc_obj:get_contents(Obj))),
    pass.

get_nodes_from_preflist(PrefList) ->
    lists:map(fun({{_Idx, Node}, _PrimaryOrFallback} ) -> Node end, PrefList).

get_non_preflist_node(Nodes, PrefListNodes) ->
    lists:last(Nodes -- PrefListNodes).
