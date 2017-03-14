-module(redirect_siblings_kv1188).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(NUM_NODES, 5).
-define(N_VAL, 3).
-define(COORDINATOR_TIMEOUT, 100). %% in milliseconds

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
    Nodes = rt:build_cluster(?NUM_NODES, [{riak_core, [{handoff_concurrency, 11}]}]),
    rt:wait_until_transfers_complete(Nodes),
    %% Install intercepts
    lists:foreach(fun(Node) ->
        rpc:call(Node, random_intercepts, unstick_random, []),
        ok = rpc:call(Node, application, set_env, [riak_kv, put_coordinator_failure_timeout, ?COORDINATOR_TIMEOUT]),
        TimeoutVal = rpc:call(Node, app_helper, get_env, [riak_kv, put_coordinator_failure_timeout]),
        lager:info("Timeout for node ~p is ~p", [Node, TimeoutVal]),
        rt_intercept:add(Node, {random, [{{uniform_s, 2}, last_for_uniform_s}]})
        end, Nodes),
    Bucket = <<"SibBucket">>,
    Key = <<"SibKey">>,
    %% While the n_val is set to 3 for the bucket below, we are going to pretend
    %% that it's ?NUM_NODES so we can get a complete list of nodes in the order
    %% in which Riak thinks they will be applied to a preflist
    {ok, PL} = rt:get_primary_preflist(hd(Nodes), Bucket, Key, ?NUM_NODES),
    lager:info("Complete PL: ~p", [PL]),
    {PrimaryNodes, FallbackNodes} = split_preflist(PL, ?N_VAL),
    lager:info("Primary, Fallback: ~p , ~p", [PrimaryNodes, FallbackNodes]),
    %% Because we always choose the last node in the preflist due to the intercept above,
    %% make the last node in the primary preflist slow to induce the issue.
    DeadNode = lists:nth(?N_VAL, PrimaryNodes),
    lager:info("DeadNode: ~p", [DeadNode]),
    %% Install slow failed startlink in dead node
    rt_intercept:add(DeadNode, {riak_kv_put_fsm, [{{start_link, 3}, really_slow_failed_start_link}]}),
    %% Pick the last node _not_ in the primary preflist to make sure we redirect the request
    NonPlNode = lists:last(FallbackNodes),
    lager:info("NonPLNode: ~p", [NonPlNode]),
    Client = rt:pbc(NonPlNode),
    ok = rt:pbc_set_bucket_prop(Client, Bucket,
                                [{allow_mult, true},
                                 {n_val, ?N_VAL},
                                 {dvv_enabled, true}]),
    lager:info("Putting object", []),
    ok = riakc_pb_socket:put(Client, riakc_obj:new(Bucket, Key, <<"value">>)),
    lager:info("Waiting for put FSMs to finish", []),
    timer:sleep(?COORDINATOR_TIMEOUT*(?NUM_NODES*3)),
    lager:info("Get object and check for siblings", []),
    {ok, Obj} = riakc_pb_socket:get(Client, Bucket, Key),
    lager:info("Obj : ~p", [Obj]),
    ?assertEqual(1, length(riakc_obj:get_contents(Obj))),
    pass.

split_preflist(PrefList, NVal) ->
    Nodes = [ Node || {{_Idx, Node}, primary} <- PrefList],
    lists:split(NVal, Nodes).
