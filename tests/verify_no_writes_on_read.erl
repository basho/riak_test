-module(verify_no_writes_on_read).
-behaviour(riak_test).
-export([confirm/0]).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(NUM_NODES, 3).
-define(BUCKET, <<"bucket">>).

confirm() ->
    Backend = proplists:get_value(backend, riak_test_runner:metadata()),
    lager:info("Running with backend ~p", [Backend]),
    ?assertEqual(bitcask, Backend),
    [Node1 | _Rest] = _Nodes = rt:build_cluster(?NUM_NODES),
    PBC = rt:pbc(Node1),
    lager:info("Setting last write wins on bucket"),
    B = ?BUCKET,
    ?assertMatch(ok, rpc:call(Node1, riak_core_bucket, set_bucket, [B, [{last_write_wins, true}]])),
    BProps = rpc:call(Node1, riak_core_bucket, get_bucket, [B]),
    lager:info("Bucket properties ~p", [BProps]),
    K = <<"Key">>,
    V = <<"Value">>,
    Obj = riakc_obj:new(B, K, V),
    lager:info("Writing a simple object"),
    riakc_pb_socket:put(PBC,Obj),
    lager:info("Waiting some time to let the stats update"),
    timer:sleep(10000),
    OrigStats = get_write_stats(Node1),
    lager:info("Stats are now ~p", [OrigStats]),
    Read1 = fun(_N) ->
                    ?assertMatch({ok,_O}, riakc_pb_socket:get(PBC, B, K))
            end,
    lager:info("Repeatedly read that object. There should be no writes"),
    lists:foreach(Read1, lists:seq(1,100)),
    lager:info("Waiting some time to let the stats update"),
    timer:sleep(10000),
    Stats = get_write_stats(Node1),
    lager:info("Stats are now ~p", [Stats]),
    ?assertEqual(OrigStats, Stats),
    riakc_pb_socket:stop(PBC),
    pass.


get_write_stats(Node) ->
    Stats = rpc:call(Node, riak_kv_stat, get_stats, []),
    Puts = proplists:get_value(vnode_puts, Stats),
    ReadRepairs = proplists:get_value(read_repairs, Stats),
    [{puts, Puts}, {read_repairs, ReadRepairs}].

