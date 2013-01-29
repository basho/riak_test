-module(new_loaded_upgrade).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->

    %% Build Cluster
    TestMetaData = riak_test_runner:metadata(),
    %% Only run 2i for level
    _Backend = proplists:get_value(backend, TestMetaData),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    Config = [{riak_search, [{enabled, true}]}],
    NumNodes = 4,
    Vsns = [{OldVsn, Config} || _ <- lists:seq(1,NumNodes)],
    Nodes = [Node1|_] = rt:build_cluster(Vsns),

    lager:info("Writing 100 keys to ~p", [Node1]),
    rt:systest_write(Node1, 100, 3),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),


    Pid = rt:pbc(Node1),
    Keys = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, 100)],
    Vals = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, 100)],
    [riakc_pb_socket:put(Pid, riakc_obj:new(<<"objects">>, Key, Val)) || {Key, Val} <- lists:zip(Keys, Vals)],
    riakc_pb_socket:stop(Pid),
    %% Now we have a cluster!
    %% Let's spawn workers against it.
    timer:sleep(10000),

    rt_worker_sup:start_link([{concurrent, 10}, {nodes, Nodes}]),

    timer:sleep(60000),

    pass.