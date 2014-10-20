-module(yz_handoff).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(INDEX, <<"test_idx">>).
-define(BUCKET, <<"test_bkt">>).
-define(CFG,
        [
         {riak_core,
          [
           {ring_creation_size, 64}
          ]},
         {yokozuna,
          [
           {enabled, true}
          ]}
        ]).

confirm() ->
    %% Setup cluster
    Nodes = rt:build_cluster(5, ?CFG),
    rt:wait_for_cluster_service(Nodes, yokozuna),
    [Node1|Nodes2] = Nodes,
    [Node2|_] = Nodes2,

    %% Generate keys, YZ only supports UTF-8 compatible keys
    Keys = [<<N:64/integer>> || N <- lists:seq(1,20000),
        not lists:any(fun(E) -> E > 127 end,binary_to_list(<<N:64/integer>>))],
    KeyCount = length(Keys),

    Pid = rt:pbc(Node2),
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    %% Create a search index and associate with a bucket
    ok = riakc_pb_socket:create_search_index(Pid, ?INDEX),
    wait_for_index(Nodes, ?INDEX),
    ok = riakc_pb_socket:set_search_index(Pid, ?BUCKET, ?INDEX),
    timer:sleep(1000),

    %% Write keys and wait for soft commit
    lager:info("Writing ~p keys", [KeyCount]),
    [ok = rt:pbc_write(Pid, ?BUCKET, Key, Key, "text/plain") || Key <- Keys],
    timer:sleep(1100),

    % TODO: count total replicas from solr_internal directly
    %% Query result should match total object count
    {ok, Resp} = riakc_pb_socket:search(Pid, ?INDEX, <<"*:*">>),
    ?assertEqual(KeyCount, Resp#search_results.num_found),

    lager:info("Leaving ~p from cluster", [Node1]),
    rt:leave(Node1),
    rt:wait_until_transfers_complete(Nodes2),

    lager:info("Waiting 10s for transfers"),
    timer:sleep(10000),

    %% Query result should still match total object count
    {ok, Resp2} = riakc_pb_socket:search(Pid, ?INDEX, <<"*:*">>),
    ?assertEqual(KeyCount, Resp2#search_results.num_found),

    pass.


%% @private
%% @doc Builds a simple riak key query
query_value(Value) ->
    V2 = iolist_to_binary(re:replace(Value, "\"", "%22")),
    V3 = iolist_to_binary(re:replace(V2, "\\\\", "%5C")),
    <<"_yz_rk:\"",V3/binary,"\"">>.

%% @private
wait_for_index(Cluster, Index) ->
    IsIndexUp =
        fun(Node) ->
                lager:info("Waiting for index ~s to be avaiable on node ~p", [Index, Node]),
                rpc:call(Node, yz_solr, ping, [Index])
        end,
    [?assertEqual(ok, rt:wait_until(Node, IsIndexUp)) || Node <- Cluster],
    ok.
