-module(yz_long_test).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(CFG,
        [
         {riak_core,
          [
           {ring_creation_size, 8}
          ]},
         {yokozuna,
          [
           {enabled, true}
          ]}
        ]).

confirm() ->
    NumNodes = 3,
    lager:info("Building cluster and waiting for yokozuna to start"),
    Nodes = rt:build_cluster(NumNodes, ?CFG),
    rt:wait_for_cluster_service(Nodes, yokozuna),
    Node = hd(Nodes),

    lager:info("Creating/activating 'test' bucket type"),

    Bucket = <<"test">>,
    Index = <<"testi">>,
    create_index(Node, Index),
    wait_for_index(Nodes, Index),
    set_bucket_props(Node, Bucket, Index),

    Keys = populate_nodes(Node, Bucket, Index),
    %% Pause for 30 minutes, then validate the dataset over 5 hours
    PauseTime = 30 * 60 * 1000,
    ?assertEqual(ok, validate_keys_exist(Nodes, Index, Keys, PauseTime, 10)),
    pass.


%% @private
%% @doc Populates 20000 indexed keys/values
populate_nodes(Node, Bucket, Index) ->
    %% Yz only supports UTF-8 compatible keys
    Keys = lists:sublist([<<N:64/integer>> || N <- lists:seq(1,20000*4),
        not lists:any(fun(E) -> E > 127 end,
            binary_to_list(<<N:64/integer>>))], 20000),

    PBC = rt:pbc(Node),

    lager:info("Writing ~p keys", [length(Keys)]),
    [ok = rt:pbc_write(PBC, Bucket, Key, Key, "text/plain") || Key <- Keys],

    %% soft commit wait, then check that last key is indexed
    lager:info("Search for keys to verify they exist"),
    timer:sleep(1000),
    LKey = lists:last(Keys),
    rt:wait_until(fun() ->
        {M, _} = riakc_pb_socket:search(PBC, Index, query_value(LKey)),
        ok == M
    end),
    Keys.

validate_keys_exist(_Node, _Index, _Keys, _Pause, 0) ->
    ok;
validate_keys_exist(Nodes, Index, Keys, Pause, LoopCount) ->
    Node = select_random(Nodes),
    lager:info("validate_keys_exist on Node ~p~n", [Node]),
    PBC = rt:pbc(Node),
    TotalFound = lists:sum([ validate_found_count(PBC, Index, Key) || Key <- Keys ]),
    lager:info(" found matching ~p~n", [TotalFound]),
    lager:info(" pausing for ~p mins~n", [ Pause / 1000 / 60 ]),
    timer:sleep(Pause),
    validate_keys_exist(Nodes, Index, Keys, Pause, LoopCount - 1).

validate_found_count(PBC, Index, Key) ->
    case riakc_pb_socket:search(PBC, Index, query_value(Key)) of
        {ok, _} -> 1;
        _ ->
            lager:info(" missing ~p~n", [query_value(Key)]),
            0
    end.

%% @private
%% @doc Builds a simple riak key query
query_value(Value) ->
    V2 = iolist_to_binary(re:replace(Value, "\"", "%22", [global])),
    V3 = iolist_to_binary(re:replace(V2, "\\\\", "%5C", [global])),
    <<"_yz_rk:\"",V3/binary,"\"">>.

%% pulled from yz_rt

%% @private
select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

%% @private
create_index(Node, Index) ->
    lager:info("Creating index ~s [~p]", [Index, Node]),
    ok = rpc:call(Node, yz_index, create, [Index]).

%% @private
wait_for_index(Cluster, Index) ->
    IsIndexUp =
        fun(Node) ->
                lager:info("Waiting for index ~s to be avaiable on node ~p", [Index, Node]),
                rpc:call(Node, yz_solr, ping, [Index])
        end,
    [?assertEqual(ok, rt:wait_until(Node, IsIndexUp)) || Node <- Cluster],
    ok.

%% @private
set_bucket_props(Node, Bucket, Index) ->
    Props = [{search_index, Index}],
    rpc:call(Node, riak_core_bucket, set_bucket, [Bucket, Props]).
