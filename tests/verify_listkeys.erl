-module(verify_listkeys).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"listkeys_bucket">>).
-define(NUM_BUCKETS, 1200).
-define(NUM_KEYS, 1000).

confirm() ->
    [Node1, Node2, Node3, Node4] = Nodes = rt:deploy_nodes(4),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    
    lager:info("Nodes deployed, but not joined."),
    
    lager:info("Writing some known data to Node 1"),
    put_keys(Node1, ?BUCKET, ?NUM_KEYS),
    put_buckets(Node1, ?NUM_BUCKETS),
    timer:sleep(2000),
    check_it_all([Node1]),

    lists:foldl(fun(Node, [N1|_] = Cluster) ->
            lager:info("An invitation to this party is cordially extended to ~p.", [Node]),
            rt:join(Node, N1),
            lager:info("Check keys and buckets during transfer"),
            Ns = lists:usort([Node|Cluster]),
            check_it_all(Ns),
            lager:info("Wait until there are no pending changes"),
            ?assertEqual(ok, rt:wait_until_no_pending_changes(Ns)),
            
            lager:info("Check keys and buckets after transfer"),
            check_it_all(Ns),
            Ns
        end, [Node1], [Node2, Node3, Node4]),

    lager:info("Stopping Node1"),
    rt:stop(Node1),
    rt:wait_until_unpingable(Node1),
    
    %% Stop current node, restart previous node, verify
    lists:foldl(fun(Node, Prev) ->
            lager:info("Stopping Node ~p", [Node]),
            rt:stop(Node),
            rt:wait_until_unpingable(Node),
            
            lager:info("Starting Node ~p", [Prev]),
            rt:start(Prev),
            rt:wait_until_pingable(Prev),
            
            lager:info("Check keys and buckets"),
            check_it_all(Nodes -- [Node]),
            Node
        end, Node1, [Node2, Node3, Node4]),
    
    lager:info("Stopping Node2"),
    rt:stop(Node2),
    rt:wait_until_unpingable(Node2),
    
    lager:info("Stopping Node3"),
    rt:stop(Node3),
    rt:wait_until_unpingable(Node3),
    
    lager:info("Only Node1 is up, so test should fail!"),
    
    check_it_all([Node1], false),
    pass.
    
put_keys(Node, Bucket, Num) ->
    Pid = rt:pbc(Node),
    Keys = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
    Vals = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
    [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val)) || {Key, Val} <- lists:zip(Keys, Vals)],
    riakc_pb_socket:stop(Pid).

list_keys(Node, Bucket, Attempt, Legacy, Num, ShouldPass) ->
    Pid = rt:pbc(Node),
    lager:info("Listing keys on ~p. Legacy: ~p, Attempt #~p", [Node, Legacy, Attempt]),
    rpc:call(Node, application, set_env, [riak_kv, legacy_keylisting, Legacy]),
    
    case ShouldPass of
        true ->
            {ok, Keys} = riakc_pb_socket:list_keys(Pid, Bucket),
            ActualKeys = lists:usort(Keys),
            ExpectedKeys = lists:usort([list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)]),
            case ExpectedKeys -- ActualKeys of
                [] -> ok;
                Diff -> lager:info("ExpectedKeys -- ActualKeys: ~p", [Diff])
            end,
            ?assertEqual(length(ActualKeys), length(ExpectedKeys)),
            ?assertEqual(ActualKeys, ExpectedKeys);
        _ ->
            {Status, Message} = riakc_pb_socket:list_keys(Pid, Bucket),
            ?assertEqual(error, Status),
            ?assertEqual(<<"insufficient_vnodes_available">>, Message)
    end,
    riakc_pb_socket:stop(Pid).
    
put_buckets(Node, Num) ->
    Pid = rt:pbc(Node),
    Buckets = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)],
    {Key, Val} = {<<"test_key">>, <<"test_value">>},
    [riakc_pb_socket:put(Pid, riakc_obj:new(Bucket, Key, Val)) || Bucket <- Buckets],
    riakc_pb_socket:stop(Pid).

list_buckets(Node, Attempt, Legacy, Num, ShouldPass) ->
    Pid = rt:pbc(Node),
    lager:info("Listing buckets on ~p. Legacy: ~p, Attempt #~p", [Node, Legacy, Attempt]),
    rpc:call(Node, application, set_env, [riak_kv, legacy_keylisting, Legacy]),
    
    {Status, Buckets} = riakc_pb_socket:list_buckets(Pid),
    ?assertEqual(ok, Status),
    ExpectedBuckets= lists:usort([?BUCKET | [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, Num - 1)]]),
    ActualBuckets = lists:usort(Buckets),
    case ShouldPass of
        true ->
            case ExpectedBuckets -- ActualBuckets of 
                [] -> ok;
                Diff -> lager:info("ExpectedBuckets -- ActualBuckets: ~p", [Diff])
            end,
            ?assertEqual(length(ActualBuckets), length(ExpectedBuckets)),
            ?assertEqual(ActualBuckets, ExpectedBuckets);
        _ ->
            ?assert(length(ActualBuckets) < length(ExpectedBuckets)),
            lager:info("This case expects inconsistent bucket lists")
    end,
    riakc_pb_socket:stop(Pid).

check_it_all(Nodes) ->
    check_it_all(Nodes, true).
check_it_all(Nodes, ShouldPass) ->
    [check_a_node(N, ShouldPass) || N <- Nodes].
    
check_a_node(Node, ShouldPass) ->
    list_keys(Node, ?BUCKET, 1, false, ?NUM_KEYS, ShouldPass),
    list_keys(Node, ?BUCKET, 1, true,  ?NUM_KEYS, ShouldPass),
    list_keys(Node, ?BUCKET, 2, false, ?NUM_KEYS, ShouldPass),
    list_keys(Node, ?BUCKET, 2, true,  ?NUM_KEYS, ShouldPass),
    list_keys(Node, ?BUCKET, 3, false, ?NUM_KEYS, ShouldPass),
    list_keys(Node, ?BUCKET, 3, true,  ?NUM_KEYS, ShouldPass),
    
    list_buckets(Node, 1, false, ?NUM_BUCKETS, ShouldPass),
    list_buckets(Node, 1, true,  ?NUM_BUCKETS, ShouldPass),
    list_buckets(Node, 2, false, ?NUM_BUCKETS, ShouldPass),
    list_buckets(Node, 2, true,  ?NUM_BUCKETS, ShouldPass),
    list_buckets(Node, 3, false, ?NUM_BUCKETS, ShouldPass),
    list_buckets(Node, 3, true,  ?NUM_BUCKETS, ShouldPass).
    