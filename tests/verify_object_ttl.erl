%% @author mikael
%% @doc @todo Add description to verify_object_ttl.


-module(verify_object_ttl).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-define(BUCKET, <<"obj-ttl">>).

-cover_modules([riak_kv_sweeper]).

-import(verify_sweep_reaper, [manually_sweep_all/1,
                              disable_sweep_scheduling/1,
                              set_tombstone_grace/2,
                              check_reaps/3,
                              get_sweep_status/1]).
%% ====================================================================
%% API functions
%% ====================================================================
-export([confirm/0]).

-define(SWEEP_TICK, 1000).
-define(SHORT_TOMBSTONE_GRACE, 1).

confirm() ->
    Config = [{riak_core, 
               [{ring_creation_size, 8}
               ]},
              {riak_kv,
               [{delete_mode, keep},
                {reap_sweep_interval, 1},
                {sweep_tick, ?SWEEP_TICK},       %% Speed up sweeping
                {obj_ttl_sweep_interval, 1}]}
             ],

    Nodes = rt:build_cluster(1, Config),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),
    
    [Client] = create_pb_clients(Nodes),
    disable_sweep_scheduling(Nodes), %% Disable sweeps so they don't mess with our tests.
    verify_object_ttl(Client),
    verify_bucket_ttl(Client),
    set_tombstone_grace(Nodes, ?SHORT_TOMBSTONE_GRACE),
    verify_ttl_sweep(Client, hd(Nodes)),
    verify_bucket_ttl_change(Client, hd(Nodes)),
    pass.

%% Tests that objects not visible after ttl expire
verify_object_ttl(Client) ->
    KV = test_data(1, 1),
    put_object_with_ttl(Client, KV, 5), %% 5s
    wait_for_expiry(Client, KV).
%% Tests that objects not visible after ttl expire on bucket
verify_bucket_ttl(Client) ->
    TTLBucket = <<"ttl_bucket">>,
    riakc_pb_socket:set_bucket(Client, TTLBucket, [{ttl, 1}]), %% 1s
    Key2 = test_data(2),
    Key3 = test_data(3),
    put_object_without_ttl(Client, TTLBucket, Key2),
    put_object_without_ttl(Client, ?BUCKET, Key3),
    timer:sleep(timer:seconds(2)),
    true = check_expired(Client, TTLBucket, Key2),
    false = check_expired(Client, ?BUCKET, Key3),

    LongTTLBucket = <<"long_ttl_bucket">>,
    riakc_pb_socket:set_bucket(Client, LongTTLBucket, [{ttl, 50000}]),
    Key4 = test_data(4),
    put_object_with_ttl(Client, Key4, 1),
    timer:sleep(timer:seconds(2)),
    true = check_expired(Client, LongTTLBucket, Key4),

    ShortTTLBucket = <<"short_ttl_bucket">>,
    riakc_pb_socket:set_bucket(Client, ShortTTLBucket, [{ttl, 1}]),
    Key5 = test_data(5),
    put_object_with_ttl(Client, ShortTTLBucket, Key5, 20),
    timer:sleep(timer:seconds(2)),
    false = check_expired(Client, ShortTTLBucket, Key5).
%% Obj -> TTL sweep -> Tombstone -> Reap sweep -> gone
verify_ttl_sweep(Client, Node) ->
   KVs1 = test_data(101, 200),
   KVs1000 = test_data(201, 300),
   put_object_with_ttl(Client, KVs1, 1), %% 1s
   put_object_with_ttl(Client, KVs1000, 1000), %% 1000s
   manually_sweep_all(Node),

   true = check_expired(Client, KVs1),
   false = check_expired(Client, KVs1000),

   timer:sleep(timer:seconds(5)),
   manually_sweep_all(Node),
   true = check_reaps(Node, Client, KVs1),
   false = check_expired(Client, KVs1000),

   delete_keys(Client, KVs1000, []),
   manually_sweep_all(Node),
   true = check_reaps(Node, Client, KVs1000),
   ok.
%% Verify that old objects get expired when we configure short ttl on bucket
verify_bucket_ttl_change(Client, Node) ->
    KVs = test_data(301, 400),
    Bucket = <<"change_ttl_bucket">>,
    riakc_pb_socket:set_bucket(Client, Bucket, [{ttl, 50000}]),
    put_object_without_ttl(Client, Bucket, KVs),
    false = check_expired(Client, Bucket, KVs),
    riakc_pb_socket:set_bucket(Client, Bucket, [{ttl, 1}]),
    timer:sleep(timer:seconds(2)),
    true = check_expired(Client, Bucket, KVs),


    KVs1 = test_data(401, 500),
    put_object_without_ttl(Client, Bucket, KVs1),
    timer:sleep(timer:seconds(2)),
    true = check_expired(Client, Bucket, KVs1),
    riakc_pb_socket:set_bucket(Client, Bucket, [{ttl, 50000}]),
    manually_sweep_all(Node),
    %% resurrect all the data
    false = check_expired(Client, Bucket, KVs1).

%% ====================================================================
%% Internal functions
%% ====================================================================

%%% Client/Key ops
create_pb_clients(Nodes) ->
    [begin
         C = rt:pbc(Node),
         riakc_pb_socket:set_options(C, [queue_if_disconnected]),
         C
     end || Node <- Nodes].

put_object_with_ttl(Client, KV, TTL) ->
    put_object_with_ttl(Client, ?BUCKET, KV, TTL).

put_object_with_ttl(Client, Bucket, KVs, TTL) when is_list(KVs) ->
    lager:info("Putting ~p object", [length(KVs)]),
    [put_object_with_ttl(Client, Bucket, KV, TTL)  || KV <- KVs];

put_object_with_ttl(Client, Bucket, {Key, Value}, TTL) ->
    Robj0 = riakc_obj:new(Bucket, Key, Value),
    MD = riakc_obj:get_metadata(Robj0),  
    Robj1 = riakc_obj:update_metadata(Robj0, riakc_obj:set_ttl(MD, TTL)),
    riakc_pb_socket:put(Client, Robj1).

put_object_without_ttl(Client, KV) ->
    put_object_without_ttl(Client, ?BUCKET, KV).

put_object_without_ttl(Client, Bucket, KVs) when is_list(KVs) ->
    lager:info("Putting ~p object", [length(KVs)]),
    [put_object_without_ttl(Client, Bucket, KV)  || KV <- KVs];

put_object_without_ttl(Client, Bucket, {Key, Value}) ->
    Robj0 = riakc_obj:new(Bucket, Key, Value),
    riakc_pb_socket:put(Client, Robj0).

wait_for_expiry(Client, KV) ->
    wait_for_expiry(Client, ?BUCKET, KV).

wait_for_expiry(Client, Bucket, KVs) when is_list(KVs) ->
    [wait_for_expiry(Client, Bucket, KV)  || KV <- KVs];

wait_for_expiry(Client, Bucket, KV) ->
    rt:wait_until(
      fun() ->
              check_expired(Client, Bucket, KV)
      end).

check_expired(Client, KVs) ->
    check_expired(Client, ?BUCKET, KVs).

check_expired(Client, Bucket, KVs) when is_list(KVs) ->
    Expired = [true || true <- [check_expired(Client, Bucket, KV)  || KV <- KVs]],
    case length(KVs) of
       N when N == length(Expired) ->
           lager:info("TTL expired ~p keys ~n", [N]),
           true;
        N ->
            lager:info("Not all keys expired ~p keys ~n", [N]),
            false
    end;

check_expired(Client, Bucket, {Key, _Value}) ->
    case riakc_pb_socket:get(Client, Bucket, Key) of
        {error, notfound} ->
            true;
        {ok, _RObj} ->
            false
    end.

test_data(Start) ->
    test_data(Start, Start).

test_data(Start, End) ->
    Keys = [int_to_key(N) || N <- lists:seq(Start, End)],
    [{K, K} || K <- Keys].

int_to_key(Ns) when is_list(Ns) ->
    [int_to_key(N) || N <- Ns];

int_to_key(N) ->
    case N < 100 of
        true ->
            list_to_binary(io_lib:format("obj~2..0B", [N]));
        _ ->
            list_to_binary(io_lib:format("obj~p", [N]))
    end.

delete_keys(Client, KVs, Opt) ->
    lager:info("Delete data ~p keys", [length(KVs)]),
    [{delete_key(Client, K, Opt)}  || {K, _V} <- KVs].

delete_key(Client, Key, Opt) ->
    {ok, Obj} = riakc_pb_socket:get(Client, ?BUCKET, Key),
    riakc_pb_socket:delete_obj(Client, Obj, Opt).
