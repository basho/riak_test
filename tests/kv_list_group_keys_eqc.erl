-module(kv_list_group_keys_eqc).

-ifdef(EQC).

-compile(export_all).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(riak_test).
-export([confirm/0]).

-define(CONFIG,
        [{riak_core, [{handoff_concurrency, 10},
                      {vnode_management_timer, 1000}]},
         {riak_kv, [{storage_backend, riak_kv_eleveldb_backend}]}]).
-define(CLUSTER_SIZE, 5).
-define(PHOTOS_PREFIX, <<"photos/2017/">>).
-define(MONTHS, [<<"January">>, <<"February">>, <<"March">>, <<"April">>,
                 <<"May">>, <<"June">>, <<"July">>, <<"August">>,
                 <<"September">>, <<"October">>, <<"November">>, <<"December">>]).

-record(group_keys_result, {
          keys = [] :: list(),
          common_prefixes = [] :: list()
         }).

confirm() ->
    Cluster = setup_cluster(?CLUSTER_SIZE),
    put_objects(Cluster),
    quickcheck(?MODULE:prop_group_keys_basic(Cluster)),
    quickcheck(?MODULE:prop_group_keys_prefix(Cluster)),
    quickcheck(?MODULE:prop_group_keys_delimiter(Cluster)),
    quickcheck(?MODULE:prop_group_keys_prefix_delimiter(Cluster)),
    rt:clean_cluster(Cluster),
    pass.

%% ====================================================================
%% EQC generators
%% ====================================================================
gen_max_keys() ->
    NumKeys = length(all_keys()),
    choose(erlang:max(1, NumKeys div 10), NumKeys).

%% ====================================================================
%% EQC Properties
%% ====================================================================

quickcheck(Property) ->
    ?assert(eqc:quickcheck(eqc:numtests(500, Property))).

forall_buckets_and_max_keys(Property) ->
    ?FORALL(Bucket, oneof(buckets()),
            collect(Bucket,
                    ?FORALL(MaxKeys, gen_max_keys(),
                            collect(MaxKeys, Property(Bucket, MaxKeys))))).

prop_group_keys_basic(Cluster) ->
    Expected = #group_keys_result{keys = lists:sort(all_keys())},
    forall_buckets_and_max_keys(
      fun(Bucket, MaxKeys) ->
              GroupParams = make_group_params([{max_keys, MaxKeys}]),
              Expected == collect_group_keys(Cluster, Bucket, GroupParams)
      end).

prop_group_keys_prefix(Cluster) ->
    Prefix = ?PHOTOS_PREFIX,
    GroupParams = make_group_params([{prefix, Prefix}]),
    Expected = #group_keys_result{
                  keys = lists:sort(lists:filter(fun(Key) ->
                                                         is_prefix(Prefix, Key)
                                                 end,
                                                 all_keys()))
                 },
    forall_buckets_and_max_keys(
      fun(Bucket, MaxKeys) ->
              GroupParams1 = riak_kv_group_keys:set_max_keys(GroupParams, MaxKeys),
              Expected == collect_group_keys(Cluster, Bucket, GroupParams1)
      end).

prop_group_keys_delimiter(Cluster) ->
    Delimiter = <<"/">>,
    GroupParams = make_group_params([{delimiter, Delimiter}]),
    Expected = #group_keys_result{
                  keys = [<<"sample.jpg">>],
                  common_prefixes = [<<"photos/">>]
                 },
    forall_buckets_and_max_keys(
      fun(Bucket, MaxKeys) ->
              GroupParams1 = riak_kv_group_keys:set_max_keys(GroupParams, MaxKeys),
              Expected == collect_group_keys(Cluster, Bucket, GroupParams1)
      end).

prop_group_keys_prefix_delimiter(Cluster) ->
    Prefix = ?PHOTOS_PREFIX,
    Delimiter = <<"/">>,
    GroupParams = make_group_params([{prefix, Prefix}, {delimiter, Delimiter}]),
    Expected = #group_keys_result{
                  keys = lists:sort([<<?PHOTOS_PREFIX/binary, "Index.html">>,
                                     <<?PHOTOS_PREFIX/binary, "index.html">>]),
                  common_prefixes = lists:sort([<<?PHOTOS_PREFIX/binary, Month/binary, "/">>
                                                || Month <- ?MONTHS])
                 },
    forall_buckets_and_max_keys(
      fun(Bucket, MaxKeys) ->
              GroupParams1 = riak_kv_group_keys:set_max_keys(GroupParams, MaxKeys),
              Expected == collect_group_keys(Cluster, Bucket, GroupParams1)
      end).

%% ====================================================================
%% Helpers
%% ====================================================================
setup_cluster(NumNodes) ->
    Nodes = rt:build_cluster(NumNodes, ?CONFIG),
    ?assertEqual(ok, rt:wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, rt:wait_until_transfers_complete(Nodes)),
    Node1 = hd(Nodes),
    [begin
         rt:create_and_activate_bucket_type(Node1, BucketType, [{n_val, NVal}]),
         rt:wait_until_bucket_type_status(BucketType, active, Nodes),
         rt:wait_until_bucket_type_visible(Nodes, BucketType)
     end || {BucketType, NVal} <- bucket_types()],
    Nodes.

bucket_types() ->
    [{<<"n_val_1">>, 1},
     {<<"n_val_2">>, 2},
     {<<"n_val_3">>, 3},
     {<<"n_val_4">>, 4},
     {<<"n_val_5">>, 5}].

buckets() ->
    [{BucketType, BucketType} || {BucketType, _} <- bucket_types()].

all_keys() ->
    [<<"sample.jpg">>,
     <<?PHOTOS_PREFIX/binary, "Index.html">>,
     <<?PHOTOS_PREFIX/binary, "index.html">>] ++
    [<<?PHOTOS_PREFIX/binary, Month/binary, "/sample", N/binary, ".jpg">>
     || Month <- ?MONTHS, N <- lists:map(fun erlang:integer_to_binary/1, lists:seq(1, 99))].

put_objects(Cluster) ->
    lists:foreach(fun({BucketType, _}) ->
                          put_objects(Cluster, {BucketType, BucketType})
                  end,
                  bucket_types()).

put_objects(Cluster, Bucket) ->
    Node = rt:select_random(Cluster),
    {ok, Client} = riak:client_connect(Node),
    lists:foreach(fun(Key) ->
                          put_object(Client, Bucket, Key)
                  end,
                  all_keys()).

put_object(Client, Bucket, Key) ->
    Obj = riak_object:new(Bucket, Key, Key),
    ok = Client:put(Obj).

make_group_params(PropList) ->
    riak_kv_group_keys:to_group_params(PropList).

is_prefix(B1, B2) when is_binary(B1), is_binary(B2) ->
    binary:longest_common_prefix([B1, B2]) == size(B1);
is_prefix(_B1, _B2) ->
    false.

collect_group_keys(Cluster, Bucket, GroupParams) ->
    collect_group_keys(Cluster, Bucket, GroupParams, #group_keys_result{}).

collect_group_keys(Cluster, Bucket, GroupParams, Acc) ->
    Response = list_group_keys(Cluster, Bucket, GroupParams),
    NewAcc = accumulate_group_keys(Response, Acc),
    case riak_kv_group_keys_response:get_next_continuation_token(Response) of
        undefined ->
            NewAcc;
        ContinuationToken ->
            NewGroupParams = set_continuation_token(GroupParams, ContinuationToken),
            collect_group_keys(Cluster, Bucket, NewGroupParams, NewAcc)
    end.

list_group_keys(Cluster, Bucket, GroupParams) ->
    Node = rt:select_random(Cluster),
    {ok, Client} = riak:client_connect(Node),
    {ok, Response} = Client:list_group_keys(Bucket, GroupParams, 5000),
    Response.

accumulate_group_keys(GroupKeysResponse, Acc) ->
    Metadatas = riak_kv_group_keys_response:get_metadatas(GroupKeysResponse),
    CommonPrefixes = riak_kv_group_keys_response:get_common_prefixes(GroupKeysResponse),
    Acc#group_keys_result{
      keys = Acc#group_keys_result.keys ++ [Key || {Key, _Meta} <- Metadatas],
      common_prefixes = Acc#group_keys_result.common_prefixes ++ CommonPrefixes
     }.

set_continuation_token(GroupParams, ContinuationToken) ->
    riak_kv_group_keys:set_continuation_token(GroupParams, ContinuationToken).

-endif. % EQC
