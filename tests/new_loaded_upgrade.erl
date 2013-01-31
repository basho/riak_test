-module(new_loaded_upgrade).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-export([kv_valgen/1, bucket/1, erlang_mr/0, int_to_key/1]).
confirm() ->

    %% Build Cluster
    TestMetaData = riak_test_runner:metadata(),
    %% Only run 2i for level
    _Backend = proplists:get_value(backend, TestMetaData),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),

    Config = [{riak_search, [{enabled, true}]}],
    NumNodes = 4,
    Vsns = [{OldVsn, Config} || _ <- lists:seq(1,NumNodes)],
    Nodes = rt:build_cluster(Vsns),

    seed_cluster(Nodes),
    %% Now we have a cluster!
    %% Let's spawn workers against it.
    timer:sleep(10000),

    %% TODO: teach this supervisor to take 1 node, and not be registered. 
    %% Then, kill a specific supervisor
    rt_worker_sup:start_link([{concurrent, 10}, {nodes, Nodes}]),

    %% TODO: Replace with a recieve block 
    timer:sleep(60000),

    ?assert(false),
    pass.


seed_cluster(_Nodes=[Node1|_]) ->
    lager:info("Seeding Cluster"),

    %% For List Keys
    lager:info("Writing 100 keys to ~p", [Node1]),
    rt:systest_write(Node1, 100, 3),
    ?assertEqual([], rt:systest_read(Node1, 100, 1)),

    Pid = rt:pbc(Node1),
    Keys = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, 100)],
    Vals = [list_to_binary(["", integer_to_list(Ki)]) || Ki <- lists:seq(0, 100)],
    [rt:pbc_write(Pid, <<"objects">>, Key, Val) || {Key, Val} <- lists:zip(Keys, Vals)],

    %% For KV
    kv_seed(Pid),

    %% for 2i
    twoi_seed(Pid),

    %% for mapred
    mr_seed(Pid),

    %% For MC Serch
    rt:enable_search_hook(Node1, bucket(search)),
    seed_search(Pid),


    riakc_pb_socket:stop(Pid).

%% Buckets
bucket(kv) -> <<"utest">>;
bucket(twoi) -> <<"2ibuquot">>;
bucket(mapred) -> <<"bryanitbs">>;
bucket(search) -> <<"scotts_spam">>.

seed_search(Pid) ->
    SpamDir = rt:config(spam_dir),
    Files = case SpamDir of
            undefined -> undefined;
            _ -> filelib:wildcard(SpamDir ++ "/*")
        end,
    seed_search(Pid, Files).

seed_search(_Pid, []) -> ok;
seed_search(Pid, [File|Files]) ->
    Key = list_to_binary(filename:basename(File)),
    rt:pbc_put_file(Pid, bucket(search), Key, File),
    seed_search(Pid, Files).

kv_seed(Pid) ->
    %% Keys: 0 - 7999
    [ rt:pbc_write(Pid, bucket(kv), list_to_binary(["", integer_to_list(Key)]), kv_valgen(Key)) || Key <- lists:seq(0, 7999)].

kv_valgen(Key) ->
    term_to_binary(lists:seq(0, Key)).

int_to_key(KInt) ->
    list_to_binary(["", integer_to_list(KInt)]).

%% Every 2i seeded object will have indexes
%% int_plusone -> [Key + 1, Key + 10000]
%% bin_plustwo -> [<<"Key + 2">>]
twoi_seed(Pid) ->
    %% Keys: 0 - 7999
    [ begin
        Obj = riakc_obj:new(bucket(twoi), list_to_binary(["", integer_to_list(Key)]), kv_valgen(Key)),
        MD1 = riakc_obj:get_update_metadata(Obj),
        MD2 = riakc_obj:set_secondary_index(MD1, [
            {{integer_index, "plusone"}, [Key + 1, Key + 10000]},
            {{binary_index, "plustwo"}, [int_to_key(Key + 2)]}
        ]),
        Obj2 = riakc_obj:update_metadata(Obj, MD2),
        riakc_pb_socket:put(Pid, Obj2)
      end || Key <- lists:seq(0, 7999)].

erlang_mr() ->
    [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, false},
         {reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, true}].

mr_seed(Pid) ->
%% to be used along with sequential_int keygen to populate known
%% mapreduce set
    [ begin
        Value = list_to_binary(["", integer_to_list(Key)]),
        Obj = riakc_obj:new(bucket(mapred), list_to_binary(["", integer_to_list(Key)]), Value),
        riakc_pb_socket:put(Pid, Obj)
      end || Key <- lists:seq(0, 9999)].

