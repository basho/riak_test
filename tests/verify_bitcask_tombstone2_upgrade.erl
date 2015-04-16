% @doc Verify that upgrading Riak with Bitcask to 2.0 or later will trigger
% an upgrade mechanism that will end up merging all existing bitcask files.
% This is necessary so that old style tombstones are reaped, which might
% otherwise stay around for a very long time. This version writes tombstones
% that can be safely dropped during a merge. Bitcask could resurrect old 
% values easily when reaping tombstones during a partial merge if a
% restart happened later.
-module(verify_bitcask_tombstone2_upgrade).
-behaviour(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    TestMetaData = riak_test_runner:metadata(),
    Backend = proplists:get_value(backend, TestMetaData),
    lager:info("Running with backend (this better be Bitcask!) ~p", [Backend]),
    ?assertEqual({backend, bitcask}, {backend, Backend}),
    OldVsn = proplists:get_value(upgrade_version, TestMetaData, previous),
    % Configure for fast merge checks
    Config = [{riak_kv, [{bitcask_merge_check_interval, 2000}]},
              {bitcask, [{max_file_size, 100}]}],
    Nodes = rt:build_cluster([{OldVsn, Config}]),
    verify_bitcask_tombstone2_upgrade(Nodes),
    pass.

% Expects nodes running a version of Riak < 2.0 using Bitcask
verify_bitcask_tombstone2_upgrade(Nodes) ->
    lager:info("Write some data, write it good"),
    write_some_data(Nodes),
    lager:info("Collect the list of bitcask files created"),
    BitcaskFiles = list_bitcask_files(Nodes),
    lager:info("Now update the node to the current version"),
    [rt:upgrade(Node, current) || Node <- Nodes],
    lager:info("And wait until all the old files have been merged, the version upgrade finished"),
    ?assertEqual(ok, rt:wait_until(upgrade_complete_fun(BitcaskFiles))),
    lager:info("And that is that").

write_some_data([Node1 | _]) ->
    rt:pbc_systest_write(Node1, 10000).

list_bitcask_files(Nodes) ->
    [{Node, list_node_bitcask_files(Node)} || Node <- Nodes].

list_node_bitcask_files(Node) ->
    % Gather partitions owned, list *.bitcask.data on each.
    Partitions = rt:partitions_for_node(Node),
    {ok, DataDir} = rt:rpc_get_env(Node, [{bitcask, data_root}]),
    [begin
         IdxStr = integer_to_list(Idx),
         IdxDir = filename:join(DataDir, IdxStr),
         BitcaskPattern = filename:join([IdxDir, "*.bitcask.data"]),
         Paths = rpc:call(Node, filelib, wildcard, [BitcaskPattern]),
         ?assert(is_list(Paths)),
         Files = [filename:basename(Path) || Path <- Paths],
         {IdxDir, Files}
     end || Idx <- Partitions].

upgrade_complete_fun(BitcaskFiles) ->
    fun() ->
            upgrade_complete(BitcaskFiles)
    end.

upgrade_complete(BitcaskFiles) ->
    all(true, [upgrade_complete(Node, PFiles)
               || {Node, PFiles} <- BitcaskFiles]).

upgrade_complete(Node, PartitionFiles) ->
    all(true,[upgrade_complete(Node, IdxDir, Files)
              || {IdxDir, Files} <- PartitionFiles]).

upgrade_complete(Node, IdxDir, Files) ->
    % Check we have version.txt, no upgrade.txt, no merge.txt
    MergeFile = filename:join(IdxDir, "merge.txt"),
    UpgradeFile = filename:join(IdxDir, "upgrade.txt"),
    VsnFile = filename:join(IdxDir, "version.txt"),
    file_exists(Node, VsnFile) andalso
        not file_exists(Node, UpgradeFile) andalso
        not file_exists(Node, MergeFile) andalso
        all(false,
            [file_exists(Node, filename:join(IdxDir, F)) || F <- Files]).

file_exists(Node, Path) ->
    case rpc:call(Node, filelib, is_regular, [Path]) of
        {badrpc, Reason} ->
            throw({can_not_check_file, Node, Path, Reason});
        Result ->
            Result
    end.

all(Val, L) ->
    lists:all(fun(E) -> E == Val end, L).
