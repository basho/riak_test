%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%-------------------------------------------------------------------
-module(yokozuna_rt).

-include_lib("eunit/include/eunit.hrl").
-include("yokozuna_rt.hrl").

-export([check_exists/2,
         clear_trees/1,
         commit/2,
         expire_trees/1,
         ensure_complete_aae_round/1,
         gen_keys/1,
         host_entries/1,
         override_schema/5,
         remove_index_dirs/2,
         rolling_upgrade/2,
         search/4,
         search/5,
         search_expect/5,
         search_expect/6,
         search_expect/7,
         assert_search/6,
         verify_num_found_query/3,
         wait_for_aae/1,
         wait_for_full_exchange_round/1,
         wait_for_full_exchange_round/2,
         wait_for_full_exchange_round/3,
         wait_for_index/2,
         wait_for_schema/2,
         wait_for_schema/3,
         write_data/5,
         write_data/6]
         ).

-type host() :: string().
-type portnum() :: integer().
-type count() :: non_neg_integer().
-type json_string() :: atom | string() | binary().

-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(SOFTCOMMIT, 1000).

-spec host_entries(rt:conn_info()) -> [{host(), portnum()}].
host_entries(ClusterConnInfo) ->
    [riak_http(I) || {_,I} <- ClusterConnInfo].

%% @doc Generate `SeqMax' keys. Yokozuna supports only UTF-8 compatible keys.
-spec gen_keys(pos_integer()) -> [binary()].
gen_keys(SeqMax) ->
    [<<N:64/integer>> || N <- lists:seq(1, SeqMax),
                         not lists:any(
                               fun(E) -> E > 127 end,
                               binary_to_list(<<N:64/integer>>))].

%% @doc Write `Keys' via the PB inteface to a `Bucket' and have them
%%      searchable in an `Index'.
-spec write_data([node()], pid(), index_name(), bucket(), [binary()]) -> ok.
write_data(Cluster, Pid, Index, Bucket, Keys) ->
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    create_and_set_index(Cluster, Pid, Bucket, Index),
    timer:sleep(1000),

    %% Write keys
    lager:info("Writing ~p keys", [length(Keys)]),
    [ok = rt:pbc_write(Pid, Bucket, Key, Key, "text/plain") || Key <- Keys],
    ok.

-spec write_data([node()], pid(), index_name(), {schema_name(), raw_schema()},
                 bucket(), [binary()]) -> ok.
write_data(Cluster, Pid, Index, {SchemaName, SchemaData},
           Bucket, Keys) ->
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    riakc_pb_socket:create_search_schema(Pid, SchemaName, SchemaData),

    create_and_set_index(Cluster, Pid, Bucket, Index, SchemaName),
    timer:sleep(1000),

    %% Write keys
    lager:info("Writing ~p keys", [length(Keys)]),
    [ok = rt:pbc_write(Pid, Bucket, Key, Key, "text/plain") || Key <- Keys],
    ok.

%% @doc Peform a rolling upgrade of the `Cluster' to a different `Version' based
%%      on current | previous | legacy.
-spec rolling_upgrade([node()], current | previous | legacy) -> ok.
rolling_upgrade(Cluster, Version) ->
    rolling_upgrade(Cluster, Version, same, [riak_kv, yokozuna]).
-spec rolling_upgrade([node()], current | previous | legacy, [term()] | same, [atom()]) -> ok.
rolling_upgrade(Cluster, Version, UpgradeConfig, WaitForServices) when is_list(Cluster) ->
    lager:info("Perform rolling upgrade on cluster ~p", [Cluster]),
    [rolling_upgrade(Node, Version, UpgradeConfig, WaitForServices) || Node <- Cluster],
    ok;
rolling_upgrade(Node, Version, UpgradeConfig, WaitForServices) ->
    rt:upgrade(Node, Version, UpgradeConfig),
    [rt:wait_for_service(Node, Service) || Service <- WaitForServices],
    ok.

%% @doc Use AAE status to verify that exchange has occurred for all
%%      partitions since the time this function was invoked.
-spec wait_for_aae([node()]) -> ok.
wait_for_aae(Cluster) ->
    wait_for_aae(Cluster, os:timestamp()).

%% @doc Use AAE status to verify that exchange has occurred for all
%%      partitions since Timestamp
-spec wait_for_aae([node()], erlang:timestamp()) -> ok.
wait_for_aae(Cluster, Timestamp) ->
    wait_for_aae(Cluster, Timestamp, false).

%% @doc Use AAE status to verify that exchange has occurred for all
%%      partitions since Timestamp
-spec wait_for_aae([node()], erlang:timestamp(), boolean()) -> ok.
wait_for_aae(Cluster, Timestamp, ForceExchanges) ->
    lager:info("Wait for AAE to migrate/repair indexes"),
    wait_for_all_trees(Cluster),
    wait_for_full_exchange_round(Cluster, Timestamp, ForceExchanges),
    ok.

%% @doc Wait for all AAE trees to be built.
-spec wait_for_all_trees([node()]) -> ok.
wait_for_all_trees(Cluster) ->
    F = fun(Node) ->
                lager:info("Check if all trees built for node ~p", [Node]),
                Info = rpc:call(Node, yz_kv, compute_tree_info, []),
                NotBuilt = [X || {_,undefined}=X <- Info],
                NotBuilt == []
        end,
    rt:wait_until(Cluster, F),
    ok.
-spec wait_for_full_exchange_round([node()]) -> ok.
wait_for_full_exchange_round(Cluster) ->
    wait_for_full_exchange_round(Cluster, erlang:now()).

%% @doc Wait for a full exchange round since `Timestamp'.  This means
%% that all `{Idx,N}' for all partitions must have exchanged after
%% `Timestamp'.
-spec wait_for_full_exchange_round([node()], erlang:timestamp()) -> ok.
wait_for_full_exchange_round(Cluster, Timestamp) ->
    wait_for_full_exchange_round(Cluster, Timestamp, false).
%% @doc Wait for a full exchange round since `Timestamp'.  This means
%% that all `{Idx,N}' for all partitions must have exchanged after
%% `Timestamp'. If `ForceExchangeRound` is true, will also call manual_exchange
%% for any waiting exchanges.
-spec wait_for_full_exchange_round([node()], erlang:timestamp(), boolean()) -> ok.
wait_for_full_exchange_round(Cluster, Timestamp, ForceExchanges) ->
    lager:info("wait for full AAE exchange round on cluster ~p", [Cluster]),
    AllExchanged =
        fun(Node) ->
            Exchanges = get_exchanges_for_node(Node),
            WaitingFor = get_exchanges_older_than(Timestamp, Exchanges),
            lager:info("Still waiting for AAE of ~p ~p", [Node, WaitingFor]),
            case ForceExchanges of
                true -> force_exchanges(Node, WaitingFor);
                _ -> ok
            end,
            [] == WaitingFor
        end,
    rt:wait_until(Cluster, AllExchanged),
    ok.

-spec force_exchanges(node(), non_neg_integer()) -> ok.
force_exchanges(Node, Indexes) ->
    _ = [rpc:call(Node, yz_entropy_mgr, manual_exchange, [Index]) || Index <- Indexes],
    ok.

get_exchanges_older_than(Timestamp, Exchanges) ->
    {_Recent, WaitingFor} = lists:partition(exchange_is_more_recent_than(Timestamp), Exchanges),
    [element(1,X) || X <- WaitingFor].

get_exchanges_for_node(Node) ->
    rpc:call(Node, yz_kv, compute_exchange_info, []).

exchange_is_more_recent_than(Timestamp) ->
    fun({_Idx, _, undefined, _RepairStats}) ->
            false;
       ({_Idx, _, AllExchangedTime, _RepairStats}) ->
            AllExchangedTime > Timestamp
    end.


%% @doc Wait for index creation. This is to handle *legacy* versions of yokozuna
%%      in upgrade tests
-spec wait_for_index(list(), index_name()) -> ok.
wait_for_index(Cluster, Index) ->
    IsIndexUp =
        fun(Node) ->
                lager:info("Waiting for index ~s to be avaiable on node ~p",
                           [Index, Node]),
                rpc:call(Node, yz_solr, ping, [Index])
        end,
    [?assertEqual(ok, rt:wait_until(Node, IsIndexUp)) || Node <- Cluster],
    ok.

%% @see wait_for_schema/3
wait_for_schema(Cluster, Name) ->
    wait_for_schema(Cluster, Name, ignore).

%% @doc Wait for the schema `Name' to be read by all nodes in
%% `Cluster' before returning.  If `Content' is binary data when
%% verify the schema bytes exactly match `Content'.
-spec wait_for_schema([node()], schema_name(), ignore | raw_schema()) -> ok.
wait_for_schema(Cluster, Name, Content) ->
    F = fun(Node) ->
                lager:info("Attempt to read schema ~s from node ~p",
                           [Name, Node]),
                {Host, Port} = riak_pb(hd(rt:connection_info([Node]))),
                {ok, PBConn} = riakc_pb_socket:start_link(Host, Port),
                R = riakc_pb_socket:get_search_schema(PBConn, Name),
                riakc_pb_socket:stop(PBConn),
                case R of
                    {ok, PL} ->
                        case Content of
                            ignore ->
                                Name == proplists:get_value(name, PL);
                            _ ->
                                (Name == proplists:get_value(name, PL)) and
                                    (Content == proplists:get_value(content, PL))
                        end;
                    _ ->
                        false
                end
        end,
    rt:wait_until(Cluster,  F),
    ok.

%% @doc Expire YZ trees
-spec expire_trees([node()]) -> ok.
expire_trees(Cluster) ->
    lager:info("Expire all trees"),
    _ = [ok = rpc:call(Node, yz_entropy_mgr, expire_trees, [])
         || Node <- Cluster],

    %% The expire is async so just give it a moment
    timer:sleep(100),
    ok.

%% @doc Expire trees and wait for aae. If any indexes were exchanged
%% before this function is called, they will be forced to rerun
%% via a call to yz_entropy_mgr:manual_exchange
-spec ensure_complete_aae_round([node()]) -> ok.
ensure_complete_aae_round(Cluster) ->
    Timestamp = os:timestamp(),
    expire_trees(Cluster),
    wait_for_aae(Cluster, Timestamp, true).

%% @doc Expire YZ trees
-spec clear_trees([node()]) -> ok.
clear_trees(Cluster) ->
    lager:info("Expire all trees"),
    _ = [ok = rpc:call(Node, yz_entropy_mgr, clear_trees, [])
         || Node <- Cluster],
    ok.


%% @doc Remove index directories, removing the index.
-spec remove_index_dirs([node()], index_name()) -> ok.
remove_index_dirs(Nodes, IndexName) ->
    IndexDirs = [rpc:call(Node, yz_index, index_dir, [IndexName]) ||
                    Node <- Nodes],
    lager:info("Remove index dirs: ~p, on nodes: ~p~n",
               [IndexDirs, Nodes]),
    [rt:stop(ANode) || ANode <- Nodes],
    [rt:del_dir(binary_to_list(IndexDir)) || IndexDir <- IndexDirs],
    [rt:start(ANode) || ANode <- Nodes],
    ok.

%% @doc Check if index/core exists in metadata, disk via yz_index:exists.
-spec check_exists([node()], index_name()) -> ok.
check_exists(Nodes, IndexName) ->
    rt:wait_until(Nodes,
                  fun(N) ->
                          rpc:call(N, yz_index, exists, [IndexName])
                  end).

-spec verify_num_found_query([node()], index_name(), count()) -> ok.
verify_num_found_query(Cluster, Index, ExpectedCount) ->
    F = fun(Node) ->
                Pid = rt:pbc(Node),
                {ok, {_, _, _, NumFound}} = riakc_pb_socket:search(Pid, Index, <<"*:*">>),
                lager:info("Check Count, Expected: ~p | Actual: ~p~n",
                           [ExpectedCount, NumFound]),
                ExpectedCount =:= NumFound
        end,
    rt:wait_until(Cluster, F),
    ok.

search_expect(HP, Index, Name, Term, Expect) ->
    search_expect(yokozuna, HP, Index, Name, Term, Expect).

search_expect(Type, HP, Index, Name, Term, Expect) ->
    {ok, "200", _, R} = search(Type, HP, Index, Name, Term),
    verify_count_http(Expect, R).

search_expect(solr, {Host, Port}, Index, Name0, Term0, Shards, Expect)
  when is_list(Shards), length(Shards) > 0 ->
    Name = quote_unicode(Name0),
    Term = quote_unicode(Term0),
    URL = internal_solr_url(Host, Port, Index, Name, Term, Shards),
    lager:info("Run search ~s", [URL]),
    Opts = [{response_format, binary}],
    {ok, "200", _, R} = ibrowse:send_req(URL, [], get, [], Opts),
    verify_count_http(Expect, R).

assert_search(Pid, Cluster, Index, Search, SearchExpect, Params) ->
    F = fun(_) ->
                lager:info("Searching ~p and asserting it exists",
                           [SearchExpect]),
                case riakc_pb_socket:search(Pid, Index, Search, Params) of
                    {ok,{search_results,[{_Index,Fields}], _Score, Found}} ->
                        ?assert(lists:member(SearchExpect, Fields)),
                        case Found of
                            1 -> true;
                            0 -> false
                        end;
                    {ok, {search_results, [], _Score, 0}} ->
                        lager:info("Search has not yet yielded data"),
                        false
                end
        end,
    rt:wait_until(Cluster, F).

search(HP, Index, Name, Term) ->
    search(yokozuna, HP, Index, Name, Term).

search(Type, {Host, Port}, Index, Name, Term) when is_integer(Port) ->
    search(Type, {Host, integer_to_list(Port)}, Index, Name, Term);

search(Type, {Host, Port}, Index, Name0, Term0) ->
    Name = quote_unicode(Name0),
    Term = quote_unicode(Term0),
    FmtStr = case Type of
                 solr ->
                     "http://~s:~s/internal_solr/~s/select?q=~s:~s&wt=json";
                 yokozuna ->
                     "http://~s:~s/search/query/~s?q=~s:~s&wt=json"
             end,
    URL = ?FMT(FmtStr, [Host, Port, Index, Name, Term]),
    lager:info("Run search ~s", [URL]),
    Opts = [{response_format, binary}],
    ibrowse:send_req(URL, [], get, [], Opts).

%%%===================================================================
%%% Private
%%%===================================================================

-spec verify_count_http(count(), json_string()) -> boolean().
verify_count_http(Expected, Resp) ->
    Count = get_count_http(Resp),
    lager:info("Expected: ~p, Actual: ~p", [Expected, Count]),
    ?assertEqual(Expected, Count).

-spec get_count_http(json_string()) -> count().
get_count_http(Resp) ->
    Struct = mochijson2:decode(Resp),
    kvc:path([<<"response">>, <<"numFound">>], Struct).

-spec riak_http({node(), rt:interfaces()} | rt:interfaces()) ->
                       {host(), portnum()}.
riak_http({_Node, ConnInfo}) ->
    riak_http(ConnInfo);
riak_http(ConnInfo) ->
    proplists:get_value(http, ConnInfo).

-spec riak_pb({node(), rt:interfaces()} | rt:interfaces()) ->
                     {host(), portnum()}.
riak_pb({_Node, ConnInfo}) ->
    riak_pb(ConnInfo);
riak_pb(ConnInfo) ->
    proplists:get_value(pb, ConnInfo).

-spec create_and_set_index([node()], pid(), bucket(), index_name()) -> ok.
create_and_set_index(Cluster, Pid, Bucket, Index) ->
    %% Create a search index and associate with a bucket
    lager:info("Create a search index ~s and associate it with bucket ~s",
               [Index, Bucket]),
    ok = riakc_pb_socket:create_search_index(Pid, Index),
    %% For possible legacy upgrade reasons, wrap create index in a wait
    wait_for_index(Cluster, Index),
    set_index(Pid, hd(Cluster), Bucket, Index).
-spec create_and_set_index([node()], pid(), bucket(), index_name(),
                           schema_name()) -> ok.
create_and_set_index(Cluster, Pid, Bucket, Index, Schema) ->
    %% Create a search index and associate with a bucket
    lager:info("Create a search index ~s with a custom schema named ~s and " ++
               "associate it with bucket ~p", [Index, Schema, Bucket]),
    ok = riakc_pb_socket:create_search_index(Pid, Index, Schema, []),
    %% For possible legacy upgrade reasons, wrap create index in a wait
    wait_for_index(Cluster, Index),
    set_index(Pid, hd(Cluster), Bucket, Index).

-spec set_index(pid(), node(), bucket(), index_name()) -> ok.
set_index(_Pid, Node, {BucketType, _Bucket}, Index) ->
    lager:info("Create and activate map-based bucket type ~s and tie it to search_index ~s",
               [BucketType, Index]),
    rt:create_and_activate_bucket_type(Node, BucketType, [{search_index, Index}]);
set_index(Pid, _Node, Bucket, Index) ->
    ok = riakc_pb_socket:set_search_index(Pid, Bucket, Index).

internal_solr_url(Host, Port, Index) ->
    ?FMT("http://~s:~B/internal_solr/~s", [Host, Port, Index]).
internal_solr_url(Host, Port, Index, Name, Term, Shards) ->
    Ss = [internal_solr_url(Host, ShardPort, Index)
          || {_, ShardPort} <- Shards],
    ?FMT("http://~s:~B/internal_solr/~s/select?wt=json&q=~s:~s&shards=~s",
         [Host, Port, Index, Name, Term, string:join(Ss, ",")]).

quote_unicode(Value) ->
    mochiweb_util:quote_plus(binary_to_list(
                               unicode:characters_to_binary(Value))).

-spec commit([node()], index_name()) -> ok.
commit(Nodes, Index) ->
    %% Wait for yokozuna index to trigger, then force a commit
    timer:sleep(?SOFTCOMMIT),
    lager:info("Commit search writes to ~s at softcommit (default) ~p",
               [Index, ?SOFTCOMMIT]),
    rpc:multicall(Nodes, yz_solr, commit, [Index]),
    ok.

-spec override_schema(pid(), [node()], index_name(), schema_name(), string()) ->
                             {ok, [node()]}.
override_schema(Pid, Cluster, Index, Schema, RawUpdate) ->
    lager:info("Overwrite schema with updated schema"),
    ok = riakc_pb_socket:create_search_schema(Pid, Schema, RawUpdate),
    yokozuna_rt:wait_for_schema(Cluster, Schema, RawUpdate),
    [Node|_] = Cluster,
    {ok, _} = rpc:call(Node, yz_index, reload, [Index]).
