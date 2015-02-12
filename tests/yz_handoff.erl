%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
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
-module(yz_handoff).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(GET(K,L), proplists:get_value(K, L)).
-define(FMT(S, Args), lists:flatten(io_lib:format(S, Args))).
-define(PATH, (rt_config:get('rtdev_path.current'))).
-define(INDEX, <<"test_idx">>).
-define(BUCKET, <<"test_bkt">>).
-define(SUCCESS, 0).
-define(TESTCYCLE, 20).
-define(STOPWORDS, "No transfers active").
-define(NUMRUNSTATES, 1).
-define(CFG,
        [
         {riak_core,
          [
           {handoff_concurrency, 11},
           {ring_creation_size, 64}
          ]},
         {yokozuna,
          [
           {enabled, true}
          ]}
        ]).

confirm() ->
    %% Setup cluster initially
    Nodes = rt:build_cluster(5, ?CFG),
    rt:wait_for_cluster_service(Nodes, yokozuna),

    [_|Nodes2Rest] = Nodes,
    %% We're going to always keep Node2 in the cluster.
    [Node2|_] = Nodes2Rest,

    ConnInfo = ?GET(Node2, rt:connection_info([Node2])),
    {Host, Port} = ?GET(http, ConnInfo),
    Shards = [begin {ok, P} = node_solr_port(Node), {Node, P} end || Node <- Nodes],

    %% Generate keys, YZ only supports UTF-8 compatible keys
    Keys = [<<N:64/integer>> || N <- lists:seq(1,20000),
                               not lists:any(fun(E) -> E > 127 end,
                                             binary_to_list(<<N:64/integer>>))],
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

    %% Separate out shards for multiple runs
    [Shard1|Shards2Rest] = Shards,
    [Shard2,_|Shards4Rest] = Shards2Rest,
    Shards24Rest = [Shard2|Shards4Rest],
    Shards124Rest = [Shard1|Shards24Rest],
    {_, SolrPort1} = Shard1,
    [{_, SolrPort2}|_] = Shards2Rest,
    SolrURL = internal_solr_url(Host, SolrPort1, ?INDEX, Shards),
    BucketURL = bucket_keys_url(Host, Port, ?BUCKET),

    wait_for_replica_count(SolrURL, KeyCount),

    %% Set Env
    DefaultEnv = [{"SEARCH_URL", search_url(Host, Port, ?INDEX)},
                  {"BUCKET_URL", BucketURL},
                  {"STOPWORDS", ?STOPWORDS},
                  {"ADMIN_PATH_NODE2", ?PATH ++ "/dev/dev2/bin/riak-admin"}],
    Envs = [[{"SOLR_URL_BEFORE", SolrURL},
             {"SOLR_URL_AFTER", internal_solr_url(Host, SolrPort2, ?INDEX, Shards2Rest)},
             {"ADMIN_PATH_NODE", ?PATH ++ "/dev/dev1/bin/riak-admin"}],
            [{"SOLR_URL_BEFORE", internal_solr_url(Host, SolrPort2, ?INDEX, Shards2Rest)},
             {"SOLR_URL_AFTER", internal_solr_url(Host, SolrPort2, ?INDEX, Shards24Rest)},
             {"ADMIN_PATH_NODE", ?PATH ++ "/dev/dev3/bin/riak-admin"}],
            [{"SOLR_URL_BEFORE", internal_solr_url(Host, SolrPort2, ?INDEX, Shards24Rest)},
             {"SOLR_URL_AFTER", internal_solr_url(Host, SolrPort2, ?INDEX, Shards124Rest)},
             {"ADMIN_PATH_NODE", ?PATH ++ "/dev/dev1/bin/riak-admin"},
             {"RIAK_PATH_NODE", ?PATH ++ "/dev/dev1/bin/riak"},
             {"JOIN_NODE", atom_to_list(Node2)}],
            [{"SOLR_URL_BEFORE", internal_solr_url(Host, SolrPort2, ?INDEX, Shards124Rest)},
             {"SOLR_URL_AFTER", SolrURL},
             {"ADMIN_PATH_NODE", ?PATH ++ "/dev/dev3/bin/riak-admin"},
             {"RIAK_PATH_NODE", ?PATH ++ "/dev/dev3/bin/riak"},
             {"JOIN_NODE", atom_to_list(Node2)}]],

    %% Run Shell Script to count/test # of replicas and leave/join
    %% nodes from the cluster
    [[begin
          check_data(receiver(spawn_handoff_executable(lists:append(Env,DefaultEnv)),
                              []), KeyCount),
          check_counts(Pid, KeyCount, BucketURL)
      end || Env <- Envs]
     || _ <- lists:seq(1,?NUMRUNSTATES)],

    pass.

%%%===================================================================
%%% Private
%%%===================================================================

spawn_handoff_executable(Env) ->
    erlang:open_port({spawn_executable, "handoff-test.sh"},
                     [exit_status, {env, Env}, stderr_to_stdout]).

receiver(P, Acc) ->
    receive
        {P, {data, Data}} ->
            Output = [list_to_tuple(string:tokens(I, " "))
                      || I <- string:tokens(Data, "\n")],
            lager:info("From Script: ~p", [Output]),
            receiver(P, lists:append(
                          [{list_to_atom(K), V} || {K, V} <- Output], Acc));
        {P, {exit_status, Status}} ->
            lager:info("Exited with status ~b", [Status]),
            ?assertEqual(?SUCCESS, Status),
            Acc;
        {P, Reason} ->
            lager:warning("Unexpected return from port: ~p", [Reason]),
            catch erlang:port_close(P),
            exit(Reason)
    after 300000 ->
            lager:warning("Timeout on port: ~p", [P]),
            catch erlang:port_close(P),
            exit(timeout)
    end.

node_solr_port(Node) ->
    riak_core_util:safe_rpc(Node, application, get_env,
                            [yokozuna, solr_port]).

internal_solr_url(Host, Port, Index) ->
    ?FMT("http://~s:~B/internal_solr/~s", [Host, Port, Index]).
internal_solr_url(Host, Port, Index, Shards) ->
    Ss = [internal_solr_url(Host, ShardPort, Index)
          || {_, ShardPort} <- Shards],
    ?FMT("http://~s:~B/internal_solr/~s/select?wt=json&q=*:*&shards=~s",
         [Host, Port, Index, string:join(Ss, ",")]).

%% @private
bucket_keys_url(Host, Port, BName) ->
    ?FMT("http://~s:~B/buckets/~s/keys?keys=true", [Host, Port, BName]).

%% @private
search_url(Host, Port, Index) ->
    ?FMT("http://~s:~B/solr/~s/select?wt=json&q=*:*", [Host, Port, Index]).

%% @private
wait_for_index(Cluster, Index) ->
    IsIndexUp =
        fun(Node) ->
                lager:info("Waiting for index ~s to be avaiable on node ~p", [Index, Node]),
                rpc:call(Node, yz_solr, ping, [Index])
        end,
    [?assertEqual(ok, rt:wait_until(Node, IsIndexUp)) || Node <- Cluster],
    ok.

wait_for_replica_count(SolrURL, KeyCount) ->
    AreReplicasUp =
        fun() ->
                lager:info("Waiting for replica count to be 3 * docs"),
                {ok, "200", _, RBody} = ibrowse:send_req(SolrURL, [], get, []),
                FoundCount = get_count(RBody),
                lager:info("Replicas Count ~b", [FoundCount]),
                FoundCount >= KeyCount * 3
        end,
    ?assertEqual(ok, rt:wait_until(AreReplicasUp, 100, 1000)),
    ok.

get_count(Resp) ->
    Struct = mochijson2:decode(Resp),
    kvc:path([<<"response">>, <<"numFound">>], Struct).

get_keys_count(Resp) ->
    Struct = mochijson2:decode(Resp),
    length(kvc:path([<<"keys">>], Struct)).

check_counts(Pid, InitKeyCount, BucketURL) ->
    PBCounts = [begin {ok, Resp} = riakc_pb_socket:search(Pid, ?INDEX, <<"*:*">>),
                    Resp#search_results.num_found
                end || _ <- lists:seq(1,?TESTCYCLE)],
    HTTPCounts = [begin {ok, "200", _, RBody} = ibrowse:send_req(BucketURL, [], get, []),
                        get_keys_count(RBody)
                  end || _ <- lists:seq(1,?TESTCYCLE)],
    MinPBCount = lists:min(PBCounts),
    MinHTTPCount = lists:min(HTTPCounts),
    lager:info("Before-Node-Leave PB: ~b, After-Node-Leave PB: ~b", [InitKeyCount, MinPBCount]),
    ?assertEqual(InitKeyCount, MinPBCount),
    lager:info("Before-Node-Leave PB: ~b, After-Node-Leave HTTP: ~b", [InitKeyCount, MinHTTPCount]),
    ?assertEqual(InitKeyCount, MinHTTPCount).

check_data(Data, KeyCount) ->
    lager:info("Running Asserts on Data"),

    CheckCount     = KeyCount * 3,
    KeysBefore     = list_to_integer(?GET(keys1, Data)),
    KeysAfter      = list_to_integer(?GET(keys2, Data)),
    DocsBefore     = list_to_integer(?GET(docs1, Data)),
    DocsAfter      = list_to_integer(?GET(docs1, Data)),
    ReplicasBefore = list_to_integer(?GET(reps1, Data)),
    ReplicasAfter  = list_to_integer(?GET(reps2, Data)),

    lager:info("KeysBefore: ~b, KeysAfter: ~b", [KeysBefore, KeysAfter]),
    ?assertEqual(KeysBefore, KeysAfter),
    lager:info("DocsBefore: ~b, DocsAfter: ~b", [DocsBefore, DocsAfter]),
    ?assertEqual(DocsBefore, DocsAfter),

    lager:info("ReplicasBefore - ~b should be - ~b", [ReplicasBefore, CheckCount]),
    ?assertEqual(ReplicasBefore, CheckCount),

    %% Same Replica Count Check - Always Failing Currently
    lager:info("ReplicasBefore: ~b, ReplicasAfter: ~b", [ReplicasBefore, ReplicasAfter]),
    ?assertEqual(ReplicasBefore, ReplicasAfter).
