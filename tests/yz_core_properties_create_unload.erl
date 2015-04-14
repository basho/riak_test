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
-module(yz_core_properties_create_unload).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(CFG, [{yokozuna, [{enabled, true}]}]).
-define(INDEX, <<"test_idx_core">>).
-define(BUCKET, <<"test_bkt_core">>).
-define(SEQMAX, 100).

confirm() ->
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    %% Generate keys, YZ only supports UTF-8 compatible keys
    Keys = [<<N:64/integer>> || N <- lists:seq(1, ?SEQMAX),
                               not lists:any(fun(E) -> E > 127 end,
                                             binary_to_list(<<N:64/integer>>))],
    KeyCount = length(Keys),

    %% Randomly select a subset of the test nodes to remove
    %% core.properties from
    RandNodes = rt:random_sublist(Cluster, 3),

    %% Select one of the modified nodes as a client endpoint
    Node = rt:select_random(RandNodes),
    Pid = rt:pbc(Node),
    riakc_pb_socket:set_options(Pid, [queue_if_disconnected]),

    %% Create a search index and associate with a bucket
    lager:info("Create and set Index ~p for Bucket ~p~n", [?INDEX, ?BUCKET]),
    ok = riakc_pb_socket:create_search_index(Pid, ?INDEX),
    ok = riakc_pb_socket:set_search_index(Pid, ?BUCKET, ?INDEX),
    timer:sleep(1000),

    %% Write keys and wait for soft commit
    lager:info("Writing ~p keys", [KeyCount]),
    [ok = rt:pbc_write(Pid, ?BUCKET, Key, Key, "text/plain") || Key <- Keys],
    timer:sleep(1100),

    verify_count(Pid, KeyCount),

    %% Remove core.properties from the selected subset
    remove_core_props(RandNodes),

    wait_until(RandNodes,
               fun(N) ->
                       rpc:call(N, yz_index, exists, [?INDEX])
               end),

    lager:info("Write one more piece of data"),
    ok = rt:pbc_write(Pid, ?BUCKET, <<"foo">>, <<"foo">>, "text/plain"),
    timer:sleep(1100),

    verify_count(Pid, KeyCount + 1),

    riakc_pb_socket:stop(Pid),

    pass.

%% @doc Verify search count.
verify_count(Pid, ExpectedKeyCount) ->
    case riakc_pb_socket:search(Pid, ?INDEX, <<"*:*">>) of
        {ok ,{search_results, _, _, NumFound}} ->
            lager:info("Check Count, Expected: ~p | Actual: ~p~n",
                       [ExpectedKeyCount, NumFound]),
            ?assertEqual(ExpectedKeyCount, NumFound);
        E ->
            lager:info("No results because ~p~n", [E])
    end.

%% @doc Remove core properties file on nodes.
remove_core_props(Nodes) ->
    IndexDirs = [rpc:call(Node, yz_index, index_dir, [?INDEX]) ||
                Node <- Nodes],
    PropsFiles = [filename:join([IndexDir, "core.properties"]) ||
                     IndexDir <- IndexDirs],
    lager:info("Remove core.properties files: ~p, on nodes: ~p~n",
               [PropsFiles, Nodes]),
    [file:delete(PropsFile) || PropsFile <- PropsFiles],
    ok.

%% @doc Wrapper around `rt:wait_until' to verify `F' against multiple
%%      nodes.  The function `F' is passed one of the `Nodes' as
%%      argument and must return a `boolean()' delcaring whether the
%%      success condition has been met or not.
-spec wait_until([node()], fun((node()) -> boolean())) -> ok.
wait_until(Nodes, F) ->
    [?assertEqual(ok, rt:wait_until(Node, F)) || Node <- Nodes],
    ok.
