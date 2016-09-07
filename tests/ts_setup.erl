%% -*- Mode: Erlang -*-
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015, 2016 Basho Technologies, Inc.
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
%% -------------------------------------------------------------------
%% @doc A util module for riak_ts tests

-module(ts_setup).

-include_lib("eunit/include/eunit.hrl").

-export([start_cluster/0, start_cluster/1, start_cluster/2,
         conn/1, conn/2, stop_a_node/1, stop_a_node/2,
         create_bucket_type/3, create_bucket_type/4,
         activate_bucket_type/2, activate_bucket_type/3]).

-spec start_cluster() -> list(node()).
start_cluster() ->
    start_cluster(1).

-spec start_cluster(NumNodes :: pos_integer()) -> list(node()).
start_cluster(NumNodes) ->
    start_cluster(NumNodes, []).

-spec start_cluster(NumNodes :: pos_integer(),
                    Config :: list(tuple())) -> list(node()).
start_cluster(NumNodes, Config) ->
    rt:set_backend(eleveldb),
    lager:info("Building cluster of ~p~n", [NumNodes]),
    rt:build_cluster(NumNodes, Config).

-spec conn(list(node())) -> pid().
conn(Nodes) ->
    conn(1, Nodes).

-spec conn(pos_integer(), list(node())) -> pid().
conn(Which, Nodes) ->
    Conn = rt:pbc(lists:nth(Which, Nodes)),
    ?assert(is_pid(Conn)),
    Conn.

-spec stop_a_node(list(node())) -> ok.
stop_a_node(Nodes) ->
    stop_a_node(2, Nodes).

-spec stop_a_node(pos_integer(), list(node())) -> ok.
stop_a_node(Which, Nodes) ->
    ok = rt:stop(lists:nth(Which, Nodes)).

-spec create_bucket_type(list(node()), string(), string()) -> ok.
create_bucket_type(Nodes, DDL, Table) ->
    create_bucket_type(Nodes, DDL, Table, 3).

-spec create_bucket_type(list(node()), string(), string(), pos_integer()) -> ok.
create_bucket_type([Node|_Rest], DDL, Table, NVal) when is_integer(NVal) ->
    Props = io_lib:format("{\\\"props\\\": {\\\"n_val\\\": ~s, \\\"table_def\\\": \\\"~s\\\"}}", [integer_to_list(NVal), DDL]),
    {ok, _} = rt:admin(Node, ["bucket-type", "create", table_to_list(Table), lists:flatten(Props)]),
    ok.


%% Attempt to activate the bucket type 4 times
-spec activate_bucket_type([node()], string()) -> {ok, string()} | term().
activate_bucket_type(Cluster, Table) ->
    activate_bucket_type(Cluster, Table, 3).

-spec activate_bucket_type([node()], string(), pos_integer()) -> ok | term().
activate_bucket_type(Cluster, Table, Retries) ->
    [Node|_Rest] = Cluster,
    {ok, Msg} = Result = rt:admin(Node, ["bucket-type", "activate", table_to_list(Table)]),
    %% Look for a successful message
    case string:str(Msg, "has been activated") of
        0 ->
            lager:error("Could not activate bucket type. Retrying. Result = ~p", [Result]),
            case Retries of
                0 -> Result;
                _ -> timer:sleep(timer:seconds(1)),
                     activate_bucket_type(Cluster, Table, Retries-1)
            end;
        _ -> ok
    end.

table_to_list(Table) when is_binary(Table) ->
    binary_to_list(Table);
table_to_list(Table) ->
    Table.
