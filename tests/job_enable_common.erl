%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(job_enable_common).

% Public API for use by other tests
-export([
    bin_bucket/1,
    bin_key/1,
    bin_val/1,
    close_client/1,
    enabled_string/1,
    index_2i/0,
    index_yz/0,
    load_data/1,
    num_buckets/0, num_buckets/1,
    num_keys/0, num_keys/1,
    open_client/2,
    populated_bucket/0,
    setup_cluster/1,
    test_buckets/0,
    test_keys/0,
    test_nums/0,
    test_operation/4,
    test_vals/0,
    undefined_bucket/0
]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").
-include("job_enable_common.hrl").

-define(DEFAULT_NUM_BUCKETS,    7).
-define(DEFAULT_NUM_KEYS,       9).

%% ===================================================================
%% Test API
%% ===================================================================

enabled_string(true) ->
    "enabled";
enabled_string(false) ->
    "disabled".

bin_bucket(Num) ->
    erlang:list_to_binary(["Bucket_", erlang:integer_to_list(Num)]).

bin_key(Num) ->
    erlang:list_to_binary(["Key_", erlang:integer_to_list(Num)]).

bin_val(Num) ->
    erlang:list_to_binary(["Val_", erlang:integer_to_list(Num)]).

index_2i() ->
    {integer_index, "valnum_index_2i"}.

index_yz() ->
    <<"valnum_index_yz">>.

num_buckets() ->
    Key = {?MODULE, num_buckets},
    case erlang:get(Key) of
        undefined ->
            Num = ?DEFAULT_NUM_BUCKETS,
            erlang:put(Key, Num),
            Num;
        Val ->
            Val
    end.

num_buckets(Num) when erlang:is_integer(Num) andalso Num > 0 ->
    Key = {?MODULE, num_buckets},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, Num),
            Num;
        Num ->
            Num;
        _ ->
            erlang:erase({?MODULE, test_buckets}),
            erlang:erase({?MODULE, populated_bucket}),
            erlang:put(Key, Num),
            Num
    end.

num_keys() ->
    Key = {?MODULE, num_keys},
    case erlang:get(Key) of
        undefined ->
            Num = ?DEFAULT_NUM_KEYS,
            erlang:put(Key, Num),
            Num;
        Val ->
            Val
    end.

num_keys(Num) when erlang:is_integer(Num) andalso Num > 0 ->
    Key = {?MODULE, num_keys},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, Num),
            Num;
        Num ->
            Num;
        _ ->
            erlang:erase({?MODULE, test_keys}),
            erlang:erase({?MODULE, test_nums}),
            erlang:erase({?MODULE, test_vals}),
            erlang:put(Key, Num),
            Num
    end.

populated_bucket() ->
    Key = {?MODULE, populated_bucket},
    case erlang:get(Key) of
        undefined ->
            Buckets = test_buckets(),
            Bucket = lists:nth(erlang:length(Buckets) div 2, Buckets),
            erlang:put(Key, Bucket),
            Bucket;
        Val ->
            Val
    end.

undefined_bucket() ->
    <<"Undefined_Bucket">>.

test_buckets() ->
    Key = {?MODULE, test_buckets},
    case erlang:get(Key) of
        undefined ->
            New = bin_buckets(num_buckets(), []),
            erlang:put(Key, New),
            New;
        Val ->
            Val
    end.

test_keys() ->
    Key = {?MODULE, test_keys},
    case erlang:get(Key) of
        undefined ->
            New = bin_keys(num_keys(), []),
            erlang:put(Key, New),
            New;
        Val ->
            Val
    end.

test_nums() ->
    Key = {?MODULE, test_nums},
    case erlang:get(Key) of
        undefined ->
            New = lists:seq(1, num_keys()),
            erlang:put(Key, New),
            New;
        Val ->
            Val
    end.

test_vals() ->
    Key = {?MODULE, test_vals},
    case erlang:get(Key) of
        undefined ->
            New = bin_vals(num_keys(), []),
            erlang:put(Key, New),
            New;
        Val ->
            Val
    end.

open_client(http = Type, Node) ->
    % HTTP connections are constant records, so re-use them
    Key = {?MODULE, httpc, Node},
    case erlang:get(Key) of
        undefined ->
            New = {Type, rhc, rt:httpc(Node)},
            erlang:put(Key, New),
            New;
        Conn ->
            Conn
    end;
open_client(pbc = Type, Node) ->
    {Type, riakc_pb_socket, rt:pbc(Node)}.

close_client({http, _Mod, _RHC}) ->
    ok;
close_client({pbc, Mod, PBC}) ->
    Mod:stop(PBC).

setup_cluster([Node | _] = Nodes) ->
    lager:info("Creating a cluster of ~b nodes ...", [erlang:length(Nodes)]),
    ?assertEqual(ok, rt:join_cluster(Nodes)),
    load_data(Node),
    ?assertEqual(ok, rt:wait_until_transfers_complete(Nodes)).

load_data(Node) ->
    lager:info("Writing known data to node ~p ...", [Node]),
    PBConn = rt:pbc(Node),
    load_data(PBConn, populated_bucket(), test_buckets()),
    riakc_pb_socket:stop(PBConn).

test_operation(Node, ?TOKEN_LIST_BUCKETS, Enabled, ClientType) ->
    {_, Mod, Conn} = Client = open_client(ClientType, Node),
    Result = Mod:list_buckets(Conn),
    close_client(Client),
    case Enabled of
        true ->
            ?assertMatch({ok, _}, Result),
            {ok, Buckets} = Result,
            ?assertEqual(test_buckets(), lists:sort(Buckets));
        false ->
            case ClientType of
                pbc ->
                    ?assertEqual({error, ?ERRMSG_LIST_BUCKETS_DISABLED}, Result);
                http ->
                    ?assertMatch({error, {"403", _}}, Result)
            end
    end;

test_operation(Node, ?TOKEN_LIST_KEYS, Enabled, ClientType) ->
    {_, Mod, Conn} = Client = open_client(ClientType, Node),
    Result = Mod:list_keys(Conn, populated_bucket()),
    close_client(Client),
    case Enabled of
        true ->
            ?assertMatch({ok, _}, Result),
            {ok, Keys} = Result,
            ?assertEqual(test_keys(), lists:sort(Keys));
        false ->
            case ClientType of
                pbc ->
                    ?assertEqual({error, ?ERRMSG_LIST_KEYS_DISABLED}, Result);
                http ->
                    ?assertMatch({error, {"403", _}}, Result)
            end
    end;

test_operation(Node, ?TOKEN_MAP_REDUCE, Enabled, ClientType) ->
    Bucket = populated_bucket(),
    {_, Mod, Conn} = Client = open_client(ClientType, Node),
    Result = Mod:mapred(Conn, Bucket, []),
    close_client(Client),
    case Enabled of
        true ->
            ?assertMatch({ok, [{_, _}]}, Result),
            {ok, [{_, Pairs}]} = Result,
            Expect = case ClientType of
                pbc ->
                    [{Bucket, Key} || Key <- test_keys()];
                http ->
                    [[Bucket, Key] || Key <- test_keys()]
            end,
            ?assertEqual(Expect, lists:sort(Pairs));
        false ->
            case ClientType of
                pbc ->
                    ?assertEqual({error, ?ERRMSG_MAP_REDUCE_DISABLED}, Result);
                http ->
                    ?assertMatch({error, {"403", _}}, Result)
            end
    end;

test_operation(Node, ?TOKEN_SEC_INDEX, Enabled, ClientType) ->
    Bucket = populated_bucket(),
    Index = index_2i(),
    Num = random:uniform(num_keys()),
    {_, Mod, Conn} = Client = open_client(ClientType, Node),
    Result = Mod:get_index(Conn, Bucket, Index, Num),
    close_client(Client),
    case Enabled of
        true ->
            Key = bin_key(Num),
            ?assertMatch({ok, {index_results_v1, [Key], _, _}}, Result);
        false ->
            case ClientType of
                pbc ->
                    ?assertEqual({error, ?ERRMSG_SEC_INDEX_DISABLED}, Result);
                http ->
                    ?assertMatch({error, {"403", _}}, Result)
            end
    end;

%% This requires that YZ be running and that
%%  riakc_pb_socket:create_search_index(Connection, index_yz())
%% (or equivalent) has been successfully called before invoking this test.
%% This module's load_data/1 function DOES NOT do this for you by default.
test_operation(Node, ?TOKEN_YZ_SEARCH, Enabled, pbc = ClientType) ->
    Index   = index_yz(),
    Bucket  = populated_bucket(),
    Num     = random:uniform(num_keys()),
    Key     = bin_key(Num),
    Query   = <<"_yz_rb:", Bucket/binary, " AND _yz_rk:", Key/binary>>,
    {_, Mod, Conn} = Client = open_client(ClientType, Node),
    Result = Mod:search(Conn, Index, Query),
    close_client(Client),
    case Enabled of
        true ->
            ?assertMatch({ok, #search_results{}}, Result);
        false ->
            ?assertEqual({error, ?ERRMSG_YZ_SEARCH_DISABLED}, Result)
    end;
test_operation(Node, ?TOKEN_YZ_SEARCH, Enabled, http) ->
    % The rhs module's search functions don't do anything remotely like their
    % PB equivalents, so this specific test has to depart from the pattern
    % entirely.
    Bucket = populated_bucket(),
    Num = random:uniform(num_keys()),
    Key = bin_key(Num),
    % more reliable than calling rt:get_https_conn_info directly
    RHC = rt:httpc(Node),
    URL = lists:flatten([
            "http://", rhc:ip(RHC), ":", erlang:integer_to_list(rhc:port(RHC)),
            "/search/query/", erlang:binary_to_list(index_yz()),
            "?wt=json&q=_yz_rb:", erlang:binary_to_list(Bucket),
            "%20AND%20_yz_rk:", erlang:binary_to_list(Key) ]),
    Result = ibrowse:send_req(URL, [], get),

    case Enabled of
        true ->
            ?assertMatch({ok, "200", _, _}, Result);
        false ->
            ?assertMatch({ok, "403", _, _}, Result)
    end.

%% ===================================================================
%% Internal
%% ===================================================================

bin_buckets(0, Result) ->
    lists:sort(Result);
bin_buckets(Count, Result) ->
    bin_buckets((Count - 1), [bin_bucket(Count) | Result]).

bin_keys(0, Result) ->
    lists:sort(Result);
bin_keys(Count, Result) ->
    bin_keys((Count - 1), [bin_key(Count) | Result]).

bin_vals(0, Result) ->
    lists:sort(Result);
bin_vals(Count, Result) ->
    bin_vals((Count - 1), [bin_val(Count) | Result]).

load_data(PBConn, Bucket, [Bucket | Buckets]) ->
    Index = index_2i(),
    Load = fun({Num, Key, Val}) ->
        Obj1  = riakc_obj:new(Bucket, Key, Val),
        Meta1 = riakc_obj:get_update_metadata(Obj1),
        Meta2 = riakc_obj:set_secondary_index(Meta1, [{Index, [Num]}]),
        Obj2  = riakc_obj:update_metadata(Obj1, Meta2),
        ?assertEqual(ok, riakc_pb_socket:put(PBConn, Obj2))
    end,
    lists:foreach(Load, lists:zip3(test_nums(), test_keys(), test_vals())),
    load_data(PBConn, Bucket, Buckets);
load_data(PBConn, PopBucket, [Bucket | Buckets]) ->
    ?assertEqual(ok, riakc_pb_socket:put(PBConn,
        riakc_obj:new(Bucket, <<"test_key">>, <<"test_value">>))),
    load_data(PBConn, PopBucket, Buckets);
load_data(_, _, []) ->
    ok.

