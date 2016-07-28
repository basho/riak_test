%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2016 Basho Technologies, Inc.
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

%% Verify functionality of async job enable/disable flags in riak.conf.
-module(verify_job_enable_rc).

-behavior(riak_test).
-export([confirm/0]).

% exports for use by other tests
-export([
    close_client/1,
    listkeys_bucket/0,
    open_client/2,
    setup_cluster/1,
    test_buckets/0,
    test_keys/0,
    test_operation/4,
    undefined_bucket/0
]).

-include_lib("eunit/include/eunit.hrl").

-define(NUM_BUCKETS,        1200).
-define(NUM_KEYS,           1000).
-define(LISTKEYS_BUCKET,    <<"listkeys_bucket">>).
-define(UNDEFINED_BUCKET,   <<"880bf69d-5dab-44ee-8762-d24c6f759ce1">>).

%% ===================================================================
%% Test API
%% ===================================================================

confirm() ->
    Config1 = {cuttlefish, [
        {"async.enable.list_buckets",   "on"},
        {"async.enable.list_keys",      "on"}
    ]},
    Config2 = {cuttlefish, [
        {"async.enable.list_buckets",   "off"},
        {"async.enable.list_keys",      "off"}
    ]},
    Config3 = {cuttlefish, [
        {"async.enable.list_buckets",   "on"},
        {"async.enable.list_keys",      "off"}
    ]},
    Config4 = {cuttlefish, [
        {"async.enable.list_buckets",   "off"},
        {"async.enable.list_keys",      "on"}
    ]},
    Configs = [Config1, Config2, Config3, Config4],

    lager:info("Deploying ~b nodes ...", [erlang:length(Configs)]),
    Nodes = rt:deploy_nodes(Configs),

    setup_cluster(Nodes),

    Tests = [
        {list_buckets,  [true, false, true, false]},
        {list_keys,     [true, false, false, true]}
    ],
    [run_test(Op, Enabled, Nodes) || {Op, Enabled} <- Tests].

run_test(Operation, [Switch | Switches], [Node | Nodes]) ->
    Enabled = enabled_string(Switch),
    [begin
        lager:info("Tesing ~s ~s ~s on ~p", [Operation, Type, Enabled, Node]),
        test_operation(Node, Operation, Switch, Type)
     end || Type <- [pbc, http] ],
    run_test(Operation, Switches, Nodes);
run_test(_, [], []) ->
    ok.

%% ===================================================================
%% Private API
%% ===================================================================

listkeys_bucket() ->
    ?LISTKEYS_BUCKET.

undefined_bucket() ->
    ?UNDEFINED_BUCKET.

test_buckets() ->
    Key = {?MODULE, test_buckets},
    case erlang:get(Key) of
        undefined ->
            New = binval_list(?NUM_BUCKETS, "Bucket", []),
            erlang:put(Key, New),
            New;
        Val ->
            Val
    end.

test_keys() ->
    Key = {?MODULE, test_keys},
    case erlang:get(Key) of
        undefined ->
            New = binval_list(?NUM_KEYS, "Key", []),
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

    lager:info("Writing some known data to the clusetr ..."),
    PB = rt:pbc(Node),
    lists:foreach(fun(Key) ->
        riakc_pb_socket:put(PB,
            riakc_obj:new(?LISTKEYS_BUCKET, Key, Key))
    end, test_keys()),
    lists:foreach(fun(Bucket) ->
        riakc_pb_socket:put(PB,
            riakc_obj:new(Bucket, <<"test_key">>, <<"test_value">>))
    end, test_buckets()),
    riakc_pb_socket:stop(PB),
    ok.

test_operation(Node, 'list_buckets', Enabled, ClientType) ->
    {_, Mod, Conn} = Client = open_client(ClientType, Node),
    {Status, Result} = Mod:list_buckets(Conn),
    close_client(Client),
    case Enabled of
        true ->
            ?assertEqual(ok, Status),
            ?assertEqual(test_buckets(), lists:sort(Result));
        false ->
            Expect = <<"Operation 'list_buckets' is not enabled">>,
            ?assertEqual(error, Status),
            ?assertEqual(Expect, Result)
%%            case ClientType of
%%                pbc ->
%%                    ?assertEqual(Expect, Result);
%%                http ->
%%                    ?assertEqual({"403", Expect}, Result)
%%            end
    end;

test_operation(Node, 'list_keys', Enabled, ClientType) ->
    {_, Mod, Conn} = Client = open_client(ClientType, Node),
    {Status, Result} = Mod:list_keys(Conn, ?LISTKEYS_BUCKET),
    close_client(Client),
    case Enabled of
        true ->
            ?assertEqual(ok, Status),
            ?assertEqual(test_keys(), lists:sort(Result));
        false ->
            Expect = <<"Operation 'list_keys' is not enabled">>,
            ?assertEqual(error, Status),
            ?assertEqual(Expect, Result)
    end.

%% ===================================================================
%% Internal
%% ===================================================================

binval_list(0, _, Result) ->
    Result;
binval_list(Count, Prefix, Result) ->
    binval_list((Count - 1), Prefix, [
        erlang:list_to_binary([Prefix, erlang:integer_to_list(Count)])
        | Result]).

enabled_string(true) ->
    "enabled";
enabled_string(false) ->
    "disabled".


