%% -------------------------------------------------------------------
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
-module(verify_conditionalput_api).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include("../src/stacktrace.hrl").

-define(DEFAULT_RING_SIZE, 16).
-define(UPDATE_KEY, <<"key">>).
-define(FRESH_KEY, <<"new_key">>).
-define(FRESHER_KEY, <<"another_key">>).

-define(CONF,
        [{riak_kv,
          [
            {anti_entropy, {off, []}},
            {delete_mode, keep},
            {tictacaae_active, active},
            {tictacaae_parallelstore, leveled_ko},
            {tictacaae_storeheads, true},
            {tictacaae_rebuildtick, 3600000}, % don't tick for an hour!
            {tictacaae_suspend, true}
          ]},
         {riak_core,
          [
            {ring_creation_size, ?DEFAULT_RING_SIZE},
            {default_bucket_props, [{allow_mult, true}]}
          ]}]
       ).

%% You should have curl installed locally to do this.
confirm() ->

    [[CurrentNode], [PreviousNode]] =
        rt:build_clusters([{1, current, ?CONF}, {1, previous, ?CONF}]),
    rt:wait_for_service(CurrentNode, riak_kv),

    RPCc = rt:pbc(CurrentNode),
    ok = test_api_consistency(RPCc, riakc_pb_socket, <<"bucketPB">>, current),

    RHCc = rt:httpc(CurrentNode),
    ok = test_api_consistency(RHCc, rhc, <<"bucketHTTP">>, current),

    rt:wait_for_service(PreviousNode, riak_kv),

    RPCp = rt:pbc(PreviousNode),
    ok = test_api_consistency(RPCp, riakc_pb_socket, <<"bucketPB">>, previous),

    RHCp = rt:httpc(PreviousNode),
    ok = test_api_consistency(RHCp, rhc, <<"bucketHTTP">>, previous),

    pass. 


test_api_consistency(Client, ClientMod, Bucket, Version) ->
    lager:info("------------------------------"),
    lager:info(
        "Testing consistency on ~w version with ~w and Bucket ~s",
        [Version, ClientMod, Bucket]),
    lager:info("------------------------------"),
    
    lager:info("Simple PUT"),
    ok =
        ClientMod:put(
            Client,
            riakc_obj:new(Bucket, ?UPDATE_KEY, <<"value1">>)),
    
    lager:info("Fetch the value, and then delete with clock"),
    {ok, Obj1} =
        ClientMod:get(Client, Bucket, ?UPDATE_KEY),
    true = riakc_obj:get_value(Obj1) == <<"value1">>,
    VC1 = riakc_obj:vclock(Obj1),
    ok = delete_vclock(ClientMod, Client, Bucket, ?UPDATE_KEY, VC1),

    lager:info("Object not there"),
    {error, notfound} =
        ClientMod:get(Client, Bucket, ?UPDATE_KEY),
    
    lager:info("Reap the tombstone (delete_mode = keep)"),
    {ok, 1} = ClientMod:aae_reap_tombs(Client, Bucket, all, all, all, local),
    rt:wait_until(
        fun() -> 
            [] == log_tombs(ClientMod, Client, Bucket)
        end
    ),
    
    lager:info("Put a fresh value on now reaped key"),
    ok =
        ClientMod:put(
            Client,
            riakc_obj:new(Bucket, ?UPDATE_KEY, <<"value2">>)),
    
    {ok, Obj2} =
        ClientMod:get(Client, Bucket, ?UPDATE_KEY),
    true = riakc_obj:get_value(Obj2) == <<"value2">>,
    VC2 = riakc_obj:vclock(Obj2),

    lager:info("Fail attempt to PUT conditional on no object existing"),
    ErrorMatchFound =
        ClientMod:put(
            Client,
            riakc_obj:new(Bucket,?UPDATE_KEY, <<"value3">>),
            [if_none_match]),
    check_match_found(ClientMod, ErrorMatchFound),
    
    lager:info("Success attempt to over-write without condition"),
    Obj4N = riakc_obj:new(Bucket, ?UPDATE_KEY, <<"value4">>),
    Obj4P = riakc_obj:set_vclock(Obj4N, VC2),
    ok = ClientMod:put(Client, Obj4P),
    {ok, Obj4} =
        ClientMod:get(Client, Bucket, ?UPDATE_KEY),
    true = riakc_obj:get_value(Obj4) == <<"value4">>,

    lager:info("Success attempt putting to different key with condition"),
    ok = 
        ClientMod:put(
            Client,
            riakc_obj:new(Bucket, ?FRESH_KEY, <<"value5">>),
            [if_none_match]),
    {ok, Obj5} =
        ClientMod:get(Client, Bucket, ?FRESH_KEY),
    true = riakc_obj:get_value(Obj5) == <<"value5">>,

    lager:info(
        "Update fails as clock/tag does not match object and condition set"),
    Obj6 = riakc_obj:update_value(Obj2, <<"value6">>),
    MatchError1 = update_match(ClientMod, Client, Obj6),
    check_match_conflict(ClientMod, MatchError1),

    lager:info(
        "Update succeeds as clock/tag does match object with condition set"),
    Obj7 = riakc_obj:update_value(Obj4, <<"value7">>),
    ok = update_match(ClientMod, Client, Obj7),
    {ok, Obj8} =
        ClientMod:get(Client, Bucket, ?UPDATE_KEY),
    true = riakc_obj:get_value(Obj8) == <<"value7">>,

    lager:info("Carelessly create siblings"),
    ok = 
        ClientMod:put(
            Client,
            riakc_obj:new(Bucket, ?UPDATE_KEY, <<"value8">>)),
    {ok, Obj9} =
        ClientMod:get(Client, Bucket, ?UPDATE_KEY),
    [<<"value7">>, <<"value8">>] = lists:sort(riakc_obj:get_values(Obj9)),

    lager:info(
        "Update siblings fail as clock does not match object"),
    ObjA = riakc_obj:update_value(Obj4, <<"valueA">>),
    MatchError2 = update_match(ClientMod, Client, ObjA),
    check_match_conflict(ClientMod, MatchError2),

    lager:info(
        "Update siblings succeed as clock does match object"),
    ObjB = riakc_obj:new(Bucket, ?UPDATE_KEY, <<"valueB">>),
    ok =
        update_match(
            ClientMod,
            Client,
            riakc_obj:set_vclock(ObjB, riakc_obj:vclock(Obj9))),
    {ok, ObjC} =
        ClientMod:get(Client, Bucket, ?UPDATE_KEY),
    true = riakc_obj:get_value(ObjC) == <<"valueB">>,

    ok = 
        case {ClientMod, Version} of
            {rhc, current} ->
                extra_http_notmodified_test(Client, Bucket, ObjC);
            _ ->
                ok
        end,

    ok.

extra_http_notmodified_test(Client, Bucket, Obj) ->
    lager:info("Http only test - using if_not_modified"),
    Obj1 = riakc_obj:update_value(Obj, <<"modified1">>),
    ok = rhc:put(Client, Obj1, [if_not_modified]),
    {ok, _Obj2} = rhc:get(Client, Bucket, ?UPDATE_KEY),

    lager:info("Http only test - generate sibling again"),
    ok = rhc:put(Client, riakc_obj:update_value(Obj1, <<"modified2">>)),
    {ok, Obj3} = rhc:get(Client, Bucket, ?UPDATE_KEY),
    [<<"modified1">>, <<"modified2">>] =
        lists:sort(riakc_obj:get_values(Obj3)),

    lager:info("Resolve siblings checking if_not_modified"),
    Obj4 = riakc_obj:new(Bucket, ?UPDATE_KEY, <<"modified3">>),
    ok =
        rhc:put(
            Client,
            riakc_obj:set_vclock(Obj4, riakc_obj:vclock(Obj3)),
            [if_not_modified]),
    {ok, Obj5} = rhc:get(Client, Bucket, ?UPDATE_KEY),
    true = riakc_obj:get_value(Obj5) == <<"modified3">>,

    lager:info("Fail to update due to if_not_modified"),
    {error, {ok, "409", _Headers5, _Body5}} =
        rhc:put(Client, Obj1, [if_not_modified]),
    
    lager:info("Succeed to update by correcting vector clock if_not_modified"),
    ok =
        rhc:put(
            Client,
            riakc_obj:set_vclock(Obj4, riakc_obj:vclock(Obj5)),
            [if_not_modified]),
    {ok, Obj6} = rhc:get(Client, Bucket, ?UPDATE_KEY),
    true = riakc_obj:get_value(Obj6) == <<"modified3">>,

    lager:info("Succeed again in creating siblings"),
    ok = rhc:put(Client, Obj1),
    {ok, Obj7} = rhc:get(Client, Bucket, ?UPDATE_KEY),
    [<<"modified1">>, <<"modified3">>] =
        lists:sort(riakc_obj:get_values(Obj7)),

    lager:info("Succeed in resolving siblings with if_not_modified"),
    ok =
        rhc:put(
            Client,
            riakc_obj:set_vclock(Obj1, riakc_obj:vclock(Obj7)),
            [if_not_modified]),
    {ok, Obj8} = rhc:get(Client, Bucket, ?UPDATE_KEY),
    true = riakc_obj:get_value(Obj8) == <<"modified1">>,
    
    lager:info("Fail to update again blocked by if_not_modified"),
    {error, {ok, "409", _Headers9, _Body9}} =
        rhc:put(
            Client,
            riakc_obj:set_vclock(Obj1, riakc_obj:vclock(Obj7)),
            [if_not_modified]),
    
    lager:info("Fail to create new object with clock if_not_modified"),
    ObjA =
        riakc_obj:set_vclock(
            riakc_obj:new(Bucket, ?FRESHER_KEY, <<"modifiedA">>),
            riakc_obj:vclock(Obj7)),
    {error, {ok, "409", _HeadersA, _BodyA}} =
        rhc:put(Client, ObjA, [if_not_modified]),

    ok.


%% The client API is inconsistent for deleting with a vclock
delete_vclock(riakc_pb_socket, Client, Bucket, Key, Clock) ->
    riakc_pb_socket:delete_vclock(Client, Bucket, Key, Clock);
delete_vclock(rhc, Client, Bucket, Key, Clock) ->
    rhc:delete(Client, Bucket, Key, [{vclock, Clock}]).

% The client API is inconsistent in presenting failure of match pre-condition
% The HTTP API resturns the 412 Status code (Precondition Failed), whereas PB
% API simply states that "match_found"
check_match_found(riakc_pb_socket, ErrorMatchFound) ->
    {error, Response} = ErrorMatchFound,
    ?assertMatch(<<"match_found">>, Response);
check_match_found(rhc, ErrorMatchFound) ->
    {error, {ok, StatusCode, _Headers, _Message}} = ErrorMatchFound,
    ?assertMatch("412", StatusCode).

update_match(riakc_pb_socket, Client, Object) ->
    riakc_pb_socket:put(Client, Object, [if_not_modified]);
update_match(rhc, Client, Object) ->
    rhc:put(Client, Object, [if_match]).

check_match_conflict(riakc_pb_socket, MatchError) ->
    {error, Response} = MatchError,
    ?assertMatch(<<"modified">>, Response);
check_match_conflict(rhc, MatchError) ->
    {error, {ok, StatusCode, _Headers, _Message}} = MatchError,
    ?assertMatch("412", StatusCode).


log_tombs(ClientMod, Client, Bucket) ->
    {ok, {keys, L}} = ClientMod:aae_find_tombs(Client, Bucket, all, all, all),
    lager:info("Found ~w tombs", [length(L)]),
    L.
