%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
%%% @copyright (C) 2013, Basho Technologies
%%% @doc
%%% riak_test for bucket validation
%%% @end

-module(bucket_props_validation).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Node] = rt:build_cluster(1),

    Connections = get_connections(Node),
    Buckets = {druuid:v4_str(), druuid:v4_str()},

    DefaultProps = default_props(),
    ValidProps = valid_props(),

    %% Check we are starting in a default state
    verify_props(Connections, Buckets, DefaultProps),

    %% Verify attempting to set invalid properties results in the
    %% expected errors
    verify_props_errors(set_props(Connections, Buckets, invalid_props())),

    %% Verify no props were harmed in the making of this request
    verify_props(Connections, Buckets, DefaultProps),

    %% Set valid properties and verify they are present when
    %% retreiving the bucket properties
    ?assertEqual({ok, ok}, set_props(Connections, Buckets, ValidProps)),
    verify_props(Connections, Buckets, ValidProps),

    close_connections(Connections),
    pass.

get_connections(Node) ->
    {rt:httpc(Node), rt:pbc(Node)}.

close_connections({_Http, PBC}) ->
    riakc_pb_socket:stop(PBC).

get_props({Http, PBC}, {HttpBucket, PbcBucket}) ->
    {ok, PbcProps} = riakc_pb_socket:get_bucket(PBC, PbcBucket),
    {ok, HttpProps} = rhc:get_bucket(Http, HttpBucket),
    {HttpProps, PbcProps}.

set_props({Http, PBC}, {HttpBucket, PbcBucket}, Props) ->
    HttpRes = rhc:set_bucket(Http, HttpBucket, Props),
    PbcRes = try riakc_pb_socket:set_bucket(PBC, PbcBucket, Props) of
                 NormalRes ->
                     NormalRes
             catch
                 Error:Reason ->
                     {Error, Reason}
             end,
    {HttpRes, PbcRes}.

verify_props(Connections, Buckets, Expected) ->
    {HttpProps, PbcProps} = get_props(Connections, Buckets),
    ?assert(sets:is_subset(sets:from_list(Expected), sets:from_list(HttpProps))),
    ?assert(sets:is_subset(sets:from_list(Expected), sets:from_list(PbcProps))).
verify_props_errors({HttpResult, PBCResult}) ->
    verify_errors(http_errors(HttpResult)),
    ?assertEqual({error, function_clause}, PBCResult).

http_errors(Result) ->
    ?assertMatch({error, _}, Result),
    {error, {ok, "400", _H, Errors0}} = Result,
    {struct, Errors} = mochijson2:decode(Errors0),
    Errors.

verify_errors(Errors) ->
    ?assertEqual(13, length(Errors)),
    [?assert(verify_error(binary_to_existing_atom(Prop, latin1),
                          binary_to_atom(Message, latin1))) || {Prop, Message} <- Errors].

verify_error(allow_mult, not_boolean) ->
    true;
verify_error(basic_quorum, not_boolean) ->
    true;
verify_error(last_write_wins, not_boolean) ->
    true;
verify_error(notfound_ok, not_boolean) ->
    true;
verify_error(big_vclock, not_integer) ->
    true;
verify_error(n_val, not_integer) ->
    true;
verify_error(old_vclock, not_integer) ->
    true;
verify_error(small_vclock, not_integer) ->
    true;
verify_error(young_vclock, not_integer) ->
    true;
verify_error(Quorum, not_valid_quorum) when Quorum =:= dw;
                                            Quorum =:= pw;
                                            Quorum =:= pr;
                                            Quorum =:= r;
                                            Quorum =:= rw;
                                            Quorum =:= w ->
    true;
verify_error(_, _) ->
    false.

default_props() ->
    [{allow_mult,false},
     {basic_quorum,false},
     {big_vclock,50},
     {chash_keyfun,{riak_core_util,chash_std_keyfun}},
     {dw,quorum},
     {last_write_wins,false},
     {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
     {n_val,3},
     {notfound_ok,true},
     {old_vclock,86400},
     {postcommit,[]},
     {pr,0},
     {precommit,[]},
     {pw,0},
     {r,quorum},
     {rw,quorum},
     {small_vclock,50},
     {w,quorum},
     {young_vclock,20}].

invalid_props() ->
    [{allow_mult, elephant},
     {basic_quorum,fasle},
     {big_vclock, 89.90},
     {dw, qurum},
     {last_write_wins, 90},
     {n_val,<<"3">>},
     {notfound_ok,<<"truish">>},
     {old_vclock, -9890},
     {pr, -90},
     {pw, "seventeen"},
     {r, <<"florum">>},
     {rw, -9090929288989898398.9090093923232},
     {small_vclock, wibble},
     {w, "a horse a horse my kingdom"},
     {young_vclock, "12"}].

valid_props() ->
    [{allow_mult, true},
     {basic_quorum, true},
     {big_vclock, 90},
     {dw, one},
     {last_write_wins, false},
     {n_val, 4},
     {notfound_ok, false},
     {old_vclock, 9090909},
     {pr, all},
     {pw, quorum},
     {r, 4},
     {rw, 1},
     {small_vclock, 22},
     {w, all},
     {young_vclock, 18}].
