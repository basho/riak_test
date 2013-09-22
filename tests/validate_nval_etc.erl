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

-module(validate_nval_etc).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"test">>).
-define(DEFAULT_PROPS, [{allow_mult,false},
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
                        {young_vclock,20}]).

-define(INVALID_BPROPS, [{allow_mult, elephant},
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
                        {young_vclock, "12"}]).

-define(VALID_BPROPS, [{allow_mult, true},
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
                        {young_vclock, 18}]).

confirm() ->
    [N1] = rt:build_cluster(1),

    C = rt:httpc(N1),

    %% Check we are starting in a default state
    verify_props(C, ?DEFAULT_PROPS),

    %% update bucket properties and check that you failed
    Result = rhc:set_bucket(C, ?BUCKET, ?INVALID_BPROPS),

    ?assertMatch({error, _}, Result),
    {error, {ok, "400", _H, Errors0}} = Result,
    {struct, Errors} = mochijson2:decode(Errors0),
    verify_errors(Errors),

    %% Verify no props were harmed in the making of this request
    verify_props(C, ?DEFAULT_PROPS),

    ok = rhc:set_bucket(C, ?BUCKET, ?VALID_BPROPS),

    verify_props(C, ?VALID_BPROPS),

    pass.

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

verify_props(C, Expected) ->
    %% Check that the errors mean no values were set
    {ok, Props} = rhc:get_bucket(C, ?BUCKET),
    ?assert(sets:is_subset(sets:from_list(Expected), sets:from_list(Props))).

