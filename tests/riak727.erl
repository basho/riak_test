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
%% -------------------------------------------------------------------
%%% @doc
%%% riak_test for github issue riak 727.
%%%
%%% Running a riak-2.0.x node with a default 1.4.x app.config causes
%%% the default bucket properties for allow_mult=true and
%%% dvv_enabled=true when they should be false.
%%%
%%% @end

-module(riak727).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).
-define(TYPE1, <<"bob">>).
-define(TYPE2, <<"fred">>).
-define(DEF_APP_CONF, "1.4-default-app.config").
-define(APP_CONF, "app.config").

confirm() ->
    [Node] = rt:build_cluster([current]),

    verify_default_bucket_props(Node, ?TYPE1),
    add_one_four_config(Node),
    verify_default_bucket_props(Node, ?TYPE2),

    pass.

%% @private check that the default bucket props for both <<"default">>
%% typed, and custome typed buckets are as expected.
-spec verify_default_bucket_props(node(), binary()) -> ok | no_return().
verify_default_bucket_props(Node, Type) ->
    rt:create_and_activate_bucket_type(Node, Type, [{nonsense, <<"value">>}]),

    DefProps = get_props(Node, <<"default">>),
    TypeProps = get_props(Node, Type),

    lager:info("~p", [DefProps]),

    ?assertEqual(false, proplists:get_value(allow_mult, DefProps)),
    ?assertEqual(false, proplists:get_value(dvv_enabled, DefProps)),
    ?assertEqual(true, proplists:get_value(allow_mult, TypeProps)),
    ?assertEqual(true, proplists:get_value(dvv_enabled, TypeProps)).

%% @private copy the default 1.4 app.config file to the node under
%% test.
-spec add_one_four_config(node()) -> ok.
add_one_four_config(Node) ->
    rt:stop_and_wait(Node),
    ok = copy(app_config_file(), node_etc_dir(Node)),
    rt:start_and_wait(Node).

-spec copy(file:filename(), file:filename()) -> ok.
copy(File, DestDir) ->
    lager:info("Copying ~p to ~p~n", [File, DestDir]),
    {ok, _} = file:copy(File, DestDir),
    ok.

-spec app_config_file() -> file:filename().
app_config_file() ->
    filename:join(rt:priv_dir(), ?DEF_APP_CONF).

%% @private get the etc config directory for the given node.
-spec node_etc_dir(node()) -> file:filename().
node_etc_dir(Node) ->
    ConfPath = ?HARNESS:get_riak_conf(Node),
    filename:join(filename:dirname(ConfPath), ?APP_CONF).

-spec get_props(node(), binary()) -> proplists:proplist() | no_return().
get_props(Node, Type) ->
    case  rpc:call(Node, riak_core_bucket_type, get, [Type]) of
        {badrpc, Reason} ->
            throw({badrpc, Reason});
        Props ->
            Props
    end.
