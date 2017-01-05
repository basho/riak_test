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

-module(ts_cluster_create_table_via_sql_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {minutes, 10}}].

-define(CUSTOM_NVAL, 4).
-define(PROP_NO_QUOTES, <<"no quotes">>).
-define(PROP_WITH_QUOTES, <<"with ' quoted '' quotes''">>).
-define(CUSTOM_PROPERTIES,
        [{n_val, ?CUSTOM_NVAL},
         {no_quotes_prop, ?PROP_NO_QUOTES},
         {with_quotes_prop, ?PROP_WITH_QUOTES}]).

init_per_suite(Config) ->
    Cluster = ts_setup:start_cluster(3),
    [{cluster, Cluster} | Config].

end_per_suite(_Config) ->
    ok.

all() ->
    [create_test,
     re_create_fail_test,
     describe_test,
     get_put_data_test,
     create_fail_because_bad_properties_test,
     get_set_property_test].


create_fail_because_bad_properties_test(Ctx) ->
    C = client_pid(Ctx),
    BadDDL =
        io_lib:format(
          "~s WITH (m_val = ~s)",
          [ddl_common(), "plain_id"]),
    Got = riakc_ts:query(C, BadDDL),
    ?assertMatch({error, {1020, _}}, Got),
    pass.

create_test(Ctx) ->
    C = client_pid(Ctx),
    GoodDDL =
        io_lib:format(
          "~s WITH (n_val=~b, no_quotes_prop = '~s', with_quotes_prop = '~s')",
          [ddl_common(), ?CUSTOM_NVAL,
           enquote_varchar(?PROP_NO_QUOTES),
           enquote_varchar(?PROP_WITH_QUOTES)]),
    Got1 = riakc_ts:query(C, GoodDDL),
    ?assertEqual({ok, {[],[]}}, Got1),
    pass.

re_create_fail_test(Ctx) ->
    C = client_pid(Ctx),
    Got = riakc_ts:query(C, ddl_common()),
    ?assertMatch({error, {1014, _}}, Got),
    pass.

describe_test(Ctx) ->

    Qry = io_lib:format("DESCRIBE ~s", [ts_data:get_default_bucket()]),
    Cluster = ?config(cluster, Ctx),
    Got = ts_ops:query(Cluster, Qry),
    ?assertEqual(table_described(), Got),
    pass.

get_put_data_test(Ctx) ->
    C = client_pid(Ctx),
    Data = [{<<"a">>, <<"b">>, 10101010, <<"not bad">>, 42.24}],
    Key = [<<"a">>, <<"b">>, 10101010],
    ?assertMatch(ok, riakc_ts:put(C, ts_data:get_default_bucket(), Data)),
    Got = riakc_ts:get(C, ts_data:get_default_bucket(), Key, []),
    lager:info("get_put_data_test Got ~p", [Got]),
    ?assertMatch({ok, {_, Data}}, Got),
    pass.

get_set_property_test(Ctx) ->
    [Node1, Node2 | _] = ?config(cluster, Ctx),
    %% these are the custom properties we have explicitly set in
    %% CREATE WITH, filtered out of the default ones and sorted
    ExpectedPL = lists:usort(?CUSTOM_PROPERTIES),
    %% make sure bucket properties are the same retrieved from
    %% different nodes
    GetCustomBucketPropsF =
        fun(Node) ->
                ActualProps = lists:usort(get_bucket_props_from_node(Node)),
                %% only select our custom properties:
                [PV || {P, _} = PV <- ActualProps, lists:keymember(P, 1, ExpectedPL)]
        end,
    ?assertEqual(ExpectedPL, GetCustomBucketPropsF(Node1)),
    ?assertEqual(ExpectedPL, GetCustomBucketPropsF(Node2)),
    pass.


get_bucket_props_from_node(Node) ->
    rpc:call(
      Node, riak_core_claimant, get_bucket_type,
      [list_to_binary(ts_data:get_default_bucket()), undefined, false]).

client_pid(Ctx) ->
    Nodes = ?config(cluster, Ctx),
    rt:pbc(hd(Nodes)).

ddl_common() ->
    ts_data:get_ddl(small).

table_described() ->
    {ok,{[<<"Column">>,<<"Type">>,<<"Nullable">>,<<"Partition Key">>,<<"Local Key">>, <<"Interval">>,<<"Unit">>,<<"Sort Order">>],
         [{<<"myfamily">>,<<"varchar">>,false,1,1,[],[], []},
          {<<"myseries">>,<<"varchar">>,false,2,2,[],[], []},
          {<<"time">>,<<"timestamp">>,false,3,3,15, <<"m">>,[]},
          {<<"weather">>,<<"varchar">>,false,[],[],[], [],[]},
          {<<"temperature">>,<<"double">>,true,[],[],[], [],[]}]}}.

enquote_varchar(P) when is_binary(P) ->
    re:replace(P, "'", "''", [global, {return, binary}]);
enquote_varchar(P) ->
    P.
