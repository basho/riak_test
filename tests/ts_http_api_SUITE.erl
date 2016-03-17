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
%% Tests for range queries around the boundaries of quanta.
%%
%% -------------------------------------------------------------------
-module(ts_http_api_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    [_Node|_] = Cluster = ts_util:build_cluster(single),
    [{cluster, Cluster} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [].

all() ->
    [ create_table_test,
      create_bad_table_test
    ].


%% column_names_def_1() ->
%%     [<<"a">>, <<"b">>, <<"c">>].


table_def_bob() ->
    "create table bob (" ++
        "a varchar not null," ++
        " b varchar not null," ++
        " c timestamp not null," ++
        " d sint64," ++
        " primary key ((a, b, quantum(c, 1, m)), a, b, c))".

bad_table_def() ->
    "create table pap ("++
        "a timestamp not null," ++
        "b timestamp not null," ++
        "c timestamp not null)".

%% client_pid(Ctx) ->
%%     [Node|_] = proplists:get_value(cluster, Ctx),
%%     rt:pbc(Node).

%%%
%%% HTTP API tests
%%%

create_table_test(Cfg) ->
    Query = table_def_bob(),
    Node = get_node(Cfg),
    URL = query_url(Node, Query),
    ct:log("URL=~s", [URL]),
    {ok, "200", _Headers, Body } = ibrowse:send_req(URL, [], post),
    Body = success_body().



create_bad_table_test(Cfg) ->
    Query = bad_table_def(),
    Node = get_node(Cfg),
    URL = query_url(Node, Query),
    {ok, "400", _Headers, "bad query:"++_} = ibrowse:send_req(URL, [], post).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_node(Cfg) ->
    [Node|_] = ?config(cluster, Cfg),
    Node.

node_ip_and_port(Node) ->
    {ok, [{IP, Port}]} = rpc:call(Node, application, get_env, [riak_api, http]),
    {IP, Port}.

query_url(Node, Query) ->
     {IP, Port} = node_ip_and_port(Node),
     query_url(IP, Port, Query).

query_url(IP, Port, Query) ->
    EncodedQuery = ibrowse_lib:url_encode(Query),
    lists:flatten(
      io_lib:format("http://~s:~B/ts/v1/query?query=~s",
                    [IP, Port, EncodedQuery])).

success_body() ->
    "{\"success\":true}".
