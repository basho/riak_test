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
%% @doc Verify MapReduce returns the right kind of errors.
-module(mapred_http_errors).
-behavior(riak_test).
-export([
         %% riak_test api
         confirm/0,

         %% used on riak node
         map_never_notfound/3
        ]).
-compile([export_all]). %% because we call ?MODULE:TestName
-include_lib("eunit/include/eunit.hrl").

%% @doc this map function always bails with a function clause error on
%% notfound
map_never_notfound(Object, _, _) when Object /= {error, notfound} ->
    [ok].

confirm() ->
    Nodes = rt_cluster:build_cluster(1),

    rt:load_modules_on_nodes([?MODULE], Nodes),
    
    [ begin
          lager:info("Running test ~p", [T]),
          ?MODULE:T(Nodes)
      end
      || T <- [proc_fun_clause,
               proc_fun_clause_chunked] ],

    pass.

httpmr(Node, Inputs, Query) ->
    ibrowse:send_req(rt:http_url(Node)++"/mapred",
                     [{"content-type", "application/json"}],
                     post,
                     rhc_mapred:encode_mapred(Inputs, Query),
                     [{response_format, binary}]).

httpmr_chunked(Node, Inputs, Query) ->
    ibrowse:send_req(rt:http_url(Node)++"/mapred?chunked=true",
                     [{"content-type", "application/json"}],
                     post,
                     rhc_mapred:encode_mapred(Inputs, Query),
                     [{response_format, binary}]).

%% @doc test that a simple variety of processing error returns useful
%% JSON details about it
proc_fun_clause([Node|_]) ->
    {ok, Code, Headers, Body} =
        httpmr(Node,
               [{<<"doesnot">>,<<"exist">>}],
               [{map, {modfun, ?MODULE, map_never_notfound}, none, true}]),
    ?assertEqual("500", Code),
    ?assertEqual("application/json",
                 proplists:get_value("Content-Type", Headers)),
    assert_proc_fun_clause_json(Body).

proc_fun_clause_chunked([Node|_]) ->
    {ok, Code, Headers, Body} =
        httpmr_chunked(
          Node,
          [{<<"doesnot">>,<<"exist">>}],
          [{map, {modfun, ?MODULE, map_never_notfound}, none, true}]),
    ?assertEqual("200", Code),
    ?assertMatch("multipart/mixed"++_,
                 proplists:get_value("Content-Type", Headers)),
    assert_proc_fun_clause_json(Body).

assert_proc_fun_clause_json(Body) ->
    {struct, Json} = mochijson2:decode(Body),
    ?assertEqual(0, proplists:get_value(<<"phase">>, Json)),
    ?assertEqual(<<"function_clause">>,
                 proplists:get_value(<<"error">>, Json)),
    ?assertEqual(
       <<"{{error,notfound},{<<\"doesnot\">>,<<\"exist\">>},undefined}">>,
       proplists:get_value(<<"input">>, Json)),
    ?assert(proplists:is_defined(<<"stack">>, Json)).
