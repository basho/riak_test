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
%% @doc A module to test riak_ts basic create bucket/put/select cycle,
%%      with a node down.

-module(ts_C_select_pass_1).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
	 confirm/0
	]).

confirm() ->
    DDL = ts_util:get_ddl(docs),
    Data = ts_util:get_valid_select_data(),
    Qry = ts_util:get_valid_qry(),
    Expected = {
        ts_util:get_cols(docs),
        ts_util:exclusive_result_from_data(Data, 2, 9)},
    {[_Node|Rest], Conn} = ClusterConn = ts_util:cluster_and_connect(multiple),
    Got = ts_util:ts_query(ClusterConn, normal, DDL, Data, Qry),
    ?assertEqual(Expected, Got),
    % Stop Node 2 after bucket type has been activated
    rt:stop(hd(Rest)),
    Got1 = ts_util:single_query(Conn, Qry),
    ?assertEqual(Expected, Got1),
    pass.
