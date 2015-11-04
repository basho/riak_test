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

-module(ts_api).

-behavior(riak_test).

-export([confirm/0]).

%---------------------------------------------------------------------
% Test full range_scan API
%---------------------------------------------------------------------

confirm() ->
    DDL  = ts_api_util:get_ddl(api),
    Data = ts_api_util:get_data(api),
    C    = ts_api_util:setup_cluster(single, normal, DDL, Data),

    confirm_GtOps(C),
    confirm_GtEqOps(C),
    confirm_LtOps(C),
    confirm_LtEqOps(C),
    confirm_EqOps(C),
    confirm_NeqOps(C).

%------------------------------------------------------------
% Test > Ops
%------------------------------------------------------------

confirm_GtOps(C) ->
    ts_api_util:confirm_Pass(C, {myint,   int,     '>', 1}),
    ts_api_util:confirm_Pass(C, {myfloat, float,   '>', 1.0}),
    ts_api_util:confirm_Error(C, {mybin,   varchar, '>', test2}),
    ts_api_util:confirm_Error(C, {mybool,  boolean, '>', true}).

%------------------------------------------------------------
% Test >= Ops
%------------------------------------------------------------

confirm_GtEqOps(C) ->
    ts_api_util:confirm_Pass(C, {myint,   int,     '>=', 1}),
    ts_api_util:confirm_Pass(C, {myfloat, float,   '>=', 1.0}),
    ts_api_util:confirm_Error(C, {mybin,   varchar, '>=', test2}),
    ts_api_util:confirm_Error(C, {mybool,  boolean, '>=', true}).

%------------------------------------------------------------
% Test < Ops
%------------------------------------------------------------

confirm_LtOps(C) ->
    ts_api_util:confirm_Pass(C, {myint,   int,     '<', 4}),
    ts_api_util:confirm_Pass(C, {myfloat, float,   '<', 4.0}),
    ts_api_util:confirm_Error(C, {mybin,   varchar, '<', test2}),
    ts_api_util:confirm_Error(C, {mybool,  boolean, '<', true}).

%------------------------------------------------------------
% Test <= Ops
%------------------------------------------------------------

confirm_LtEqOps(C) ->
    ts_api_util:confirm_Pass(C, {myint,   int,     '<=', 4}),
    ts_api_util:confirm_Pass(C, {myfloat, float,   '<=', 4.0}),
    ts_api_util:confirm_Error(C, {mybin,   varchar, '<=', test2}),
    ts_api_util:confirm_Error(C, {mybool,  boolean, '<=', true}).

%------------------------------------------------------------
% Test == Ops
%------------------------------------------------------------

confirm_EqOps(C) ->
    ts_api_util:confirm_Pass(C, {myint,   int,     '=', 2}),
    ts_api_util:confirm_Pass(C, {myfloat, float,   '=', 2.0}),
    ts_api_util:confirm_Pass(C, {mybin,   varchar, '=', test2}),
    ts_api_util:confirm_Pass(C, {mybool,  boolean, '=', true}).

%------------------------------------------------------------
% Test != Ops
%------------------------------------------------------------

confirm_NeqOps(C) ->
    ts_api_util:confirm_Pass(C, {myint,   int,     '!=', 2}),
    ts_api_util:confirm_Pass(C, {myfloat, float,   '!=', 2.0}),
    ts_api_util:confirm_Pass(C, {mybin,   varchar, '!=', test2}),
    ts_api_util:confirm_Pass(C, {mybool,  boolean, '!=', true}).

