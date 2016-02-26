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
%% @doc A module to test riak_ts basic create bucket/put/select cycle
%%      using http client (rhc_ts).

-module(ts_cluster_http).
-behavior(riak_test).
-export([confirm/0]).

%% inject characters to check transit of keys over http barrier
%% (involves urlendofing/decoding of field values)
-define(PVAL_P1, <<"ZXC1/1">>).
-define(PVAL_P2, <<"PDP -11">>).

confirm() ->
    ts_cluster_comprehensive:run_tests(?PVAL_P1, ?PVAL_P2, rhc_ts).
