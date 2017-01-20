%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_kv_ts_newtype_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_ts_newtype_orig).

delayed_new_type(BucketType) ->
    intercepted_new_type(BucketType, 10, "delayed").

really_delayed_new_type(BucketType) ->
    intercepted_new_type(BucketType, 100, "really_delayed").

intercepted_new_type(BucketType, Delay, DelayType) ->
    DelayTypeFormat = DelayType ++ "~n",
    io:format(DelayTypeFormat),
    ?I_INFO(DelayTypeFormat),
    timer:sleep(Delay),
    ?M:new_type_orig(BucketType).
