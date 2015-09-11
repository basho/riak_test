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
%%-------------------------------------------------------------------

-module(riak_kv_bitcask_backend_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_bitcask_backend_orig).

corrupting_put(Bucket, Key, IndexSpecs, Val0, ModState) ->
    Val = 
        case random:uniform(20) of 
            10 -> 
                corrupt_binary(Val0);
            _ -> Val0
        end,
    ?M:put_orig(Bucket, Key, IndexSpecs, Val, ModState).

corrupting_get(Bucket, Key, ModState) ->
    case ?M:get_orig(Bucket, Key, ModState) of
        {ok, BinVal0, UpdModState} ->
            BinVal =
                case random:uniform(20) of 
                    10 ->
                        corrupt_binary(BinVal0);
                    _ -> BinVal0
                end,
            {ok, BinVal, UpdModState};
        Else -> Else
    end.
            
corrupt_binary(O) ->
    crypto:rand_bytes(byte_size(O)).
