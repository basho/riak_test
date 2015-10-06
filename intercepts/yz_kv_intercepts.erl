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
-module(yz_kv_intercepts).
-compile(export_all).

-include("intercept.hrl").

-define(M, yz_kv_orig).

handle_delete_operation(BProps, Obj, Docs, BKey, LP) ->
    Lookup = ets:lookup(intercepts_tab, del_put),
    case Lookup of
        [] -> original_delete_op(BProps, Obj, Docs, BKey, LP);
        _ ->
            case proplists:get_value(del_put, Lookup) of
                0 ->
                    error_logger:info_msg(
                      "Delete operation intercepted for BKey ~p", [BKey]),
                    ets:update_counter(intercepts_tab, del_put, 1),
                    [];
                _ ->
                    original_delete_op(BProps, Obj, Docs, BKey, LP)
            end
    end.

original_delete_op(BProps, Obj, Docs, BKey, LP) ->
    error_logger:info_msg(
      "Delete operation original for BKey ~p | ~p", [BKey, Docs]),
    ?M:delete_operation_orig(BProps, Obj, Docs, BKey, LP).
