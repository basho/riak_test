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

-module(riak_kv_index_hashtree_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_index_hashtree_orig).

%% @doc Perform a delayed compare, which delays the receipt of a
%%      message.
delayed_compare(_IndexN, _Remote, _AccFun, _TreePid) ->
    timer:sleep(1000000),
    [].

%% @doc When attempting to get the lock on a hashtree, return the
%%      not_built atom which means the tree has not been computed yet.
not_built(_TreePid, _Type, _Version) ->
    not_built.

%% @doc When attempting to get the lock on a hashtree, return the
%%      already_locked atom which means the tree is locked by another
%%      process.
already_locked(_TreePid, _Type, _Version) ->
    already_locked.

%% @doc When attempting to get the lock on a hashtree, return the
%%      bad_version atom which means the local tree does not match
%%      the requested version
bad_version(_TreePid, _Type, _Version) ->
    bad_version.