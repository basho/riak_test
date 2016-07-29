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

-ifndef(job_enable_common_included).
-define(job_enable_common_included, true).

-define(ADVANCED_CONFIG_KEY,    'job_accept_class').
-define(CUTTLEFISH_PREFIX,      "async.enable").

-define(TOKEN_LIST_BUCKETS,     'list_buckets').
-define(TOKEN_LIST_KEYS,        'list_keys').
-define(TOKEN_MAP_REDUCE,       'map_reduce').
-define(TOKEN_SEC_INDEX,        'secondary_index').
-define(TOKEN_YZ_SEARCH,        'riak_search').

-define(ERRMSG_LIST_BUCKETS_DISABLED,
        <<"Operation 'list_buckets' is not enabled">>).
-define(ERRMSG_LIST_KEYS_DISABLED,
        <<"Operation 'list_keys' is not enabled">>).
-define(ERRMSG_MAP_REDUCE_DISABLED,
        <<"Operation 'map_reduce' is not enabled">>).
-define(ERRMSG_SEC_INDEX_DISABLED,
        <<"Operation 'secondary_index' is not enabled">>).
-define(ERRMSG_YZ_SEARCH_DISABLED,
        <<"Operation 'riak_search' is not enabled">>).

-endif. % job_enable_common_included
