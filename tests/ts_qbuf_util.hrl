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

-define(TABLE, "t1").
-define(TIMEBASE, (5*1000*1000)).
-define(LIFESPAN, 500).
-define(LIFESPAN_EXTRA, 510).
-define(WHERE_FILTER_A, <<"A00001">>).
-define(WHERE_FILTER_B, <<"B">>).
-define(ORDBY_COLS, ["a", "b", "c", "d", "e", undefined]).

%% error codes as defined in riak_kv_ts_svc.erl
-define(E_SUBMIT,                1001).
-define(E_SELECT_RESULT_TOO_BIG, 1022).
-define(E_QBUF_CREATE_ERROR,     1023).
-define(E_QBUF_LDB_ERROR,        1024).
-define(E_QUANTA_LIMIT,          1025).
-define(E_QBUF_INTERNAL_ERROR,   1027).
