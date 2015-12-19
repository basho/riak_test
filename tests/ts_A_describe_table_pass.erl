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

-module(ts_A_describe_table_pass).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    Cluster = ts_util:build_cluster(single),
    C = rt:pbc(hd(Cluster)),

    Table = ts_util:get_default_bucket(),
    ts_util:create_and_activate_bucket_type(
      Cluster, ts_util:get_ddl(), Table),

    Got = riakc_ts:query(C, "DESCRIBE " ++ ts_util:get_default_bucket()),
    Expected =
        {[<<"Column">>,<<"Type">>,<<"Is Null">>,<<"Primary Key">>, <<"Local Key">>],
         [{<<"myfamily">>,<<"varchar">>,   false,  1,  1},
          {<<"myseries">>,<<"varchar">>,   false,  2,  2},
          {<<"time">>,    <<"timestamp">>, false,  3,  3},
          {<<"weather">>, <<"varchar">>,   false, [], []},
          {<<"temperature">>,<<"double">>, true,  [], []}]},
    ?assertEqual(Expected, Got),
    pass.
