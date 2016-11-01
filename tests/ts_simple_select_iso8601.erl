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

-module(ts_simple_select_iso8601).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

%% We'll populate the table with one record per second between the
%% lower and upper bounds
-define(LOWER, "2016-08-02 10:15:00").
-define(UPPER, "2016-08-02 10:45:00").

-define(PASS_TESTS, [
                %% Test format, illustrated:

                %% We expect 9 seconds between 10:19:50 and 10:20:00,
                %% exclusive on both ends.
                {9,
                 {">", "2016-08-02 10:19:50"},
                 {"<", "2016-08-02 10:20"}
                },

                {60,
                 {">=", "2016-08-02 10:19"},
                 {"<", "2016-08-02 10:20"}
                },

                %% Two full minutes, from 10:19:00 to 10:20:59
                %% inclusive
                {120,
                 {">=", "2016-08-02 10:19"},
                 {"<=", "2016-08-02 10:20"}
                },

                {61,
                 {">=", "2016-08-02 10:19"},
                 {"<=", "2016-08-02 10:20.01"}
                },

                {61,
                 {">=", "2016-08-02 10:19"},
                 {"<", "2016-08-02 10:20:01"}
                },

                {59,
                 {">", "2016-08-02 10:19.0"},
                 {"<", "2016-08-02 10:20"}
                },

                {0,
                 {">", "2016-08-02 10:19.99"},
                 {"<", "2016-08-02 10:20"}
                }
               ]).
%% expected error code is the first entry
-define(FAIL_TESTS, [
                {
                  1001,
                  {">", "2016-08-02 10:19"},
                  {"<", "2016-08-02 10:20"}
                }
              ]).


confirm() ->
    Table = ts_data:get_default_bucket(),
    DDL = ts_data:get_ddl(),
    Start = jam:to_epoch(jam:compile(jam_iso8601:parse(?LOWER)), 3),
    End = jam:to_epoch(jam:compile(jam_iso8601:parse(?UPPER)), 3),
    AllData = ts_data:get_valid_select_data(fun() -> lists:seq(Start, End, 1000) end),
    QryFmt =
        "SELECT * FROM GeoCheckin "
        "WHERE time ~s '~s' and time ~s '~s' "
        "AND myfamily = 'family1' "
        "AND myseries ='seriesX' ",

    Cluster = ts_setup:start_cluster(1),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),
    ts_ops:put(Cluster, Table, AllData),

    DDL = ts_data:get_ddl(),

    lists:foreach(
        fun({Tally, {Op1, String1}, {Op2, String2}}) ->
              Qry = ts_data:flat_format(QryFmt, [Op1, String1,
                                        Op2, String2]),

              {ok, {_Cols, Data}} = ts_ops:query(Cluster, Qry),

              ?assertEqual(Tally, length(Data))
        end, ?PASS_TESTS),

    lists:foreach(
        fun({ErrCode, {Op1, String1}, {Op2, String2}}) ->
              Qry = ts_data:flat_format(QryFmt, [Op1, String1,
                                        Op2, String2]),

              RetMsg = ts_ops:query(Cluster, Qry),
              ?assertMatch({error, {ErrCode, _}}, RetMsg)
        end, ?FAIL_TESTS),

    pass.
