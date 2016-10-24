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

-define(FAIL_TESTS, [
                {0,
                  {">", "2016-08-02 10:19"},
                  {"<", "2016-08-02 10:20"}
                }
              ]).



confirm() ->
    DDL = ts_util:get_ddl(),
    Start = jam:to_epoch(jam:compile(jam_iso8601:parse(?LOWER)), 3),
    End = jam:to_epoch(jam:compile(jam_iso8601:parse(?UPPER)), 3),
    AllData = ts_util:get_valid_select_data(fun() -> lists:seq(Start, End, 1000) end),

    {Cluster, Conn} = ts_util:cluster_and_connect(single),
    Bucket = ts_util:get_default_bucket(),
    ts_util:create_table(normal, Cluster, DDL, Bucket),
    riakc_ts:put(Conn, Bucket, AllData),

    QryFmt =
        "SELECT * FROM GeoCheckin "
        "WHERE time ~s '~s' and time ~s '~s' "
        "AND myfamily = 'family1' "
        "AND myseries ='seriesX' ",

    lists:foreach(
      fun({Tally, {Op1, String1}, {Op2, String2}}) ->
              Qry = lists:flatten(
                      io_lib:format(QryFmt, [Op1, String1,
                                             Op2, String2])),

              {ok, {_Cols, Data}} = ts_util:single_query(Conn, Qry),

              ?assertEqual(Tally, length(Data))
      end, ?PASS_TESTS),

    lists:foreach(
      fun({_Tally, {Op1, String1}, {Op2, String2}}) ->
              Qry = lists:flatten(
                      io_lib:format(QryFmt, [Op1, String1,
                                             Op2, String2])),

              RetMsg = ts_util:single_query(Conn, Qry),
              %% Assert that RetMsg returns a tuple with error in first place {error, {}}
              ?assertMatch({error, {_ErrCode, _ErrMsg}}, RetMsg)
      end, ?FAIL_TESTS),

    pass.
