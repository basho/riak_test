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

-module(ts_A_select_window_fn_simple).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    Table = ts_util:get_default_bucket(),
    DDL   = ts_util:get_ddl(),
    Data  = ts_util:get_valid_select_data(),
    lager:info("DDL being used is ~p~n", [DDL]),

    Cluster = ts_util:build_cluster(single),
    C = rt:pbc(hd(Cluster)),
    {[], []} = ts_util:single_query(C, DDL),

    lager:info("putting data ~p~n", [Data]),
    ok = riakc_ts:put(C, Table, Data),

    Where = " Where time > 1 and time < 10 and myfamily = 'family1' and myseries ='seriesX'",
    QQ = [
          {"select myfamily from GeoCheckin" ++ Where,
           {[<<"myfamily">>],
            [{<<"family1">>},
             {<<"family1">>},
             {<<"family1">>},
             {<<"family1">>},
             {<<"family1">>},
             {<<"family1">>},
             {<<"family1">>},
             {<<"family1">>}]}},
          {"select * from GeoCheckin" ++ Where,
           {ts_util:get_cols(),
            [list_to_tuple(R) || R <- valid_data(Data)]}},
          {"select count(weather) from GeoCheckin" ++ Where,
           {[<<"COUNT(weather)">>], [{8}]}},
          {"select avg(temperature) from GeoCheckin" ++ Where,
           {[<<"AVG(temperature)">>], [{avg_temp(Data)}]}},
          {"select max(temperature) from GeoCheckin" ++ Where,
           {[<<"MAX(temperature)">>], [{max_temp(Data)}]}}
         ],

    lists:foreach(
      fun({Q, E}) -> ?assertEqual(ts_util:single_query(C, Q), E) end,
      QQ),
    pass.


valid_data(Data) ->
    [R || R = [_, _, N, _, _] <- Data, N > 1, N < 10].
valid_temp(Data) ->
    [T || [_, _, _, _, T] <- valid_data(Data)].

avg_temp(Data) ->
    ValidTemps = valid_temp(Data),
    lists:sum(ValidTemps) / length(ValidTemps).

max_temp(Data) ->
    ValidTemps = valid_temp(Data),
    lists:max(ValidTemps).
