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
%% @doc A module to test riak_ts basic create bucket/put/select cycle.

-module(ts_cluster_random_query_pass_eqc).
-compile(export_all).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    ClusterConn = ts_util:cluster_and_connect(multiple),
    ?assert(eqc:quickcheck(eqc:numtests(500, ?MODULE:prop_ts(ClusterConn)))),
    pass.

prop_ts(ClusterConn) ->
    ?FORALL({NVal, NPuts, Q, NSpans},
            {gen_n_val(), gen_no_of_puts(), gen_quantum(), gen_spans()},
            run_query(ClusterConn, NVal, NPuts, Q, NSpans)).

run_query(ClusterConn, NVal, NPuts, Q, NSpans) ->

    Bucket = "Bucket_" ++ timestamp(),
    lager:debug("Bucket is ~p~n", [Bucket]),

    DDL = get_ddl(Bucket, Q),
    lager:debug("DDL is ~p~n", [DDL]),

    Data = make_data(NPuts, Q, NSpans),

    Query = make_query(Bucket, Q, NSpans),

    {_Cluster, Conn} = ClusterConn,

    {ok, _} = ts_util:create_and_activate_bucket_type(ClusterConn, DDL, Bucket, NVal),
    ok = riakc_ts:put(Conn, Bucket, Data),
    {ok, {_, Got}} = ts_util:single_query(Conn, Query),

    ?assertEqual(Data, Got),

    true.

make_query(Bucket, Q, NSpans) ->
    Multi = get_multi(Q),
    Start = 1,
    End = Multi * NSpans,
    "select * from " ++ Bucket ++
        " Where time >= " ++ integer_to_list(Start) ++
        " and time <= "   ++ integer_to_list(End) ++
        " and myfamily = 'family1' and myseries ='seriesX'".

get_ddl(Bucket, {No, Q}) ->
    _SQL = "CREATE TABLE " ++ Bucket ++ " (" ++
        "myfamily    varchar   not null, " ++
        "myseries    varchar   not null, " ++
        "time        timestamp not null, " ++
        "weather     varchar   not null, " ++
        "temperature double, " ++
        "PRIMARY KEY ((myfamily, myseries, quantum(time, " ++ integer_to_list(No) ++ ", '" ++
        atom_to_list(Q) ++ "')), " ++
        "myfamily, myseries, time))".

make_data(NPuts, Q, NSpans) ->
    Multi = get_multi(Q) * NSpans,
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    Times = lists:seq(1, NPuts),
    [{Family, Series, trunc((X/NPuts) * Multi),
      ts_util:get_varchar(),
      ts_util:get_float()}
     || X <- Times].

get_multi({No, y})  -> 365*24*60*60*1000 * No;
get_multi({No, mo}) -> 31*24*60*60*1000 * No;
get_multi({No, d})  -> 24*60*60*1000 * No;
get_multi({No, h})  -> 60*60*1000 * No;
get_multi({No, m})  -> 60*1000 * No;
get_multi({No, s})  -> 1000 * No.

timestamp() ->
    {A, B, C} = now(),
    integer_to_list(A) ++ integer_to_list(B) ++ integer_to_list(C).

%%
%% eqc generators
%%
%% generates a list with the following elements
%% [varchar, varchar, integer, double, boolean]
gen_n_val() ->
    ?LET(N, choose(1, 5), N).

gen_no_of_puts() ->
    ?LET(N, choose(1, 11), N).

% maximum 5 sub-queries means maximum of 4 spans
gen_spans() ->
    ?LET(N, choose(1, 4), N).

gen_quantum() ->
    ?LET({N, M}, {choose(1, 12), elements([d, h, m, s])}, {N, M}).

-endif. % EQC
