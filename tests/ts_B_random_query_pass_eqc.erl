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

-module(ts_B_random_query_pass_eqc).
-compile(export_all).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUMBEROFNODES, 3).

confirm() ->
    rt:set_backend(eleveldb),
    [Node | _] = build_cluster(?NUMBEROFNODES),
    ?assert(eqc:quickcheck(eqc:numtests(500, ?MODULE:prop_ts(Node)))),
    pass.

prop_ts(Node) ->
    ?FORALL({NVal, NPuts, Q, NSpans},
	    {gen_n_val(), gen_no_of_puts(), gen_quantum(), gen_spans()},
            run_query(Node, NVal, NPuts, Q, NSpans)).

run_query(Node, NVal, NPuts, Q, NSpans) ->

    Bucket = "Bucket_" ++ timestamp(),
    lager:debug("Bucket is ~p~n", [Bucket]),
    %% Bucket = "Bucket_1445256281934858",

    DDL = get_ddl(Bucket, Q),
    lager:debug("DDL is ~p~n", [DDL]),

    {ok, _Return1} = create_bucket(Bucket, Node, DDL, NVal),
    %% io:format("Create bucket returns ~p~n", [Return1]),

    {ok, _Return2} = activate_bucket(Bucket, Node),
    %% io:format("Activate bucket returns ~p~n", [Return2]),

    Data = make_data(NPuts, Q, NSpans),

    io:format("Data is ~p~n", [Data]),

    C = rt:pbc(Node),
    ok = riakc_ts:put(C, Bucket, Data),

    Query = make_query(Bucket, Q, NSpans),

    lager:debug("Query is ~p~n", [Query]),

    {_, Got} = riakc_ts:query(C, Query),
    %% should get the data back
    Got2 = [tuple_to_list(X) || X <- Got],

    ?assertEqual(Data, Got2),

    true.

activate_bucket(Bucket, Node) ->
    rt:admin(Node, ["bucket-type", "activate", Bucket]).

create_bucket(Bucket, Node, DDL, NVal) when is_integer(NVal) ->
    Props = io_lib:format("{\\\"props\\\": {\\\"n_val\\\": " ++
			      integer_to_list(NVal) ++
			      ", \\\"table_def\\\": \\\"~s\\\"}}", [DDL]),
    io:format("Creating bucket with ~p~n", [lists:flatten(Props)]),
    rt:admin(Node, ["bucket-type", "create", Bucket,
		    lists:flatten(Props)]).

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
	"temperature float, " ++
	"PRIMARY KEY ((quantum(time, " ++ integer_to_list(No) ++ ", '" ++
	atom_to_list(Q) ++ "'), myfamily, myseries), " ++
	"time, myfamily, myseries))".

make_data(NPuts, Q, NSpans) ->
    Multi = get_multi(Q) * NSpans,
    Family = <<"family1">>,
    Series = <<"seriesX">>,
    Times = lists:seq(1, NPuts),
    [[Family, Series, trunc((X/NPuts) * Multi),
      timeseries_util:get_varchar(),
      timeseries_util:get_float()]
     || X <- Times].

get_multi({No, y})  -> 365*24*60*60*1000 * No;
get_multi({No, mo}) -> 31*24*60*60*1000 * No;
get_multi({No, d})  -> 24*60*60*1000 * No;
get_multi({No, h})  -> 60*60*1000 * No;
get_multi({No, m})  -> 60*1000 * No;
get_multi({No, s})  -> 1000 * No.

build_cluster(Size) ->
    rt:set_backend(eleveldb),
    Nodes = rt:deploy_nodes(Size, []),
    rt:join_cluster(Nodes),
    Nodes.

timestamp() ->
    {A, B, C} = now(),
    integer_to_list(A) ++ integer_to_list(B) ++ integer_to_list(C).

%%
%% eqc generators
%%
%% generates a list with the following elements
%% [varchar, varchar, integer, float, boolean]
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
