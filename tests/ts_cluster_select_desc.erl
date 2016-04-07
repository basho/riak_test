-module(ts_cluster_select_desc).

-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

confirm() ->
    [Node1|_] = Cluster = ts_util:build_cluster(single),
    Pid = rt:pbc(Node1),

    create_data_def_1(Pid, Cluster),
    select_def_1_test(Pid),

    create_data_def_2(Pid, Cluster),
    select_def_2_test(Pid),

    pass.

%%%
%%% TABLE 1
%%%

column_names_def_1() ->
    [<<"a">>, <<"b">>, <<"c">>].

table_def_1() ->
    "CREATE TABLE table1 ("
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "PRIMARY KEY ((a,b,quantum(c, 1, 's')), a,b DESC,c ))".

create_data_def_1(Pid, Cluster) ->
    {ok, _} = (
        ts_util:create_and_activate_bucket_type(Cluster, table_def_1(), <<"table1">>)),
    ok = riakc_ts:put(Pid, <<"table1">>, [{1,1,N} || N <- lists:seq(1,200)]).

select_def_1_test(Pid) ->
    Query =
        "SELECT * FROM table1 WHERE a = 1 AND b = 1 AND c >= 35 AND c <= 45",
    ?assertEqual(
        {ok, {column_names_def_1(), [{1,1,N} || N <- lists:seq(45,35,-1)]}},
        riakc_ts:query(Pid, Query)
    ).


%%%
%%% TABLE 2
%%%

column_names_def_2() ->
    [<<"a">>, <<"b">>, <<"c">>].

table_def_2() ->
    "CREATE TABLE table2 ("
    "a SINT64 NOT NULL, "
    "b SINT64 NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "PRIMARY KEY ((a,b,quantum(c, 1, 's')), a,b,c DESC))".

create_data_def_2(Pid, Cluster) ->
    {ok, _} = (
        ts_util:create_and_activate_bucket_type(Cluster, table_def_2(), <<"table2">>)),
    ok = riakc_ts:put(Pid, <<"table2">>, [{1,1,N} || N <- lists:seq(200,200*100,200)]).

select_def_2_test(Pid) ->
    Query =
        "SELECT * FROM table2 WHERE a = 1 AND b = 1 AND c >= 3000 AND c <= 5000",
    ?assertEqual(
        {ok, {column_names_def_2(), [{1,1,N} || N <- lists:seq(5000,3000,-200)]}},
        riakc_ts:query(Pid, Query)
    ).
