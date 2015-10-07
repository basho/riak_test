-module(ts_A_select_interpolation_pass_1).

-behavior(riak_test).

-export([confirm/0]).

-import(timeseries_util, [
                          get_ddl/1,
                          get_valid_select_data/0,
                          get_valid_interpolated_qry/0,
                          get_cols/1,
                          exclusive_result_from_data/3,
                          confirm_select/7
                         ]).

confirm() ->
    Cluster = single,
    TestType = normal,
    DDL = get_ddl(docs),
    Data = get_valid_select_data(),
    {QrySql, Interps} = get_valid_interpolated_qry(),
    Expected = {
      get_cols(docs),
      exclusive_result_from_data(Data, 2, 9)},
    confirm_select(Cluster, TestType, DDL, Data, QrySql, Interps, Expected).
