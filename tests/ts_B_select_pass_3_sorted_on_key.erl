
%%% This test asserts that results for small queries are
%%% returned in key-sorted order

-module(ts_B_select_pass_3_sorted_on_key).

-behavior(riak_test).

-export([confirm/0]).

confirm() ->
    Cluster = multiple,
    TestType = normal,
    DDL = timeseries_util:get_ddl(docs),
    Data = timeseries_util:get_valid_select_data(),
    ShuffledData = shuffle_list(Data),
    Qry = timeseries_util:get_valid_qry(),
    Expected = {
        timeseries_util:get_cols(docs),
        timeseries_util:exclusive_result_from_data(Data, 1, 9000000000)},
    % write the shuffled TS records but expect the
    % unshuffled records
    timeseries_util:confirm_select(
        Cluster, TestType, DDL, ShuffledData, Qry, Expected).

%%
shuffle_list(List) ->
    random:seed(),
    RSeqd1 = [{random:uniform(), E} || E <- List],
    RSeqd2 = lists:sort(RSeqd1),
    [E || {_, E} <- RSeqd2].
