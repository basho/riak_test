
%%% This test asserts that results for small queries are
%%% returned in key-sorted order

-module(ts_B_select_pass_3_sorted_on_key).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    ClusterType = multiple,
    TestType = normal,
    DDL = ts_util:get_ddl(docs),
    Data = ts_util:get_valid_select_data(),
    ShuffledData = shuffle_list(Data),
    Qry = ts_util:get_valid_qry(),
    Expected = {
        ts_util:get_cols(docs),
        ts_util:exclusive_result_from_data(Data, 2, 9)},
    % write the shuffled TS records but expect the
    % unshuffled records
    Got = ts_util:ts_query(ts_util:cluster_and_connect(ClusterType), TestType, DDL, ShuffledData, Qry),
    ?assertEqual(Expected, Got),
    pass.

%%
shuffle_list(List) ->
    random:seed(),
    RSeqd1 = [{random:uniform(), E} || E <- List],
    RSeqd2 = lists:sort(RSeqd1),
    [E || {_, E} <- RSeqd2].
