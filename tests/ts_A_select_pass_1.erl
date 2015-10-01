-module(ts_A_select_pass_1).

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  get_valid_select_data/0,
			  get_valid_qry/0,
			  confirm_select/6
			  ]).

confirm() ->
    DDL = get_ddl(docs),
    Data = get_valid_select_data(),
    Qry = get_valid_qry(),
    confirm_select(single, normal, DDL, Data, Qry, {get_cols(docs), to_result(Data)}).

get_cols(docs) ->
        [<<"myfamily">>,
         <<"myseries">>,
         <<"time">>,
         <<"weather">>,
         <<"temperature">>].

to_result(Data) ->
	[_|Tail] = remove_last([list_to_tuple(R) || R <- Data]),
	Tail	.

remove_last(Data) ->
	lists:reverse(tl(lists:reverse(Data))).