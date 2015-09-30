-module(ts_A_put_fail_2).

%%
%% this test tries to write well structured data that doesn't
%% meet the criteria defined in the DDL into a bucket
%%

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    ClusterType = single,
    DDL = timeseries_util:get_ddl(docs),
    Obj = [timeseries_util:get_invalid_obj()],
    ?assertMatch(
    	{error,_},
	    timeseries_util:confirm_put(ClusterType, normal, DDL, Obj)
	),
	pass.
