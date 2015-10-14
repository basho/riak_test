-module(ts_A_create_table_fail_3a).

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  confirm_create/3
			  ]).

%%
%% should error if you try and create a table twice
%%

confirm() ->
    ClusterType = single,
    %% create the table twice with a different DDL
    DDL1 =  "CREATE TABLE GeoCheckin (" ++
	"myfamily    varchar   not null, " ++
	"myseries    varchar   not null, " ++
	"time        timestamp not null, " ++
	"weather     varchar   not null, " ++
	"temperature float, " ++
	"PRIMARY KEY ((quantum(time, 15, 'm'), myfamily, myseries), " ++
	"time, myfamily, myseries))",
    Expected1 = {ok, "GeoCheckin created\n\nWARNING: After activating GeoCheckin, " ++
		     "nodes in this cluster\ncan no longer be downgraded to a version of Riak prior to 2.0\n"},
    DDL2 =  "CREATE TABLE GeoCheckin (" ++
	"myfamily    varchar   not null, " ++
	"myseries    varchar   not null, " ++
	"time        timestamp not null, " ++
	"weather     varchar   not null, " ++
	"PRIMARY KEY ((quantum(time, 15, 'm'), myfamily, myseries), " ++
	"time, myfamily, myseries))",
    Expected2 = {ok,"some error message, yeah?"},
    pass = confirm_create(ClusterType, DDL1, Expected1),
    confirm_create(ClusterType, DDL2, Expected2).
