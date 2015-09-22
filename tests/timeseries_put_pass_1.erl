-module(timeseries_put_pass_1).

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  get_valid_obj/0,
			  confirm_put/5
			  ]).

confirm() ->
    Cluster = single,
    DDL = get_ddl(docs),
    Obj = [get_valid_obj()],
    Expected = ok,
    confirm_put(Cluster, normal, DDL, Obj, Expected).
