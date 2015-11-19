-module(ts_A_create_table_split_key).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([
	 confirm/0
	]).

confirm() ->
    ClusterType = single,
    DDL = ts_util:get_ddl(splitkey_fail),
    Expected = {ok,"Error creating bucket type GeoCheckin:\nLocal key does not match primary key\n"},
    Got = ts_util:create_bucket_type(ts_util:build_cluster(ClusterType), DDL),
    ?assertEqual(Expected, Got),
    pass.
