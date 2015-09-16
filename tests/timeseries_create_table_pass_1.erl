-module(timeseries_create_table_pass_1).

-define(DDL,      docs).
-define(EXPECTED, {ok,"GeoCheckin created\n\nWARNING: After activating GeoCheckin, nodes in this cluster\ncan no longer be downgraded to a version of Riak prior to 2.0\n"}).

-include("timeseries_single_node_create_table.part").
-include("timeseries_ddl_sql.part").
