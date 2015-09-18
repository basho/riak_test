-module(timeseries_activate_table_pass_1).

-define(TYPE,     activate).
-define(CLUSTER,  single).
-define(DDL,      docs).
-define(EXPECTED, {ok,"GeoCheckin has been activated\n\nWARNING: Nodes in this cluster can no longer be\ndowngraded to a version of Riak prior to 2.0\n"}).

-include("timeseries.part").
