-module(timeseries_put_pass_1).

-define(TYPE,     put).
-define(CLUSTER,  single).
-define(DDL,      docs).
-define(EXPECTED, {ok,"GeoCheckin created\n\nWARNING: After activating GeoCheckin, nodes in this cluster\ncan no longer be downgraded to a version of Riak prior to 2.0\n"}).

-include("timeseries.part").
