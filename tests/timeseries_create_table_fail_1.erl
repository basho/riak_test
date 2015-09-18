-module(timeseries_create_table_fail_1).

-define(TYPE,     create).
-define(CLUSTER,  single).
-define(DDL,      shortkey_fail).
-define(EXPECTED, 'some error message, yeah?').

-include("timeseries.part").
