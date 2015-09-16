-module(timeseries_create_table_fail_1).

-define(DDL,      shortkey_fail).
-define(EXPECTED, 'some error message, yeah?').

-include("timeseries_single_node_create_table.part").
-include("timeseries_ddl_sql.part").
