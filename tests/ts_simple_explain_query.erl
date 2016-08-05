%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(ts_simple_explain_query).

-behavior(riak_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

%% Test basic table description

confirm() ->
    DDL = ts_util:get_ddl(big, "MyTable"),
    ClusterConn = {_Cluster, Conn} = ts_util:cluster_and_connect(single),
    ts_util:create_and_activate_bucket_type(ClusterConn, DDL, "MyTable"),
    Qry = "EXPLAIN SELECT myint, myfloat, myoptional FROM MyTable WHERE "
        "myfamily='wingo' AND myseries='dingo' AND time > 0 AND time < 2000000 "
        "AND (mybool=true OR myvarchar='banana')",

    Got = ts_util:single_query(Conn, Qry),
    Expected =
        {ok,{[<<"Subquery">>,<<"Range Scan Start Key">>,
            <<"Is Start Inclusive?">>,
            <<"Range Scan End Key">>,
            <<"Is End Inclusive?">>,<<"Filter">>],
            [{1,
                "myfamily = 'wingo', myseries = 'dingo', time = 1",
                false,
                "myfamily = 'wingo', myseries = 'dingo', time = 900000",
                false,
                "mybool = true OR myvarchar = 'banana'"},
            {2,
                "myfamily = 'wingo', myseries = 'dingo', time = 900000",
                false,
                "myfamily = 'wingo', myseries = 'dingo', time = 1800000",
                false,
                "mybool = true OR myvarchar = 'banana'"},
            {3,
                "myfamily = 'wingo', myseries = 'dingo', time = 1800000",
                false,
                "myfamily = 'wingo', myseries = 'dingo', time = 2000000",
                false,
                "mybool = true OR myvarchar = 'banana'"}]}},
    ?assertEqual(Expected, Got),
    pass.
