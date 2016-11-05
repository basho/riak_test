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
    Table = "MyTable",
    DDL = ts_data:get_ddl(big, Table),
    Cluster = ts_setup:start_cluster(1),
    ts_setup:create_bucket_type(Cluster, DDL, Table),
    ts_setup:activate_bucket_type(Cluster, Table),

    Qry = "EXPLAIN SELECT myint, myfloat, myoptional FROM MyTable WHERE "
        "myfamily='wingo' AND myseries='dingo' AND time > 0 AND time < 2000000 "
        "AND ((mybool=true AND myvarchar='banana') OR (myoptional=7))",

    Got = ts_ops:query(Cluster, Qry),
    Expected =
        {ok,{[<<"Subquery">>,
            <<"Coverage Plan">>,
            <<"Range Scan Start Key">>,
            <<"Is Start Inclusive?">>,
            <<"Range Scan End Key">>,
            <<"Is End Inclusive?">>,<<"Filter">>],
            [{1,
                <<"dev1@127.0.0.1/49, dev1@127.0.0.1/50, dev1@127.0.0.1/51">>,
                <<"myfamily = 'wingo', myseries = 'dingo', time = 1">>,
                false,
                <<"myfamily = 'wingo', myseries = 'dingo', time = 900000">>,
                false,
                <<"((myoptional = 7) OR ((mybool = true) AND (myvarchar = 'banana')))">>},
             {2,
                <<"dev1@127.0.0.1/11, dev1@127.0.0.1/12, dev1@127.0.0.1/13">>,
                <<"myfamily = 'wingo', myseries = 'dingo', time = 900000">>,
                false,
                <<"myfamily = 'wingo', myseries = 'dingo', time = 1800000">>,
                false,
                <<"((myoptional = 7) OR ((mybool = true) AND (myvarchar = 'banana')))">>},
             {3,
                <<"dev1@127.0.0.1/59, dev1@127.0.0.1/60, dev1@127.0.0.1/61">>,
                <<"myfamily = 'wingo', myseries = 'dingo', time = 1800000">>,
                false,
                <<"myfamily = 'wingo', myseries = 'dingo', time = 2000000">>,
                false,
                <<"((myoptional = 7) OR ((mybool = true) AND (myvarchar = 'banana')))">>}]}},
    ?assertEqual(Expected, Got),

    %% Now try NOT using TTB
    Got2 = ts_ops:query(Cluster, Qry, [{use_ttb, false}]),
    ?assertEqual(Expected, Got2),
    pass.
