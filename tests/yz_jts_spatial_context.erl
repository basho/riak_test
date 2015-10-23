%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%--------------------------------------------------------------------
-module(yz_jts_spatial_context).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(INDEX, <<"test_spatial">>).
-define(BUCKET, <<"test_spatial">>).
-define(SCHEMANAME, <<"test_spatial">>).

-define(SPATIAL_SCHEMA,
        <<"<schema name=\"test_spatial\" version=\"1.5\">
     <fields>
       <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\"/>
       <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
       <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
       <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
       <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
       <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
       <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
       <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
       <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
       <field name=\"location_rpt\" type=\"location_rpt\" indexed=\"true\" stored=\"true\" multiValued=\"true\" />
     </fields>
     <uniqueKey>_yz_id</uniqueKey>
     <types>
       <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
       <fieldType name=\"location_rpt\" class=\"solr.SpatialRecursivePrefixTreeFieldType\"
         spatialContextFactory=\"com.spatial4j.core.context.jts.JtsSpatialContextFactory\"
         distErrPct=\"0.025\"
         maxDistErr=\"0.000009\"
         units=\"degrees\" />
     </types>
   </schema>">>).

-define(CONFIG,
        [
         {riak_core,
          [{ring_creation_size, 8}]
         },
         {yokozuna,
          [{enabled, true}]
         }]).

-define(FIELD_KEY, <<"location_rpt">>).
-define(POLYGON, <<"POLYGON((-10 30, -40 40, -10 -20, 40 20, 0 0, -10 30))">>).
-define(GEO_DOC, <<"{\"", ?FIELD_KEY/binary, "\":\"", ?POLYGON/binary, "\"}">>).
-define(GEO_QUERY, <<?FIELD_KEY/binary, ":\"IsDisjointTo(10.12 50.02)\"">>).

confirm() ->
    Cluster = rt:build_cluster(4, ?CONFIG),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    GenKeys = yokozuna_rt:gen_keys(20),

    Pid = rt:pbc(rt:select_random(Cluster)),

    lager:info("Write data to index ~p with schema ~p", [?INDEX, ?SCHEMANAME]),
    yokozuna_rt:write_data(Cluster, Pid, ?INDEX,
                           {?SCHEMANAME, ?SPATIAL_SCHEMA},
                           ?BUCKET, GenKeys),
    yokozuna_rt:commit(Cluster, ?INDEX),

    lager:info("Write and check geo field of type location_rpt"),
    GeoObj = riakc_obj:new(?BUCKET, <<"GeoKey">>, ?GEO_DOC, "application/json"),
    {ok, _Obj} = riakc_pb_socket:put(Pid, GeoObj, [return_head]),
    yokozuna_rt:assert_search(Pid, Cluster, ?INDEX, ?GEO_QUERY,
                              {?FIELD_KEY, ?POLYGON}, []),
    pass.
