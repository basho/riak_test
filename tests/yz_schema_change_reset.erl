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
-module(yz_schema_change_reset).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(GET(K,L), proplists:get_value(K, L)).
-define(INDEX, <<"test_schema_change_reset">>).
-define(TYPE, <<"test_schema_change">>).
-define(BUCKET1, <<"test_schema_change_reset">>).
-define(BUCKET2, {?TYPE, <<"test_schema_change_reset_2">>}).
-define(SCHEMANAME, <<"test">>).

-define(TEST_SCHEMA,
<<"<schema name=\"test\" version=\"1.5\">
<fields>
   <dynamicField name=\"*_foo_register\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <dynamicField name=\"*\" type=\"ignored\"/>

   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
   <field name=\"age\" type=\"int\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
    <fieldType name=\"ignored\" indexed=\"false\" stored=\"false\" multiValued=\"false\" class=\"solr.StrField\" />

    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"int\" class=\"solr.TrieIntField\" precisionStep=\"0\" positionIncrementGap=\"0\" />
    <fieldType name=\"text_general\" class=\"solr.TextField\" positionIncrementGap=\"100\">
      <analyzer type=\"index\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
        <filter class=\"solr.LowerCaseFilterFactory\"/>
      </analyzer>
      <analyzer type=\"query\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
        <filter class=\"solr.SynonymFilterFactory\" synonyms=\"synonyms.txt\" ignoreCase=\"true\" expand=\"true\"/>
        <filter class=\"solr.LowerCaseFilterFactory\"/>
      </analyzer>
    </fieldType>
</types>
</schema>">>).
-define(TEST_SCHEMA_UPDATE,
<<"<schema name=\"test\" version=\"1.5\">
<fields>
   <dynamicField name=\"*_foo_register\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <dynamicField name=\"*_baz_counter\" type=\"int\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <dynamicField name=\"paths.*\" type=\"int\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <dynamicField name=\"*\" type=\"ignored\"/>

   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"text\" type=\"text_general\" indexed=\"true\" stored=\"false\" multiValued=\"true\"/>
   <field name=\"age\" type=\"string\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"hello_i\" type=\"int\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
    <fieldType name=\"ignored\" indexed=\"false\" stored=\"false\" multiValued=\"false\" class=\"solr.StrField\" />
    <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\" />
    <fieldType name=\"int\" class=\"solr.TrieIntField\" precisionStep=\"0\" positionIncrementGap=\"0\" />
    <fieldType name=\"text_general\" class=\"solr.TextField\" positionIncrementGap=\"100\">
      <analyzer type=\"index\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
        <filter class=\"solr.LowerCaseFilterFactory\"/>
      </analyzer>
      <analyzer type=\"query\">
        <tokenizer class=\"solr.StandardTokenizerFactory\"/>
        <filter class=\"solr.StopFilterFactory\" ignoreCase=\"true\" words=\"stopwords.txt\" enablePositionIncrements=\"true\" />
        <filter class=\"solr.SynonymFilterFactory\" synonyms=\"synonyms.txt\" ignoreCase=\"true\" expand=\"true\"/>
        <filter class=\"solr.LowerCaseFilterFactory\"/>
      </analyzer>
    </fieldType>
</types>
</schema>">>).

-define(SEQMAX, 20).
-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16},
           {anti_entropy_build_limit, {100, 1000}},
           {anti_entropy_concurrency, 8}
          ]},
         {yokozuna,
          [
           {anti_entropy_tick, 1000},
           {enabled, true}
          ]}
        ]).

confirm() ->
    [Node1|_RestNodes] = Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    %% Generate keys, YZ only supports UTF-8 compatible keys
    GenKeys = [<<N:64/integer>> || N <- lists:seq(1, ?SEQMAX),
                                  not lists:any(
                                        fun(E) -> E > 127 end,
                                        binary_to_list(<<N:64/integer>>))],
    KeyCount = length(GenKeys),
    lager:info("KeyCount ~p", [KeyCount]),

    Pid = rt:pbc(rt:select_random(Cluster)),

    lager:info("Write initial data to index ~p with schema ~p",
               [?INDEX, ?SCHEMANAME]),

    yokozuna_rt:write_data(Cluster, Pid, ?INDEX,
                           {?SCHEMANAME, ?TEST_SCHEMA},
                           ?BUCKET1, GenKeys),
    timer:sleep(1100),

    lager:info("Create and activate map-based bucket type ~s and tie it to search_index ~s",
               [?TYPE, ?INDEX]),
    rt:create_and_activate_bucket_type(Node1, ?TYPE, [{datatype, map},
                                                      {search_index, ?INDEX}]),

    lager:info("Write and check age at integer per original schema"),

    NewObj1A = riakc_obj:new(?BUCKET1, <<"keyA">>,
                             <<"{\"age\":26}">>,
                             "application/json"),

    NewObj1B = riakc_obj:new(?BUCKET1, <<"keyB">>,
                             <<"{\"age\":99}">>,
                             "application/json"),

    {ok, _ObjA} = riakc_pb_socket:put(Pid, NewObj1A, [return_head]),
    timer:sleep(1100),
    {ok, _ObjB} = riakc_pb_socket:put(Pid, NewObj1B, [return_head]),
    timer:sleep(1100),

    yokozuna_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount + 2),

    assert_search(Pid, Cluster, <<"age:26">>, {<<"age">>, <<"26">>}, []),
    assert_search(Pid, Cluster, <<"age:99">>, {<<"age">>, <<"99">>}, []),

    Map1 = riakc_map:update(
             {<<"0_foo">>, register},
             fun(R) ->
                     riakc_register:set(<<"44ab">>, R)
             end, riakc_map:new()),
    ok = riakc_pb_socket:update_type(
           Pid,
           ?BUCKET2,
           <<"keyMap1">>,
           riakc_map:to_op(Map1)),

    {ok, Map2} = riakc_pb_socket:fetch_type(Pid, ?BUCKET2, <<"keyMap1">>),
    Map3 = riakc_map:update(
             {<<"1_baz">>, counter},
             fun(R) ->
                     riakc_counter:increment(10, R)
             end, Map2),
    ok = riakc_pb_socket:update_type(
           Pid,
           ?BUCKET2,
           <<"keyMap1">>,
           riakc_map:to_op(Map3)),

    timer:sleep(1100),
    assert_search(Pid, Cluster, <<"0_foo_register:44ab">>, {<<"0_foo_register">>,
                                                            <<"44ab">>}, []),

    lager:info("Expire and re-check count before updating schema"),

    yokozuna_rt:expire_trees(Cluster),
    yokozuna_rt:wait_for_aae(Cluster),

    yokozuna_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount + 3),

    lager:info("Overwrite schema with updated schema"),
    override_schema(Pid, Cluster, ?INDEX, ?SCHEMANAME, ?TEST_SCHEMA_UPDATE),

    lager:info("Write and check hello_i at integer per schema update"),

    NewObj2 = riakc_obj:new(?BUCKET1, <<"key2">>,
                            <<"{\"hello_i\":36}">>,
                            "application/json"),

    {ok, _Obj2} = riakc_pb_socket:put(Pid, NewObj2, [return_head]),
    timer:sleep(1100),

    yokozuna_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount + 4),
    assert_search(Pid, Cluster, <<"hello_i:36">>, {<<"hello_i">>, <<"36">>}, []),

    lager:info("Write and check age at string per schema update"),

    NewObj3 = riakc_obj:new(?BUCKET1, <<"key3">>,
                            <<"{\"age\":\"3jlkjkl\"}">>,
                            "application/json"),

    {ok, _Obj3} = riakc_pb_socket:put(Pid, NewObj3, [return_head]),

    yokozuna_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount + 5),
    assert_search(Pid, Cluster, <<"age:3jlkjkl">>,
                  {<<"age">>, <<"3jlkjkl">>}, []),

    lager:info("Expire and re-check count to make sure we're correctly indexed
               by the new schema"),

    yokozuna_rt:expire_trees(Cluster),
    yokozuna_rt:wait_for_aae(Cluster),

    yokozuna_rt:verify_num_found_query(Cluster, ?INDEX, KeyCount + 5),

    HP = rt:select_random(yokozuna_rt:host_entries(rt:connection_info(Cluster))),
    yokozuna_rt:search_expect(HP, ?INDEX, <<"age">>, <<"*">>, 2),

    lager:info("Re-Put because AAE won't find a diff even though the types
               have changed, as it only compares based on bkey currently.
               Also, this re-put will work as we have a default bucket (type)
               with allow_mult=false... no siblings"),

    {ok, _Obj4} = riakc_pb_socket:put(Pid, NewObj1A, [return_head]),
    timer:sleep(1100),

    assert_search(Pid, Cluster, <<"age:26">>, {<<"age">>, <<"26">>}, []),

    lager:info("Re-Put Map data by dec/inc counter to account for *change* and
               allow previously unindexed counter to be searchable"),

    {ok, Map4} = riakc_pb_socket:fetch_type(Pid, ?BUCKET2, <<"keyMap1">>),
    Map5 = riakc_map:update(
             {<<"1_baz">>, counter},
             fun(R) ->
                     riakc_counter:decrement(0, R),
                     riakc_counter:increment(0, R)
             end, Map4),
    ok = riakc_pb_socket:update_type(
           Pid,
           ?BUCKET2,
           <<"keyMap1">>,
           riakc_map:to_op(Map5)),

    timer:sleep(1100),
    assert_search(Pid, Cluster, <<"0_foo_register:44ab">>, {<<"0_foo_register">>,
                                                            <<"44ab">>}, []),
    assert_search(Pid, Cluster, <<"1_baz_counter:10">>, {<<"1_baz_counter">>,
                                                         <<"10">>}, []),

    lager:info("Test nested json searches w/ unsearched fields ignored"),

    NewObj5 = riakc_obj:new(?BUCKET1, <<"key4">>,
                            <<"{\"quip\":\"blashj3\",
                                \"paths\":{\"quip\":\"88\"}}">>,
                            "application/json"),
    {ok, _Obj5} = riakc_pb_socket:put(Pid, NewObj5, [return_head]),

    timer:sleep(1100),
    assert_search(Pid, Cluster, <<"paths.quip:88">>,
                  {<<"paths.quip">>, <<"88">>}, []),

    riakc_pb_socket:stop(Pid),

    pass.

override_schema(Pid, Cluster, Index, Schema, RawUpdate) ->
    ok = riakc_pb_socket:create_search_schema(Pid, Schema, RawUpdate),
    yokozuna_rt:wait_for_schema(Cluster, Schema, RawUpdate),
    [Node|_] = Cluster,
    {ok, _} = rpc:call(Node, yz_index, reload, [Index]).

assert_search(Pid, Cluster, Search, SearchExpect, Params) ->
    F = fun(_) ->
                lager:info("Searching ~p and asserting it exists",
                           [SearchExpect]),
                {ok,{search_results,[{_Index,Fields}], _Score, Found}} =
                    riakc_pb_socket:search(Pid, ?INDEX, Search, Params),
                ?assert(lists:member(SearchExpect, Fields)),
                case Found of
                    1 -> true;
                    0 -> false
                end
        end,
    rt:wait_until(Cluster, F).
