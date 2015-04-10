%%-------------------------------------------------------------------
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
%% -------------------------------------------------------------------
%% @author Brett Hazen
%% @copyright (C) 2015, Basho Technologies
%% @doc
%%
%% @end
%% Created : 30. Mar 2015 3:25 PM
%%-------------------------------------------------------------------
-module(rt_test_plan).
-author("Brett Hazen").

-include("rt.hrl").

%% API
-export([new/0,
         new/1,
         get/2,
         get_module/1,
         set/2,
         set/3]).

-record(rt_test_plan_v1, {
    id=-1 :: integer(),
    module :: atom(),
    project :: atom() | binary(),
    platform :: string(),
    backend=riak_kv_bitcask_backend :: rt_properties2:storage_backend(),
    upgrade_path=[] :: [rt_properties2:product_version()],
    properties :: rt_properties2:properties()
}).

-type test_plan() :: #rt_test_plan_v1{}.

-export_type([test_plan/0]).

-define(RT_TEST_PLAN, #rt_test_plan_v1).
-define(RECORD_FIELDS, record_info(fields, rt_test_plan_v1)).

%% Internal

%% @doc Create a new test plan record with all fields initialized to
%% the default values.
-spec new() -> test_plan().
new() ->
    ?RT_TEST_PLAN{}.

%% @doc Create a new test plan record with the fields initialized to
%% non-default value.  Each field to be initialized should be
%% specified as an entry in a property list (<i>i.e.</i> a list of
%% pairs). Invalid field fields are ignored by this function.
-spec new(proplists:proplist()) -> test_plan().
new(FieldDefaults) ->
    {Fields, _}  =
        lists:foldl(fun set_field/2, {?RT_TEST_PLAN{}, []}, FieldDefaults),
    Fields.

%% @doc Get the value of a field from a test plan record. An error
%% is returned if `TestPlan' is not a valid `rt_test_plan' record
%% or if the field requested is not a valid field.
-spec get(atom(), test_plan()) -> term() | {error, atom()}.
get(Field, TestPlan) ->
    get(Field, TestPlan, validate_request(Field, TestPlan)).

%% @doc Get the value of the test name from a test plan record. An error
%% is returned if `TestPlan' is not a valid `rt_test_plan' record
%% or if the field requested is not a valid field.
-spec get_module(test_plan()) -> term() | {error, atom()}.
get_module(TestPlan) ->
    get(module, TestPlan, validate_request(module, TestPlan)).

%% @doc Set the value for a field in a test plan record. An error
%% is returned if `TestPlan' is not a valid `rt_test_plan' record
%% or if any of the fields to be set are not a valid field. In
%% the case that invalid fields are specified the error returned
%% contains a list of erroneous fields.
-spec set([{atom(), term()}], test_plan()) -> test_plan() | {error, atom()}.
set(FieldList, TestPlan) when is_list(FieldList) ->
    set_fields(FieldList, TestPlan, validate_record(TestPlan)).

%% @doc Set the value for a field in a test plan record. An error
%% is returned if `TestPlan' is not a valid `rt_test_plan' record
%% or if the field to be set is not a valid field.
-spec set(atom(), term(), test_plan()) -> {ok, test_plan()} | {error, atom()}.
set(Field, Value, TestPlan) ->
    set_field(Field, Value, TestPlan, validate_request(Field, TestPlan)).


-spec get(atom(), test_plan(), ok | {error, atom()}) ->
    term() | {error, atom()}.
get(Field, Fields, ok) ->
    element(field_index(Field), Fields);
get(_Field, _Fields, {error, _}=Error) ->
    Error.

%% This function is used by `new/1' to set fields at record
%% creation time and by `set/2' to set multiple fields at once.
%% Test plan record validation is done by this function. It is
%% strictly used as a fold function which is the reason for the odd
%% structure of the input parameters.  It accumulates any invalid
%% properties that are encountered and the caller may use that
%% information or ignore it.
-spec set_field({atom(), term()}, {test_plan(), [atom()]}) ->
    {test_plan(), [atom()]}.
set_field({Field, Value}, {TestPlan, Invalid}) ->
    case is_valid_field(Field) of
        true ->
            {setelement(field_index(Field), TestPlan, Value), Invalid};
        false ->
            {TestPlan, [Field | Invalid]}
    end.

-spec set_field(atom(), term(), test_plan(), ok | {error, atom()}) ->
    {ok, test_plan()} | {error, atom()}.
set_field(Field, Value, TestPlan, ok) ->
    {ok, setelement(field_index(Field), TestPlan, Value)};
set_field(_Field, _Value, _Fields, {error, _}=Error) ->
    Error.

-spec set_fields([{atom(), term()}],
    test_plan(),
    ok | {error, {atom(), [atom()]}}) ->
    {test_plan(), [atom()]}.
set_fields(FieldList, TestPlan, ok) ->
    case lists:foldl(fun set_field/2, {TestPlan, []}, FieldList) of
        {UpdFields, []} ->
            UpdFields;
        {_, InvalidFields} ->
            {error, {invalid_properties, InvalidFields}}
    end;
set_fields(_, _, {error, _}=Error) ->
    Error.

-spec validate_request(atom(), test_plan()) -> ok | {error, atom()}.
validate_request(Field, TestPlan) ->
    validate_field(Field, validate_record(TestPlan)).

-spec validate_record(test_plan()) -> ok | {error, invalid_properties}.
validate_record(Record) ->
    case is_valid_record(Record) of
        true ->
            ok;
        false ->
            {error, invalid_properties}
    end.

-spec validate_field(atom(), ok | {error, atom()}) -> ok | {error, invalid_field}.
validate_field(Field, ok) ->
    case is_valid_field(Field) of
        true ->
            ok;
        false ->
            {error, invalid_field}
    end;
validate_field(_Field, {error, _}=Error) ->
    Error.

-spec is_valid_record(term()) -> boolean().
is_valid_record(Record) ->
    is_record(Record, rt_test_plan_v1).

-spec is_valid_field(atom()) -> boolean().
is_valid_field(Field) ->
    Fields = ?RECORD_FIELDS,
    lists:member(Field, Fields).

-spec field_index(atom()) -> non_neg_integer().
field_index(id) ->
    ?RT_TEST_PLAN.id;
field_index(module) ->
    ?RT_TEST_PLAN.module;
field_index(project) ->
    ?RT_TEST_PLAN.project;
field_index(platform) ->
    ?RT_TEST_PLAN.platform;
field_index(backend) ->
    ?RT_TEST_PLAN.backend;
field_index(upgrade_path) ->
    ?RT_TEST_PLAN.upgrade_path;
field_index(properties) ->
    ?RT_TEST_PLAN.properties.

