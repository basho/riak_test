-module(rt_cluster_info).
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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

%% @doc Implements a set of functions for accessing and manipulating
%% an `rt_cluster_info' record.

-record(rt_cluster_info_v1, {
          node_ids :: [string()],
          leader :: string(),
          name :: string()
         }).
-type cluster_info() :: #rt_cluster_info_v1{}.

-export_type([cluster_info/0]).

-define(RT_CLUSTER_INFO, #rt_cluster_info_v1).
-define(RECORD_FIELDS, record_info(fields, rt_cluster_info_v1)).

-export([new/0,
         new/1,
         get/2,
         set/2,
         set/3]).

%% @doc Create a new cluster_info record with all fields initialized to
%% the default values.
-spec new() -> cluster_info().
new() ->
    ?RT_CLUSTER_INFO{}.

%% @doc Create a new cluster_info record with the fields initialized to
%% non-default value.  Each field to be initialized should be
%% specified as an entry in a property list (<i>i.e.</i> a list of
%% pairs). Invalid fields are ignored by this function.
-spec new(proplists:proplist()) -> cluster_info().
new(Defaults) ->
    {ClusterInfo, _}  =
        lists:foldl(fun set_field/2, {?RT_CLUSTER_INFO{}, []}, Defaults),
    ClusterInfo.

%% @doc Get the value of a field from a cluster_info record. An error
%% is returned if `ClusterInfo' is not a valid `rt_cluster_info' record
%% or if the property requested is not a valid property.
-spec get(atom(), cluster_info()) -> term() | {error, atom()}.
get(Field, ClusterInfo) ->
    get(Field, ClusterInfo, validate_request(Field, ClusterInfo)).

%% @doc Set the value for a field in a cluster_info record. An error
%% is returned if `ClusterInfo' is not a valid `rt_cluster_info' record
%% or if any of the fields to be set are not valid. In
%% the case that invalid fields are specified the error returned
%% contains a list of erroneous fields.
-spec set([{atom(), term()}], cluster_info()) -> cluster_info() | {error, atom()}.
set(FieldList, ClusterInfo) when is_list(FieldList) ->
    set_fields(FieldList, ClusterInfo, validate_record(ClusterInfo)).

%% @doc Set the value for a field in a cluster_info record. An error
%% is returned if `ClusterInfo' is not a valid `rt_cluster_info' record
%% or if the field to be set is not valid.
-spec set(atom(), term(), cluster_info()) -> {ok, cluster_info()} | {error, atom()}.
set(Field, Value, ClusterInfo) ->
    set_field(Field, Value, ClusterInfo, validate_request(Field, ClusterInfo)).

-spec get(atom(), cluster_info(), ok | {error, atom()}) ->
                 term() | {error, atom()}.
get(Field, ClusterInfo, ok) ->
    element(field_index(Field), ClusterInfo);
get(_Field, _ClusterInfo, {error, _}=Error) ->
    Error.

%% This function is used by `new/1' to set fields at record
%% creation time and by `set/2' to set multiple properties at once.
%% No cluster_info record validation is done by this function. It is
%% strictly used as a fold function which is the reason for the odd
%% structure of the input parameters.  It accumulates any invalid
%% fields that are encountered and the caller may use that
%% information or ignore it.
-spec set_field({atom(), term()}, {cluster_info(), [atom()]}) ->
                          {cluster_info(), [atom()]}.
set_field({Field, Value}, {ClusterInfo, Invalid}) ->
    case is_valid_field(Field) of
        true ->
            {setelement(field_index(Field), ClusterInfo, Value), Invalid};
        false ->
            {ClusterInfo, [Field | Invalid]}
    end.

-spec set_field(atom(), term(), cluster_info(), ok | {error, atom()}) ->
                 {ok, cluster_info()} | {error, atom()}.
set_field(Field, Value, ClusterInfo, ok) ->
    {ok, setelement(field_index(Field), ClusterInfo, Value)};
set_field(_Field, _Value, _ClusterInfo, {error, _}=Error) ->
            Error.

-spec set_fields([{atom(), term()}],
                   cluster_info(),
                   ok | {error, {atom(), [atom()]}}) ->
                          {cluster_info(), [atom()]}.
set_fields(FieldList, ClusterInfo, ok) ->
    case lists:foldl(fun set_field/2, {ClusterInfo, []}, FieldList) of
        {UpdClusterInfo, []} ->
            UpdClusterInfo;
        {_, InvalidClusterInfo} ->
            {error, {invalid_properties, InvalidClusterInfo}}
    end;
set_fields(_, _, {error, _}=Error) ->
    Error.

-spec validate_request(atom(), term()) -> ok | {error, atom()}.
validate_request(Field, ClusterInfo) ->
    validate_field(Field, validate_record(ClusterInfo)).

-spec validate_record(term()) -> ok | {error, invalid_cluster_info}.
validate_record(Record) ->
    case is_valid_record(Record) of
        true ->
            ok;
        false ->
            {error, invalid_cluster_info}
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
    is_record(Record, rt_cluster_info_v1).

-spec is_valid_field(atom()) -> boolean().
is_valid_field(Field) ->
    Fields = ?RECORD_FIELDS,
    lists:member(Field, Fields).

-spec field_index(atom()) -> non_neg_integer().
field_index(node_ids) ->
    ?RT_CLUSTER_INFO.node_ids;
field_index(leader) ->
    ?RT_CLUSTER_INFO.leader;
field_index(name) ->
    ?RT_CLUSTER_INFO.name.
