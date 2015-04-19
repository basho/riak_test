-module(rt_properties2).
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
%% -------------------------------------------------------------------

%% @doc Implements a set of functions for accessing and manipulating
%% an `rt_properties2' record.

-record(rt_cluster_topology_v1, {
    name :: atom(),
    connected_to :: [] | [atom()],
    nodes :: [product_version()]
}).
-define(RT_CLUSTER_TOPOLOGY, #rt_cluster_topology_v1).

-record(rt_properties_v2, {
    description :: string(),
    supported_products :: [atom()],
    minimum_version :: string(),
    maximum_version :: string(),
    supported_backends=all :: [storage_backend()],
    wait_for_transfers=false :: boolean(),
    bucket_types=[] :: rt_bucket_types:bucket_types(),
    indexes=[] :: [index()],
    ring_size=auto :: [atom() | non_neg_integer()],
    features :: feature_flag(),
    required_services=[riak_kv] :: [atom()],
    cluster_topology=default_topology(3) :: [topology()],
    default_version=rt_config:get_default_version() :: product_version(),
    upgrade_path :: [product_version()],
    config=default_config() :: term()
}).

-type properties() :: #rt_properties_v2{}.
-type topology() :: #rt_cluster_topology_v1{}.
-type feature_flag() :: strong_consistency | yokozuna | jmx | snmp | security.
-type product_version() :: string() | atom().
-type storage_backend() :: all | riak_kv_bitcask_backend | riak_kv_eleveldb_backend | riak_kv_memory_backend | riak_kv_multi_backend.
-type index() :: {binary(), binary(), binary()}.

-export_type([properties/0,
              feature_flag/0,
              product_version/0,
              storage_backend/0,
              index/0]).


-define(RT_PROPERTIES, #rt_properties_v2).
-define(RECORD_FIELDS, record_info(fields, rt_properties_v2)).

-export([new/0,
    new/1,
    get/2,
    set/2,
    set/3,
    default_topology/1,
    default_config/0]).

%% @doc Create a new properties record with all fields initialized to
%% the default values.
-spec new() -> properties().
new() ->
    ?RT_PROPERTIES{}.

%% @doc Create a new properties record with the fields initialized to
%% non-default value.  Each field to be initialized should be
%% specified as an entry in a property list (<i>i.e.</i> a list of
%% pairs). Invalid property fields are ignored by this function.
-spec new(proplists:proplist()) -> properties().
new(PropertyDefaults) ->
    {Properties, _}  =
        lists:foldl(fun set_property/2, {?RT_PROPERTIES{}, []}, PropertyDefaults),
    Properties.

%% @doc Get the value of a property from a properties record. An error
%% is returned if `Properties' is not a valid `rt_properties2' record
%% or if the property requested is not a valid property.
-spec get(atom(), properties()) -> term() | {error, atom()}.
get(Property, Properties) ->
    get(Property, Properties, validate_request(Property, Properties)).

%% @doc Set the value for a property in a properties record. An error
%% is returned if `Properties' is not a valid `rt_properties2' record
%% or if any of the properties to be set are not a valid property. In
%% the case that invalid properties are specified the error returned
%% contains a list of erroneous properties.
-spec set([{atom(), term()}], properties()) -> properties() | {error, atom()}.
set(PropertyList, Properties) when is_list(PropertyList) ->
    set_properties(PropertyList, Properties, validate_record(Properties)).

%% @doc Set the value for a property in a properties record. An error
%% is returned if `Properties' is not a valid `rt_properties2' record
%% or if the property to be set is not a valid property.
-spec set(atom(), term(), properties()) -> {ok, properties()} | {error, atom()}.
set(Property, Value, Properties) ->
    set_property(Property, Value, Properties, validate_request(Property, Properties)).


-spec get(atom(), properties(), ok | {error, atom()}) ->
    term() | {error, atom()}.
get(Property, Properties, ok) ->
    element(field_index(Property), Properties);
get(_Property, _Properties, {error, _}=Error) ->
    Error.

%% This function is used by `new/1' to set properties at record
%% creation time and by `set/2' to set multiple properties at once.
%% Node properties record validation is done by this function. It is
%% strictly used as a fold function which is the reason for the odd
%% structure of the input parameters.  It accumulates any invalid
%% properties that are encountered and the caller may use that
%% information or ignore it.
-spec set_property({atom(), term()}, {properties(), [atom()]}) ->
    {properties(), [atom()]}.
set_property({Property, Value}, {Properties, Invalid}) ->
    case is_valid_property(Property) of
        true ->
            {setelement(field_index(Property), Properties, Value), Invalid};
        false ->
            {Properties, [Property | Invalid]}
    end.

-spec set_property(atom(), term(), properties(), ok | {error, atom()}) ->
    {ok, properties()} | {error, atom()}.
set_property(Property, Value, Properties, ok) ->
    {ok, setelement(field_index(Property), Properties, Value)};
set_property(_Property, _Value, _Properties, {error, _}=Error) ->
    Error.

-spec set_properties([{atom(), term()}],
    properties(),
    ok | {error, {atom(), [atom()]}}) ->
    {properties(), [atom()]}.
set_properties(PropertyList, Properties, ok) ->
    case lists:foldl(fun set_property/2, {Properties, []}, PropertyList) of
        {UpdProperties, []} ->
            UpdProperties;
        {_, InvalidProperties} ->
            {error, {invalid_properties, InvalidProperties}}
    end;
set_properties(_, _, {error, _}=Error) ->
    Error.

-spec validate_request(atom(), term()) -> ok | {error, atom()}.
validate_request(Property, Properties) ->
    validate_property(Property, validate_record(Properties)).

-spec validate_record(term()) -> ok | {error, invalid_properties}.
validate_record(Record) ->
    case is_valid_record(Record) of
        true ->
            ok;
        false ->
            {error, invalid_properties}
    end.

-spec validate_property(atom(), ok | {error, atom()}) -> ok | {error, invalid_property}.
validate_property(Property, ok) ->
    case is_valid_property(Property) of
        true ->
            ok;
        false ->
            {error, invalid_property}
    end;
validate_property(_Property, {error, _}=Error) ->
    Error.

-spec default_config() -> [term()].
default_config() ->
    [{riak_core, [{handoff_concurrency, 11}]},
        {riak_search, [{enabled, true}]},
        {riak_pipe, [{worker_limit, 200}]}].

%% @doc Create a single default cluster topology with default node versions
-spec default_topology(pos_integer()) -> [topology()].
default_topology(N) ->
    ?RT_CLUSTER_TOPOLOGY{name=cluster1, connected_to=[], nodes=[rt_config:get_default_version() || lists:seq(1,N)]}.

-spec is_valid_record(term()) -> boolean().
is_valid_record(Record) ->
    is_record(Record, rt_properties_v2).

-spec is_valid_property(atom()) -> boolean().
is_valid_property(Property) ->
    Fields = ?RECORD_FIELDS,
    lists:member(Property, Fields).

-spec field_index(atom()) -> non_neg_integer().
field_index(description) ->
    ?RT_PROPERTIES.description;
field_index(supported_products) ->
    ?RT_PROPERTIES.supported_products;
field_index(minimum_version) ->
    ?RT_PROPERTIES.minimum_version;
field_index(supported_backends) ->
    ?RT_PROPERTIES.supported_backends;
field_index(wait_for_transfers) ->
    ?RT_PROPERTIES.wait_for_transfers;
field_index(bucket_types) ->
    ?RT_PROPERTIES.bucket_types;
field_index(indexes) ->
    ?RT_PROPERTIES.indexes;
field_index(upgrade_path) ->
    ?RT_PROPERTIES.upgrade_path;
field_index(ring_size) ->
    ?RT_PROPERTIES.ring_size;
field_index(features) ->
    ?RT_PROPERTIES.features;
field_index(required_services) ->
    ?RT_PROPERTIES.required_services;
field_index(cluster_topology) ->
    ?RT_PROPERTIES.cluster_topology;
field_index(default_version) ->
    ?RT_PROPERTIES.default_version;
field_index(config) ->
    ?RT_PROPERTIES.config.
