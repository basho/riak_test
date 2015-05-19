-module(rt_riak_driver).

-behavior(rt_driver).

-export([cluster_module/0,
         new_configuration/0,
         new_configuration/1,
         get_configuration_key/2,
         set_configuration_key/3]).

-record(riak_configuration_v1, {
    %% TODO Add support for backend configuration options
    supported_backends=all :: [storage_backend()],
    wait_for_transfers=false :: boolean(),
    bucket_types=[] :: rt_bucket_types:bucket_types(),
    indexes=[] :: [index()],
    ring_size=auto :: [atom() | pos_integer()],
    features=[] :: [feature_flag()],
    mr_modules=[] :: [module()],
    %% TODO Can rt_riak_node properly calculate the list of required services?
    required_services=[riak_kv] :: [atom()],
    node_config=default_node_config() :: proplists:proplist()
    %% TODO What do we need to configure security?
}).
-define(CONFIGURATION_RECORD, #riak_configuration_v1).

-type riak_configuration() :: ?CONFIGURATION_RECORD{}.

-type feature() :: aae | strong_consistency | yokozuna | jmx | snmp | security.
-type feature_flag() :: { feature(), boolean() }.
-type storage_backend() :: bitcask | leveldb | memory | multi_backend.
-type index() :: {binary(), binary(), binary()}.

-exporttype([feature/0,
             feature_flag/0,
             storage_backend/0,
             index/0]).

-spec cluster_module() -> module().
cluster_module() ->
    rt_riak_cluster.

-spec default_node_config() -> [term()].
default_node_config() ->
    [{riak_core, [{handoff_concurrency, 11}]},
        {riak_search, [{enabled, true}]},
        {riak_pipe, [{worker_limit, 200}]}].

-spec new_configuration() -> riak_configuration().
new_configuration() ->
    ?CONFIGURATION_RECORD{}.

-spec new_configuration(atom()) -> riak_configuration().
new_configuration(default) ->
    ?CONFIGURATION_RECORD{}.

-spec get_configuration_key(atom(), riak_configuration()) -> term() | {error, string()}.
get_configuration_key(Key, Configuration) when is_record(Configuration, riak_configuration_v1) ->
    maybe_get_configuration_key(field_index(Key), Configuration);
get_configuration_key(Key, Configuration) ->
    {error, io_lib:format("~w is not a riak_configuration_v1 record from which to retrieve ~w", [Configuration, Key])}.

-spec maybe_get_configuration_key(pos_integer() | {error, string()}, riak_configuration()) -> term() | {error, string()}.
maybe_get_configuration_key(Error={error, _}, _Configuration) ->
    Error;
maybe_get_configuration_key(FieldIndex, Configuration) ->
    element(FieldIndex, Configuration).

-spec set_configuration_key(atom(), term(), riak_configuration()) -> {ok, riak_configuration()} | {error, string()}.
set_configuration_key(Key, Value, Configuration) when is_record(Configuration, riak_configuration_v1) ->
    maybe_set_configuration_key(field_index(Key), Configuration, Value);
set_configuration_key(Configuration, Key, Value) ->
    {error, io_lib:format("~w is not a riak_configuration_v1 record from which to set ~w to ~w", [Configuration, Key, Value])}.

-spec maybe_set_configuration_key({error, string()} | pos_integer, riak_configuration, term()) -> 
                                         {ok, riak_configuration()} | {error, string()}.
maybe_set_configuration_key(Error={error, _}, _Configuration, _Value) ->
    Error;
maybe_set_configuration_key(FieldIndex, Configuration, Value) ->
    {ok, setelement(FieldIndex, Configuration, Value)}.

-spec field_index(atom()) -> pos_integer | {error, string()}.
field_index(supported_backends) ->
    ?CONFIGURATION_RECORD.supported_backends;
field_index(wait_for_transfers) ->
    ?CONFIGURATION_RECORD.supported_backends;
field_index(bucket_types) ->
    ?CONFIGURATION_RECORD.bucket_types;
field_index(indexes) ->
    ?CONFIGURATION_RECORD.indexes;
field_index(ring_sizes) ->
    ?CONFIGURATION_RECORD.ring_size;
field_index(features) ->
    ?CONFIGURATION_RECORD.features;
field_index(mr_modules) ->
    ?CONFIGURATION_RECORD.mr_modules;
field_index(required_services) ->
    ?CONFIGURATION_RECORD.required_services;
field_index(node_config) ->
    ?CONFIGURATION_RECORD.node_config;
field_index(Unknown) ->
    {error, "Unknown Riak configuration field: " ++ Unknown}.

