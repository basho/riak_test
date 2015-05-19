-module(rt_driver).

-define(DRIVER_MODULE, (rt_config:get(driver))).

-export([behaviour_info/1,
         cluster_module/0,
         new_configuration/0,
         new_configuration/1,
         get_configuration_key/2,
         set_configuration_key/3]).

-type configuration() :: term().
-exporttype([configuration/0]).

behaviour_info(callbacks) ->
    [{new_configuration, 0}, {new_configuration, 1}, {get_configuration_key, 2}, 
     {set_configuration_key, 3}, {cluster_module, 0}];
behaviour_info(_) ->
    undefined.

-spec cluster_module() -> module().
cluster_module() ->
    ?DRIVER_MODULE:cluster_module().

-spec new_configuration() -> configuration().
new_configuration() ->
    ?DRIVER_MODULE:new_configuration().

-spec new_configuration(atom()) -> configuration().
new_configuration(Profile) ->
    ?DRIVER_MODULE:new_configuration(Profile).

-spec get_configuration_key(atom(), configuration()) -> term().
get_configuration_key(Key, Configuration) ->
    ?DRIVER_MODULE:get_configuration_key(Configuration, Key).

-spec set_configuration_key(atom(), term(), configuration()) -> {ok, configuration()} | {error, string()}.
set_configuration_key(Configuration, Key, Value) ->
    ?DRIVER_MODULE:set_configuration_key(Configuration, Key, Value).
