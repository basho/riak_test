%% @doc A test that always returns `fail'.
-module(always_fail_test).

%% -behaviour(riak_test).

-export([confirm/1]).

-spec confirm(rt_properties:properties()) -> pass | fail.
confirm(_Properties) ->
    lager:info("Running test confirm function"),
    fail.
