%% @doc A test that always returns `fail'.
-module(always_fail_test).
-export([confirm/2]).

-spec confirm(rt_properties:properties(), proplists:proplist()) -> pass | fail.
confirm(_Properties, _MD) ->
    lager:info("Running test confirm function"),
    fail.
