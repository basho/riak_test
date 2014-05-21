%% @doc A test that always returns `fail'.
-module(always_fail_test).
-export([confirm/0]).

-spec confirm() -> pass | fail.
confirm() ->
    fail.
