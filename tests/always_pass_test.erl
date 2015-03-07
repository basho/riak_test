%% @doc A test that always returns `pass'.
-module(always_pass_test).
-behavior(riak_test).
-export([confirm/0]).

-spec confirm() -> pass | fail.
confirm() ->
    pass.
