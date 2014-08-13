%% @doc A test that always returns `fail'.
-module(always_pass_test).
-export([properties/0,
         confirm/2]).

-include_lib("eunit/include/eunit.hrl").

properties() ->
    rt_properties:new([{make_cluster, false}]).

-spec confirm(rt_properties:properties(), proplists:proplist()) -> pass | fail.
confirm(_Properties, _MD) ->
    lager:info("Running test confirm function"),
    ?assertEqual(1,1),
    pass.
