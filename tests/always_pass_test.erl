%% @doc A test that always returns `fail'.
-module(always_pass_test).

%% -behaviour(riak_test).

-export([properties/0,
         confirm/1]).

-include_lib("eunit/include/eunit.hrl").

properties() ->
    rt_properties:new([{make_cluster, false}]).

-spec confirm(rt_properties:properties()) -> pass | fail.
confirm(_Properties) ->
    lager:info("Running test confirm function"),
    ?assertEqual(1,1),
    pass.
