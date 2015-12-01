-module(verify_cluster_converge).

-behavior(riak_test).
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(assertDenied(Op), ?assertMatch({error, <<"Permission",_/binary>>}, Op)).

confirm() ->
    lager:info("Deploy & cluster some nodes"),

    _Nodes = rt:build_cluster(4),
    pass.