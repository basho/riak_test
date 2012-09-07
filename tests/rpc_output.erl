-module(rpc_output).

-export([confirm/0]).

confirm() ->
    io:format("This should be a lager message!"),
    lager:info("This is a lager message").