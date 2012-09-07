-module(rpc_output).

-export([confirm/0]).

confirm() ->
    io:put_chars("This is an io:put_chars/1 call"),
    io:format("This is an io:format/1 call"),
    io:format("This is an io:format/~w call", [2]),
    lager:info("This is a lager message").