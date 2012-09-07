-module(riak_test_util).

-export([contains_substring/2]).

contains_substring(String, Substring) ->
    contains_substring(String, Substring, Substring).

contains_substring([], _, _) -> false;
contains_substring([Head|_Tail], [Head], _) -> true;
contains_substring([Head|Tail], [Head|SubTail], Substring) ->
    contains_substring(Tail, SubTail, Substring);
contains_substring([_Head|Tail], [_|_], Substring) ->
    contains_substring(Tail, Substring, Substring).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
contains_substring_test() ->
    ?assert(contains_substring("Hi, I like turtles.", "turtles")),
    ?assert(contains_substring("Hi, I like turtles.", "turt")),
    ?assert(contains_substring("Hi, I like turtles.", "I")),
    ?assertNot(contains_substring("Hi, I like turtles.", "penguins")),
    ok.
-endif.