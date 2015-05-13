-module(rt_util).

-type error() :: {error(), term()}.
-type result() :: ok | error().

-export_type([error/0,
              result/0]).
-export([backend_to_atom_list/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Convert string or atom to list of atoms
-spec(backend_to_atom_list(atom()|string()) -> undefined | list()).
backend_to_atom_list(undefined) ->
    undefined;
backend_to_atom_list(Backends) when is_atom(Backends) ->
    ListBackends = atom_to_list(Backends),
    case lists:member($, , ListBackends) of
        true ->
            [list_to_atom(X) || X <- string:tokens(ListBackends, ", ")];
        _ ->
            [Backends]
    end;
backend_to_atom_list(Backends) when is_list(Backends) ->
    case lists:member($, , Backends) of
        true ->
            [list_to_atom(X) || X <- string:tokens(Backends, ", ")];
        _ ->
            [list_to_atom(Backends)]
    end.

-ifdef(TEST).
%% Properly covert backends to atoms
backend_to_atom_list_test() ->
    ?assertEqual(undefined, backend_to_atom_list(undefined)),
    ?assertEqual([memory], backend_to_atom_list(memory)),
    ?assertEqual([memory], backend_to_atom_list("memory")),
    ?assertEqual([bitcask, eleveldb, memory], lists:sort(backend_to_atom_list("memory, bitcask,eleveldb"))),
    ?assertEqual([bitcask, eleveldb, memory], lists:sort(backend_to_atom_list('memory, bitcask,eleveldb'))).
-endif.