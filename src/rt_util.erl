%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(rt_util).
-include_lib("eunit/include/eunit.hrl").

-export([convert_to_atom/1,
         convert_to_atom_list/1,
         convert_to_string/1,
         find_atom_or_string/2,
         find_atom_or_string_dict/2,
         pmap/2]).

%% @doc Look up values by both atom and by string
find_atom_or_string(Key, Table) ->
    case {Key, proplists:get_value(Key, Table)} of
        {_, undefined} when is_atom(Key) ->
            proplists:get_value(atom_to_list(Key), Table);
        {_, undefined} when is_list(Key) ->
            proplists:get_value(list_to_atom(Key), Table);
        {Key, Value} ->
            Value
    end.

%% @doc Look up values in an orddict by both atom and by string
-spec find_atom_or_string(term(), orddict:orddict()) -> term() | undefined.
find_atom_or_string_dict(Key, Dict) ->
    case {Key, orddict:is_key(Key, Dict)} of
        {_, false} when is_atom(Key) ->
            case orddict:is_key(atom_to_list(Key), Dict) of
                true -> orddict:fetch(atom_to_list(Key), Dict);
                _ -> undefined
            end;
        {_, false} when is_list(Key) ->
            case orddict:is_key(list_to_atom(Key), Dict) of
                true -> orddict:fetch(list_to_atom(Key), Dict);
                _ -> undefined
            end;
        {_, true} ->
            orddict:fetch(Key, Dict)
    end.

%% @doc: Convert an atom to a string if it is not already
-spec convert_to_string(string()|atom()) -> string().
convert_to_string(Val) when is_atom(Val) ->
    atom_to_list(Val);
convert_to_string(Val) when is_list(Val) ->
    Val.

%% @doc: Convert a string to an atom if it is not already
-spec convert_to_atom(string()|atom()) -> atom().
convert_to_atom(Val) when is_list(Val) ->
    list_to_atom(Val);
convert_to_atom(Val) ->
    Val.

%% @doc Convert string or atom to list of atoms
-spec convert_to_atom_list(atom()|string()) -> undefined | list().
convert_to_atom_list(undefined) ->
    undefined;
convert_to_atom_list(Values) when is_atom(Values) ->
    ListOfValues = atom_to_list(Values),
    case lists:member($, , ListOfValues) of
        true ->
            [list_to_atom(X) || X <- string:tokens(ListOfValues, ", ")];
        _ ->
            [Values]
    end;
convert_to_atom_list(Values) when is_list(Values) ->
    case lists:member($, , Values) of
        true ->
            [list_to_atom(X) || X <- string:tokens(Values, ", ")];
        _ ->
            [list_to_atom(Values)]
    end.

%% @doc Parallel Map: Runs function F for each item in list L, then
%%      returns the list of results
-spec pmap(F :: fun(), L :: list()) -> list().
pmap(F, L) ->
    Parent = self(),
    lists:foldl(
    fun(X, N) ->
          spawn_link(fun() ->
                        Parent ! {pmap, N, F(X)}
                end),
          N+1
    end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

-ifdef(TEST).

%% Look up values in a proplist by either a string or atom
find_atom_to_string_test() ->
    ?assertEqual(undefined, find_atom_or_string(value, [{a,1},{b,2}])),
    ?assertEqual(undefined, find_atom_or_string("value", [{a,1},{b,2}])),
    ?assertEqual(1, find_atom_or_string(a, [{a,1},{b,2}])),
    ?assertEqual(1, find_atom_or_string("a", [{a,1},{b,2}])).

%% Look up values in an orddict by either a string or atom
find_atom_to_string_dict_test() ->
    Dict = [{a,"a"},{"b",b}],
    ?assertEqual(undefined, find_atom_or_string_dict(value, Dict)),
    ?assertEqual(undefined, find_atom_or_string_dict("value", Dict)),
    ?assertEqual("a", find_atom_or_string(a, Dict)),
    ?assertEqual(b, find_atom_or_string("b", Dict)).

%% Convert a string to an atom, otherwise it remains unchanged
convert_to_atom_test() ->
    ?assertEqual(a, convert_to_atom(a)),
    ?assertEqual(a, convert_to_atom("a")),
    ?assertEqual(1, convert_to_atom(1)).

%% Properly covert backends to atoms
convert_to_atom_list_test() ->
    ?assertEqual(undefined, convert_to_atom_list(undefined)),
    ?assertEqual([memory], convert_to_atom_list(memory)),
    ?assertEqual([memory], convert_to_atom_list("memory")),
    ?assertEqual([bitcask, eleveldb, leveled, memory], lists:sort(convert_to_atom_list("memory, bitcask,eleveldb, leveled"))),
    ?assertEqual([bitcask, eleveldb, leveled, memory], lists:sort(convert_to_atom_list('memory, bitcask,eleveldb, leveled'))).

-endif.
