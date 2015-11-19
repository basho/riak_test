%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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

-module(ts_api).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

%---------------------------------------------------------------------
% Test full range_scan API
%---------------------------------------------------------------------

confirm() ->
    DDL  = ts_util:get_ddl(api),
    Data = ts_util:get_data(api),
    ClusterConn = {_Cluster, Conn} = ts_util:cluster_and_connect(single),
    ?assertEqual(ok, ts_util:ts_put(ClusterConn, normal, DDL, Data)),

    confirm_GtOps(Conn),
    confirm_GtEqOps(Conn),
    confirm_LtOps(Conn),
    confirm_LtEqOps(Conn),
    confirm_EqOps(Conn),
    confirm_NeqOps(Conn),
    pass.

%------------------------------------------------------------
% Test > Ops
%------------------------------------------------------------

confirm_GtOps(C) ->
    confirm_Pass(C, {myint,   int,     '>', 1}),
    confirm_Pass(C, {myfloat, float,   '>', 1.0}),
    confirm_Error(C, {mybin,   varchar, '>', test2}),
    confirm_Error(C, {mybool,  boolean, '>', true}).

%------------------------------------------------------------
% Test >= Ops
%------------------------------------------------------------

confirm_GtEqOps(C) ->
    confirm_Pass(C, {myint,   int,     '>=', 1}),
    confirm_Pass(C, {myfloat, float,   '>=', 1.0}),
    confirm_Error(C, {mybin,   varchar, '>=', test2}),
    confirm_Error(C, {mybool,  boolean, '>=', true}).

%------------------------------------------------------------
% Test < Ops
%------------------------------------------------------------

confirm_LtOps(C) ->
    confirm_Pass(C, {myint,   int,     '<', 4}),
    confirm_Pass(C, {myfloat, float,   '<', 4.0}),
    confirm_Error(C, {mybin,   varchar, '<', test2}),
    confirm_Error(C, {mybool,  boolean, '<', true}).

%------------------------------------------------------------
% Test <= Ops
%------------------------------------------------------------

confirm_LtEqOps(C) ->
    confirm_Pass(C, {myint,   int,     '<=', 4}),
    confirm_Pass(C, {myfloat, float,   '<=', 4.0}),
    confirm_Error(C, {mybin,   varchar, '<=', test2}),
    confirm_Error(C, {mybool,  boolean, '<=', true}).

%------------------------------------------------------------
% Test == Ops
%------------------------------------------------------------

confirm_EqOps(C) ->
    confirm_Pass(C, {myint,   int,     '=', 2}),
    confirm_Pass(C, {myfloat, float,   '=', 2.0}),
    confirm_Pass(C, {mybin,   varchar, '=', test2}),
    confirm_Pass(C, {mybool,  boolean, '=', true}).

%------------------------------------------------------------
% Test != Ops
%------------------------------------------------------------

confirm_NeqOps(C) ->
    confirm_Pass(C, {myint,   int,     '!=', 2}),
    confirm_Pass(C, {myfloat, float,   '!=', 2.0}),
    confirm_Pass(C, {mybin,   varchar, '!=', test2}),
    confirm_Pass(C, {mybool,  boolean, '!=', true}).

%------------------------------------------------------------
% Runs a query, and checks that the result is non-trivial (ie, returns
% > 0 records) AND matches the expected.  This protects against bad
% tests that have no expected matches
%------------------------------------------------------------

confirm_pass(C, Qry, Expected) ->
    Got = ts_util:single_query(C, Qry),
    {_Cols, Records} = Got,
    N = length(Records),
    ?assertEqual(Expected, Got),
    ?assert(N > 0).

%------------------------------------------------------------
% Runs a query, and checks that the result is an error
%------------------------------------------------------------

confirm_error(C, Qry, _Expected) ->
    Got = ts_util:single_query(C, Qry),
    {Status, _Reason} = Got,
    ?assertEqual(Status, error).

%------------------------------------------------------------
% Utility to build a list
%------------------------------------------------------------

buildList(Acc, Next) ->
    case Acc of
        [] ->
            [Next];
        _ ->
            Acc ++ [Next]
    end.

%------------------------------------------------------------
% Given a list of lists, return a list of tuples
%------------------------------------------------------------

ltot(Lists) ->
    lists:foldl(fun(Entry, Acc) ->
        buildList(Acc, list_to_tuple(Entry))
                end, [], Lists).

%------------------------------------------------------------
% Return a list of indices corresponding to the passed list of field
% names
%------------------------------------------------------------

indexOf(Type, FieldNames) ->
    Fields = ts_util:get_map(Type),
    lists:foldl(fun(Name, Acc) ->
        {_Name, Index} = lists:keyfind(Name, 1, Fields),
        buildList(Acc, Index)
                end, [], FieldNames).

%------------------------------------------------------------
% Return a list of values corresponding to the requested field names
%------------------------------------------------------------

valuesOf(Type, FieldNames, Record) ->
    Indices = indexOf(Type, FieldNames),
    lists:foldl(fun(Index, Acc) ->
        buildList(Acc, lists:nth(Index, Record))
                end, [], Indices).

%------------------------------------------------------------
% Return all records matching the eval function
%------------------------------------------------------------

recordsMatching(Type, Data, FieldNames, CompVals, CompFun) ->
    ltot(lists:foldl(fun(Record, Acc) ->
        Vals = valuesOf(Type, FieldNames, Record),
        case CompFun(Vals, CompVals) of
            true ->
                buildList(Acc, Record);
            false ->
                Acc
        end
                     end, [], Data)).

%------------------------------------------------------------
% Return the expected data from a query
%------------------------------------------------------------

expected(Type, Data, Fields, CompVals, CompFn) ->
    Records = recordsMatching(Type, Data, Fields, CompVals, CompFn),
    case Records of
        [] ->
            {[],[]};
        _ ->
            {ts_util:get_cols(Type), Records}
    end.

%------------------------------------------------------------
% Construct a base-SQL query
%------------------------------------------------------------

baseQry() ->
    "SELECT * FROM GeoCheckin "
    "WHERE time >= 100 AND time <= 400 "
    "AND myfamily = 'family1' "
    "AND myseries = 'seriesX' ".

getQry({NameAtom, float, OpAtom, Val}) ->
    baseQry() ++ "AND " ++ atom_to_list(NameAtom) ++ " " ++ atom_to_list(OpAtom) ++ " " ++ float_to_list(Val);
getQry({NameAtom, int, OpAtom, Val}) ->
    baseQry() ++ "AND " ++ atom_to_list(NameAtom) ++ " " ++ atom_to_list(OpAtom) ++ " " ++ integer_to_list(Val);
getQry({NameAtom, varchar, OpAtom, Val}) ->
    baseQry() ++ "AND " ++ atom_to_list(NameAtom) ++ " " ++ atom_to_list(OpAtom) ++ " '" ++ atom_to_list(Val) ++ "'";
getQry({NameAtom, boolean, OpAtom, Val}) ->
    baseQry() ++ "AND " ++ atom_to_list(NameAtom) ++ " " ++ atom_to_list(OpAtom) ++ " " ++ atom_to_list(Val).

%------------------------------------------------------------
% Template for checking that a query passes
%------------------------------------------------------------

confirm_Pass(C, {NameAtom, TypeAtom, OpAtom, Val}) ->
    confirm_Template(C, {NameAtom, TypeAtom, OpAtom, Val}, pass).

%------------------------------------------------------------
% Template for checking that a query returns an error
%------------------------------------------------------------

confirm_Error(C, {NameAtom, TypeAtom, OpAtom, Val}) ->
    confirm_Template(C, {NameAtom, TypeAtom, OpAtom, Val}, error).

%------------------------------------------------------------
% Template for single-key range queries.  For example
%
%    confirm_Template(C, {myint, int, '>', 1}, pass)
%
% adds a query clause "AND myint > 1" to the base query SQL, and
% checks that the test passes. (passing 'error' as the Result atom
% would check that the test returns an error)
%------------------------------------------------------------

confirm_Template(C, {NameAtom, TypeAtom, OpAtom, Val}, Result) ->
    Data = ts_util:get_data(api),
    Qry = getQry({NameAtom, TypeAtom, OpAtom, Val}),
    Fields   = [<<"time">>, <<"myfamily">>, <<"myseries">>] ++ [list_to_binary(atom_to_list(NameAtom))],
    case TypeAtom of
        varchar ->
            CompVals = [<<"family1">>,  <<"seriesX">>] ++ [list_to_binary(atom_to_list(Val))];
        _ ->
            CompVals = [<<"family1">>,  <<"seriesX">>] ++ [Val]
    end,
    case OpAtom of
        '>' ->
            CompFn = fun(Vals, Cvals) ->
                [T,V1,V2,V3] = Vals,
                [C1,C2,C3] = Cvals,
                (T >= 100) and (T =< 400) and (V1 == C1) and (V2 == C2) and (V3 > C3)
                     end;
        '>=' ->
            CompFn = fun(Vals, Cvals) ->
                [T,V1,V2,V3] = Vals,
                [C1,C2,C3] = Cvals,
                (T >= 100) and (T =< 400) and (V1 == C1) and (V2 == C2) and (V3 >= C3)
                     end;
        '<' ->
            CompFn = fun(Vals, Cvals) ->
                [T,V1,V2,V3] = Vals,
                [C1,C2,C3] = Cvals,
                (T >= 100) and (T =< 400) and (V1 == C1) and (V2 == C2) and (V3 < C3)
                     end;
        '=<' ->
            CompFn = fun(Vals, Cvals) ->
                [T,V1,V2,V3] = Vals,
                [C1,C2,C3] = Cvals,
                (T >= 100) and (T =< 400) and (V1 == C1) and (V2 == C2) and (V3 =< C3)
                     end;
        '<=' ->
            CompFn = fun(Vals, Cvals) ->
                [T,V1,V2,V3] = Vals,
                [C1,C2,C3] = Cvals,
                (T >= 100) and (T =< 400) and (V1 == C1) and (V2 == C2) and (V3 =< C3)
                     end;
        '=' ->
            CompFn = fun(Vals, Cvals) ->
                [T,V1,V2,V3] = Vals,
                [C1,C2,C3] = Cvals,
                (T >= 100) and (T =< 400) and (V1 == C1) and (V2 == C2) and (V3 == C3)
                     end;
        '!=' ->
            CompFn = fun(Vals, Cvals) ->
                [T,V1,V2,V3] = Vals,
                [C1,C2,C3] = Cvals,
                (T >= 100) and (T =< 400) and (V1 == C1) and (V2 == C2) and (V3 /= C3)
                     end
    end,
    Expected = expected(api, Data, Fields, CompVals, CompFn),
    case Result of
        pass ->
            confirm_pass(C, Qry, Expected);
        error ->
            confirm_error(C, Qry, Expected)
    end.
