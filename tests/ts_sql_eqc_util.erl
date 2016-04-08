%% -*- Mode: Erlang -*-
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
%% @doc A module to test riak_ts basic create bucket/put/select cycle.

-module(ts_sql_eqc_util).

-compile(export_all).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_ql/include/riak_ql_ddl.hrl").

%% eqc generators

%%
%% Create Table generators
%%

gen_valid_create_table() ->
    ?LET(DDL, gen_valid_ddl(), {DDL#ddl_v1.table, make_valid_create_table(DDL)}).

%%
%% These generators generate DDL records
%%
gen_valid_ddl() ->
    ?SUCHTHAT(DDL, gen_ddl(), is_valid_ddl(DDL)).

gen_ddl() ->
    ?LET({TblName, Keys, Quantum, Fields},
         {
           gen_name("Table"),
           gen_key(),
           gen_quantum(),
           list(gen_field())
         },
         #ddl_v1{table         = TblName,
                 fields        = set_positions(Keys ++ Fields),
                 partition_key = make_pk(Keys, Quantum),
                 local_key     = make_lk(Keys)}).

gen_key() ->
    ?LET({Fam, Series, TS},
         {gen_series_or_family_key_field(), gen_series_or_family_key_field(), gen_timeseries_key_field()},
         [Fam, Series, TS]).

gen_field() ->
    ?LET({Nm, Type, Opt},
         {gen_name("Field"), gen_field_type(), gen_optional()},
         #riak_field_v1{name     = Nm,
                        type     = Type,
                        optional = Opt}).

gen_series_or_family_key_field() ->
    ?LET({Nm, Type},
         {gen_name("Key"), gen_field_type()},
         #riak_field_v1{name     = Nm,
                        type     = Type,
                        optional = false}).

gen_timeseries_key_field() ->
    ?LET(Nm,
         gen_name("TSKey"),
         #riak_field_v1{name     = Nm,
                        type     = timestamp,
                        optional = false}).

%%
%% generate field components
%%
gen_name(Prefix) ->
    %% utf8().
    gen_ascii_name(Prefix).

gen_field_type() ->
    ?LET(N, oneof([sint64, double, boolean, timestamp, varchar]), N).

gen_optional() ->
    ?LET(N, oneof([true, false]), N).

%% can't have a quantum of 0 so add one
gen_quantum() ->
    ?LET({Unit, No}, 
         {oneof([d, h, m, s]), int()},
         {Unit, abs(No + 1)}).

%%
%% generate query components
%%
gen_operator() ->
    ?LET(N, oneof(['=', '!=', '>', '<', '<=', '>=']), N).

%%
%% rando generators (make debugging easier)
%%
gen_ascii_name(Prefix) ->
    ?LET(N,
         vector(10, oneof([gen_ascii_lower_case(), gen_ascii_upper_case(), gen_ascii_num()])),
         list_to_binary(Prefix ++ "_" ++ N)).

gen_ascii_lower_case() ->
    ?LET(N, oneof([97, 98, 99, 100,
                   101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
                   111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
                   121, 122]), N).

gen_ascii_upper_case() ->
    ?LET(N, oneof([65, 66, 67, 68, 69, 70,
                   71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
                   81, 82, 83, 84, 85, 86, 87, 88, 89, 90]), N).

gen_ascii_num() ->
    ?LET(N, oneof([48, 49, 50, 51, 52, 53, 54, 55, 56, 57]), N).

%%
%% utility functions
%%
set_positions(Fields) ->
    set_p2(Fields, 1, []).

set_p2([], _N, Acc) ->
    lists:reverse(Acc);
set_p2([H | T], N, Acc) ->
    set_p2(T, N + 1, [H#riak_field_v1{position = N} | Acc]).

make_pk([Family, Series, TS], {Unit, No}) ->
    Quantum =  [#hash_fn_v1{mod  = riak_ql_quanta,
                            fn   = quantum,
                            args = [#param_v1{name = TS#riak_field_v1.name}, Unit, No],
                            type = timestamp}],
    AST = [#param_v1{name = X#riak_field_v1.name} || X <- [Family, Series]] ++ Quantum,
    #key_v1{ast = AST}.
    
make_lk(Key) ->
    #key_v1{ast = [#param_v1{name = X#riak_field_v1.name} || X <- Key]}.

is_valid_ddl(#ddl_v1{fields = Fs}) ->
    Fs2 = lists:sort([X#riak_field_v1.name || X <- Fs]),
    _IsValid = case lists:usort(Fs2) of
                   Fs2 -> true;
                   _   -> false
end.

make_valid_create_table(#ddl_v1{table         = Tb,
                                fields        = Fs,
                                partition_key = PK,
                                local_key     = LK}) ->
    "CREATE TABLE " ++ binary_to_list(Tb) ++ " (" ++ make_fields(Fs) ++ "PRIMARY KEY ((" ++ pk_to_sql(PK) ++ "), " ++ lk_to_sql(LK) ++ "))".

make_fields(Fs) ->
    make_f2(Fs, []).

make_f2([], Acc) ->
    lists:flatten(lists:reverse(Acc));
make_f2([#riak_field_v1{name    = Nm,
                       type     = Ty,
                       optional = IsOpt} | T], Acc) ->
    Args = [
            binary_to_list(Nm),
            atom_to_list(Ty)
           ] ++ case IsOpt of
                    true  -> [];
                    false -> ["not null"]
                end,
    NewAcc = string:join(Args, " ") ++ ", ",
    make_f2(T, [NewAcc | Acc]).

pk_to_sql(#key_v1{ast = [Fam, Series, TS]}) ->
    string:join([binary_to_list(X#param_v1.name) || X <- [Fam, Series]] ++ [make_q(TS)], ", ").

make_q(#hash_fn_v1{mod  = riak_ql_quanta,
                   fn   = quantum,
                   args = Args,
                   type = timestamp}) ->
              [#param_v1{name = Nm}, Unit, No] = Args,
    _Q = "quantum(" ++ string:join([binary_to_list(Nm), integer_to_list(No), "'" ++ atom_to_list(Unit) ++ "'"], ", ") ++ ")".

lk_to_sql(LK) ->
    string:join([binary_to_list(X#param_v1.name) || X <- LK#key_v1.ast], ", ").

-endif.
