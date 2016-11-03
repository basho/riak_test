%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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
-module(ts_cluster_riak_shell_basic_sql).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-define(DONT_INCREMENT_PROMPT, false).

%% we cant run the test in this process as it receives various messages
%% and the running test interprets then as being messages to the shell
confirm() ->
    {Nodes, Conn} = ts_util:cluster_and_connect(multiple),
    lager:info("Built a cluster of ~p~n", [Nodes]),
    Self = self(),
    _Pid = spawn_link(fun() -> create_table_test(Self) end),
    Got1 = riak_shell_test_util:loop(),
    Result1 = ts_util:assert("Create Table", pass, Got1),
    _Pid2 = spawn_link(fun() -> query_table_test(Self, Conn) end),
    Got2 = riak_shell_test_util:loop(),
    Result2 = ts_util:assert("Query Table", pass, Got2),
    ts_util:results([
        Result1,
        Result2
    ]),
    pass.

create_table_test(Pid) ->
    State = riak_shell_test_util:shell_init(),
    lager:info("~n~nStart running the command set-------------------------", []),
    CreateTable = lists:flatten(io_lib:format("~s;", [ts_util:get_ddl(small)])),
    Describe =
        "Column,Type,Is Null,Primary Key,Local Key,Interval,Unit\n"
        "myfamily,varchar,false,1,1,,\n"
        "myseries,varchar,false,2,2,,\n"
        "time,timestamp,false,3,3,15,m\n"
        "weather,varchar,false,,,,\n"
        "temperature,double,true,,,,\n",
    Cmds = [
            %% 'connection prompt on' means you need to do unicode printing and stuff
            {run,
             "connection_prompt off;"},
            {run,
             "show_cookie;"},
            {run,
             "show_connection;"},
            {run,
             "connect 'dev1@127.0.0.1';"},
            {{match, ""},
                CreateTable},
            {{match, Describe},
             "DESCRIBE GeoCheckin;"}
           ],
    Result = riak_shell_test_util:run_commands(Cmds, State,
                                               ?DONT_INCREMENT_PROMPT),
    lager:info("Result is ~p~n", [Result]),
    lager:info("~n~n------------------------------------------------------", []),
    Pid ! Result.

query_table_test(Pid, Conn) ->
    %% Throw some tests data out there
    Data = ts_util:get_valid_select_data(),
    ok = riakc_ts:put(Conn, ts_util:get_default_bucket(), Data),
    SQL = "select time, weather, temperature from GeoCheckin where myfamily='family1' and myseries='seriesX' and time > 0 and time < 1000",
    Select = lists:flatten(io_lib:format("~s;", [SQL])),
    State = riak_shell_test_util:shell_init(),
    Expected = lists:flatten(query(Conn, SQL)),
    lager:info("~n~nStart running the command set-------------------------", []),
    Cmds = [
        %% 'connection prompt on' means you need to do unicode printing and stuff
        {run,
            "connection_prompt off;"},
        {run,
            "show_cookie;"},
        {run,
            "show_connection;"},
        {run,
            "connect 'dev1@127.0.0.1';"},
        {{match, Expected},
            Select}
    ],
    Result = riak_shell_test_util:run_commands(Cmds, State,
        ?DONT_INCREMENT_PROMPT),
    lager:info("Result is ~p~n", [Result]),
    lager:info("~n~n------------------------------------------------------", []),
    Pid ! Result.

%% Stolen from the innards of riak_shell
format_table({"", _}) ->
    "";
format_table({[[]], _}) ->
    "";
format_table({String, _}) ->
    re:replace(lists:flatten(String), "\r", "", [global,{return,list}]).

query(Conn, SQL) ->
    case riakc_ts:query(Conn, SQL, [], undefined, [{datatypes, true}]) of
        {error, {ErrNo, Binary}} ->
            io_lib:format("Error (~p): ~s", [ErrNo, Binary]);
        {ok, {Header, Rows}} ->
            Rs = [begin
                      Row = lists:zip(tuple_to_list(RowTuple), Header),
                      [{to_list(Name), to_list(X)} || {X, {Name, _Type}} <- Row]
                  end || RowTuple <- Rows],
            Status = clique_status:table(Rs),
            format_table(clique_writer:write([Status], "csv"))
    end.

to_list(A) when is_atom(A)    -> atom_to_list(A);
to_list(B) when is_binary(B)  -> binary_to_list(B);
%% Timestamps when pulled via the Erlang client are integers, but are
%% rendered as strings via riak-shell.
%%
%% We can cheat here: the only integer in the DDL is a timestamp, so
%% any integer we see we can convert to ISO 8601.
to_list(I) when is_integer(I) -> jam_iso8601:to_string(jam:from_epoch(I, 3));
to_list(F) when is_float(F)   -> mochinum:digits(F);
to_list(L) when is_list(L)    -> L.
