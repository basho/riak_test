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
-module(ts_riak_shell_basic_sql).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

-define(DONT_INCREMENT_PROMPT, false).

%% we cant run the test in this process as it receives various messages
%% and the running test interprets then as being messages to the shell
confirm() ->
    {Nodes, Conn} = ts_util:cluster_and_connect(multiple),
    lager:info("Built a cluster of ~p~n", [Nodes]),
    Got1 = create_table_test(),
    Result1 = ts_util:assert("Create Table", pass, Got1),
    Got2 = query_table_test(Conn),
    Result2 = ts_util:assert("Query Table", pass, Got2),
    ts_util:results([
        Result1,
        Result2
    ]),
    pass.

create_table_test() ->
    State = riak_shell_test_util:shell_init(),
    lager:info("~n~nStart running the command set-------------------------", []),
    CreateTable = lists:flatten(io_lib:format("~s;", [ts_util:get_ddl(small)])),
    Describe = io_lib:format("+-----------+---------+-------+-----------+---------+~n"
    "|  Column   |  Type   |Is Null|Primary Key|Local Key|~n"
    "+-----------+---------+-------+-----------+---------+~n"
    "| myfamily  | varchar | false |     1     |    1    |~n"
    "| myseries  | varchar | false |     2     |    2    |~n"
    "|   time    |timestamp| false |     3     |    3    |~n"
    "|  weather  | varchar | false |           |         |~n"
    "|temperature| double  | true  |           |         |~n"
    "+-----------+---------+-------+-----------+---------+", []),
    Cmds = [
            %% 'connection prompt on' means you need to do unicode printing and stuff
           {run,
            "show_version;"},
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
    Result = riak_shell_test_util:run_commands(Cmds, State, ?DONT_INCREMENT_PROMPT),
    lager:info("Result is ~p~n", [Result]),
    lager:info("~n~n------------------------------------------------------", []),
    application:stop(riak_shell),
    Result.

query_table_test(Conn) ->
    %% Throw some tests data out there
    Data = ts_util:get_valid_select_data(),
    ok = riakc_ts:put(Conn, ts_util:get_default_bucket(), Data),
    SQL = "select time, weather, temperature from GeoCheckin where myfamily='family1' and myseries='seriesX' and time > 0 and time < 1000",
    Expected = query(Conn, SQL),
    Select = lists:flatten(io_lib:format("~s;", [SQL])),
    State = riak_shell_test_util:shell_init(),
    lager:info("~n~nStart running the command set-------------------------", []),
    Cmds = [
        %% 'connection prompt on' means you need to do unicode printing and stuff
        {run,
            "show_version;"},
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
    Result = riak_shell_test_util:run_commands(Cmds, State, ?DONT_INCREMENT_PROMPT),
    lager:info("Result is ~p~n", [Result]),
    lager:info("~n~n------------------------------------------------------", []),
    application:stop(riak_shell),
    Result.

%% Stolen from the innards of riak_shell
query(Conn, SQL) ->
    case riakc_ts:query(Conn, SQL) of
        {error, {ErrNo, Binary}} ->
            io_lib:format("Error (~p): ~s", [ErrNo, Binary]);
        {Header, Rows} ->
            Hdr = [binary_to_list(X) || X <- Header],
            Rs = [begin
                      Row = tuple_to_list(RowTuple),
                      [riak_shell_util:to_list(X) || X <- Row]
                  end || RowTuple <- Rows],
            case {Hdr, Rs} of
                {[], []} ->
                    "";
                _ ->
                    clique_table:autosize_create_table(Hdr, Rs)
            end
    end.

