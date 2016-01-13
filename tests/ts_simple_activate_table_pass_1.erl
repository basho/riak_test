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

-module(ts_simple_activate_table_pass_1).

-behavior(riak_test).

-include_lib("eunit/include/eunit.hrl").

-export([confirm/0]).

confirm() ->
    {[Node|_], Client} = ts_util:cluster_and_connect(single),
    ok = confirm_create(
           Client, Node, make_ddl("t1"), "t1"),
    ok = confirm_not_activated_fail(
           Client, Node, make_ddl("t2"), "t2"),
    pass.


confirm_create(Client, Node, DDL, Table) ->
    Res1 = create_bucket_type(Node, DDL, Table),
    ?assertMatch({ok, _}, Res1),

    Res2 = activate_bucket_type(Node, Table, _Retries = 5),
    ?assertMatch({ok, _}, Res2),

    Res3 = ts_util:single_query(Client, make_query(Table)),
    ?assertMatch({[], []}, Res3),
    ok.

confirm_not_activated_fail(Client, Node, DDL, Table) ->
    Res1 = create_bucket_type(Node, DDL, Table),
    ?assertMatch({ok, _}, Res1),

    %% Res2 = activate_bucket_type(Node, Table, 5)
    %% ?assertMatch({ok, _}, Res2),

    Res3 = ts_util:single_query(Client, make_query(Table)),
    ?assertMatch({error, {1019, _}}, Res3),

    Res4 = riakc_ts:put(Client, Table, [[<<"a">>, <<"b">>, 2]]),
    ?assertMatch({error, {1019, _}}, Res4),

    Res4 = riakc_ts:get(Client, Table, [<<"a">>, <<"b">>, 2], []),
    ?assertMatch({error, {1019, _}}, Res4),

    Res5 = riakc_ts:delete(Client, Table, [<<"a">>, <<"b">>, 2], []),
    ?assertMatch({error, {1019, _}}, Res5),

    ok.


make_ddl(T) ->
    "create table " ++ T ++ " ("
        " a varchar   not null,"
        " b varchar   not null,"
        " c timestamp not null,"
        " primary key ((a, b, quantum(c, 1, m)), a, b, c))".

make_query(T) ->
    "select * from " ++ T ++ " where a = 'a' and b = 'b' and c > 1 and c < 3".


%% copied from ts_util to ensure we explicitly use riak-admin to
%% create and then, separately, activate the bucket (and not, as we
%% may deliberately decide, to accomplish both in one call via query).
create_bucket_type(Node, DDL, Bucket) ->
    Props = lists:flatten(
              io_lib:format("{\\\"props\\\": {\\\"n_val\\\": 3, \\\"table_def\\\": \\\"~s\\\"}}",
                            [DDL])),
    rt:admin(Node, ["bucket-type", "create", Bucket, Props]).

activate_bucket_type(Node, Bucket, Retries) ->
    {ok, Msg} = Result = rt:admin(Node, ["bucket-type", "activate", Bucket]),
    %% Look for a successful message
    case string:str(Msg, "has been activated") of
        0 ->
            lager:error("Could not activate bucket type. Retrying. Result = ~p", [Result]),
            case Retries of
                0 -> Result;
                _ -> timer:sleep(1000),
                     activate_bucket_type(Node, Bucket, Retries - 1)
            end;
        _ -> Result
    end.
