%% -------------------------------------------------------------------
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
%%% @doc
%%% riak_test for vnode capability controlling HEAD request use in get
%%% fsm
%%% @end

-module(verify_head_capability).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, <<"test-bucket">>).

confirm() ->
    %% Create a mixed cluster of current and previous
    %% Create a PB client
    %% Do some puts so we have some data
    %% do GETs, they should all be _gets_ at the vnode
    %% Upgrade nodes to current
    %% Do some GETS
    %% check that HEADs happen after cap is negotiated

    %% Use redbug tracing to detect calls to riak_kv_vnode_head after
    %% upgrade
    rt_redbug:set_tracing_applied(true),

    [Prev1, Prev2, _Curr1, _Curr2] = Nodes = rt:build_cluster([previous, previous, current, current]),

    Res = rt:systest_write(Prev1, 1, 50, ?BUCKET, 2),

    ?assertEqual([], Res),
    {ok, CWD} = file:get_cwd(),
    FnameBase = "rt_vhc",
    FileBase = filename:join([CWD, FnameBase]),

    PrevTrcFile = FileBase ++ "Before",
    CurrTrcFile = FileBase ++ "After",

    lager:info("STARTUNG TRACE"),

    [redbug:start("riak_kv_vnode:head/3",
                  default_trace_options() ++
                      [{target, Node},
                       {arity, true},
                       {print_file, PrevTrcFile}]) || Node <- Nodes],
    ReadRes = rt:systest_read(Prev1, 1, 50, ?BUCKET, 2),

    redbug:stop(),

    assert_head_cnt(0, PrevTrcFile),

    ?assertEqual([], ReadRes),

    lager:info("upgrade all to current"),

    rt:upgrade(Prev1, current),
    rt:upgrade(Prev2, current),

    ?assertEqual(ok, rt:wait_until_capability(Prev1, {riak_kv, get_request_type}, head)),

    lager:info("TRACE AGAIN"),

    [redbug:start("riak_kv_vnode:head/3",
                  default_trace_options() ++
                      [{target, Node},
                       {arity, true}, {print_file, CurrTrcFile}]) || Node <- Nodes],

    %%    rt_redbug:trace(Nodes, ["riak_kv_vnode:head/3"], [{arity, true}, {file, FileBase ++ "Curr"}]),
    ReadRes2 = rt:systest_read(Prev1, 1, 50, ?BUCKET, 2),

    redbug:stop(),

    %% one per read (should we count the handle_commands instead?)
    assert_head_cnt(50, CurrTrcFile),

    ?assertEqual([], ReadRes2),

    %% Delete trace files
    [file:delete(F) || F <- [PrevTrcFile, CurrTrcFile]],
    pass.


assert_head_cnt(ExpectedHeadCnt, File) ->
    Actual = head_cnt(File),
    ?assertEqual(ExpectedHeadCnt, Actual).

head_cnt(File) ->
    %% @TODO read the  and count the number of HEADs in it
    lager:info("checking ~p", [File]),
    {ok, FileData} = file:read_file(File),
    count_matches(re:run(FileData, "{riak_kv_vnode,head,3}", [global])).

count_matches(nomatch) ->
    0;
count_matches({match, Matches}) ->
    length(Matches).


default_trace_options() ->
    [
     %% give a nice long timeout so we get all messages (1 minute)
     {time,(60*1000)},
     %% raise the threshold for the number of traces that can be received by
     %% redbug before tracing is suspended
     {msgs, 1000},
     %% print milliseconds
     {print_msec, true}
    ].
