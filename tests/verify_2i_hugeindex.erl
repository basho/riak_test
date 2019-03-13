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
-module(verify_2i_hugeindex).
-behavior(riak_test).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-import(secondary_index_tests, [int_to_key/1,
                                stream_pb/3,
                                http_query/3]).
-define(BUCKET, <<"2ibucket">>).
-define(BAR, <<"bar">>).
-define(FOO, <<"foo">>).
-define(Q_OPTS, [{return_terms, true}]).
-define(BUFFER_SIZE_CONF, [{webmachine, [{recbuf, 12288}]}]).

%% Prior to Riak 2.1 HTTP headers could be of arbitrary size, and hence index 
%% entries.  In 2.1 a change to mochiweb constarined http headers to be only as
%% big as the receive buffer (default 8KB).
%%
%% From 2.9, this constraint still exists, but the receive buffer is now
%% configurable.  This is a test of that behaviour (and also a change to the
%% HTTP API to return 431 to indicate that a header too large, rather than
%% returning a non-specific error in this case).

confirm() ->
    inets:start(),

    rt:set_advanced_conf(all, ?BUFFER_SIZE_CONF),
    Nodes = rt:build_cluster(3),
    ?assertEqual(ok, (rt:wait_until_nodes_ready(Nodes))),

    RiakHttp = rt:httpc(hd(Nodes)),
    RiakUrl = rt:http_url(hd(Nodes)),
    PBPid = rt:pbc(hd(Nodes)),

    ok = put_an_object(RiakHttp, 1, 1024),
    ok = put_an_object(RiakHttp, 2, 2048),
    ok = put_an_object(RiakHttp, 3, 4096),

    ok = put_an_object(RiakHttp, 4, 8192),
        % This index entry is larger than the standard 8KB receive buffer, but
        % smaller than the configured receive buffer

    {error, {ok, "431", _H, _B}} = put_an_object(RiakHttp, 5, 16384),
        % This object is larger than the configured receive buffer
        % so should get 431 error to indicate invalid size

    assertEqual(RiakUrl, PBPid, 4, {<<"field1_bin">>, ?BAR, ?FOO}, ?Q_OPTS, results),
    assertEqual(RiakUrl, PBPid, 4, {<<"field2_int">>, 1, 4}, ?Q_OPTS, results),

    riakc_pb_socket:stop(PBPid),
    pass.

%% Check the PB result against our expectations
%% and the non-streamed HTTP
assertEqual(Http, PB, Expected, Query, Opts, ResultKey) ->
    {ok, PBRes} = stream_pb(PB, Query, Opts),
    PBKeys = proplists:get_value(ResultKey, PBRes, []),
    HTTPRes = http_query(Http, Query, Opts),
    HTTPResults0 = proplists:get_value(atom_to_binary(ResultKey, latin1), HTTPRes, []),
    HTTPResults = decode_http_results(ResultKey, HTTPResults0),
    ?assertEqual(Expected, length(PBKeys)),
    ?assertEqual(Expected, length(HTTPResults)).

decode_http_results(keys, Keys) ->
    Keys;
decode_http_results(results, Results) ->
    decode_http_results(Results, []);

decode_http_results([], Acc) ->
    lists:reverse(Acc);
decode_http_results([{struct, [Res]} | Rest ], Acc) ->
    decode_http_results(Rest, [Res | Acc]).


%% general 2i utility
put_an_object(HTTPc, N, IndexSize) ->
    Key = int_to_key(N),
    Data = io_lib:format("data~p", [N]),
    BinIndex = generate_big_value(IndexSize, ?BAR),
    Indexes = [{"field1_bin", BinIndex},
               {"field2_int", N}
              ],
    put_an_object(HTTPc, Key, Data, Indexes).

put_an_object(HTTPc, Key, Data, Indexes) when is_list(Indexes) ->
    lager:info("Putting object ~p", [Key]),
    MetaData = dict:from_list([{<<"index">>, Indexes}]),
    Robj0 = riakc_obj:new(?BUCKET, Key),
    Robj1 = riakc_obj:update_value(Robj0, Data),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    rhc:put(HTTPc, Robj2).

generate_big_value(0, BigVal) ->
    lager:info("Generated big index value of size ~w", [byte_size(BigVal)]),
    BigVal;
generate_big_value(N, <<BigValAcc/binary>>) ->
    M = min(N, 8),
    generate_big_value(N - M, <<BigValAcc/binary, "aaaaaaaa">>).