%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.
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
%% @doc Verify some MapReduce with a multi-stage pre-reduce and m/r stage
%%
-module(mapred_index_peoplesearch).
-behavior(riak_test).
-export([
         %% riak_test api
         confirm/0
        ]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

-define(BUCKET, <<"2ibucket">>).
-define(FOO, <<"foo">>).
-define(OBJECTS, 2000).

confirm() ->
    Nodes = rt:build_cluster(3),

    SW = os:timestamp(),
    load_test_data(Nodes, ?OBJECTS),
    lager:info("Loaded ~w objects in ~w ms",
                [?OBJECTS, timer:now_diff(os:timestamp(), SW)/1000]),

    lager:info("Generate person to find"),
    SpecialKey = int_to_key(999999),
    PeopleIdx =
        complete_peoplesearch_index("SMINOKOWSKI",
                                    "19391219",
                                    ["S250", "S000", "J500"],
                                    "1 Acacia Avenue, Manchester"),
    lager:info("People index ~s", [PeopleIdx]),
    put_an_object(rt:pbc(hd(Nodes)),
                    SpecialKey,
                    "Special person to find",
                    [{"psearch_bin", PeopleIdx}]),
    {ok, [SminObj]} =
        rpcmr(
            hd(Nodes),
            {index, 
                ?BUCKET,
                    <<"psearch_bin">>,
                    <<"SM">>, <<"SM~">>,
                    true,
                    "^SM[^\|]*KOWSKI\\|",
                    % query the range of all family names beginning with SM
                    % but apply an additional regular expression to filter for
                    % only those names ending in *KOWSKI 
                    [{riak_kv_index_prereduce,
                            extract_regex,
                            {term,
                                [dob, givennames, address],
                                this,
                                "[^\|]*\\|(?<dob>[0-9]{8})\\|(?<givennames>[A-Z0-9]+)\\|(?<address>.*)"}},
                        % Use a regular expresssion to split the term into three different terms
                        % dob, givennames and address.  As Keep=this, only those three KV pairs will
                        % be kept in the indexdata to the next stage, and the original `term` attribute
                        % will be dropped
                        {riak_kv_index_prereduce,
                            apply_range,
                            {dob,
                                all,
                                <<"0">>,
                                <<"19401231">>}},
                        % Filter out all dates of births up to an including the last day of 1940.
                        % Need to keep all terms as givenname and address filters still to be
                        % applied
                        {riak_kv_index_prereduce,
                            apply_regex,
                            {givennames,
                                all,
                                "S000"}},
                        % Use a regular expression to only include those results with a given name
                        % which sounds like Sue
                        {riak_kv_index_prereduce,
                            extract_encoded,
                            {address,
                                address_sim,
                                this}},
                        % This converts the base64 encoded hash back into a binary, and only `this`
                        % is required now - so only the [{address_sim, Hash}] will be in the
                        % IndexData downstream
                        {riak_kv_index_prereduce,
                            extract_hamming,
                            {address_sim,
                                address_distance,
                                this,
                                riak_kv_index_prereduce:simhash(<<"Acecia Avenue, Manchester">>)}},
                        % This generates a new projected attribute `address_distance` which
                        % is the hamming distance between the query and the indexed address
                        {riak_kv_index_prereduce,
                            log_identity,
                            address_distance},
                        % This adds a log for troubleshooting - the term passed to logidentity
                        % is the projected attribute to log (`key` can be used just to log
                        % the key
                        {riak_kv_index_prereduce,
                            apply_range,
                            {address_distance,
                                this,
                                0,
                                50}}
                        % Filter out any result where the hamming distance to the query
                        % address is more than 50
                            ]},
            [{reduce, {modfun, riak_kv_mapreduce, reduce_index_min}, {address_distance, 10}, false},
                % Restricts the number of results to be fetched to the ten matches with the
                % smallest hamming distance to the queried address
                {map, {modfun, riak_kv_mapreduce, map_identity}, none, true}
                % Fetch all the matching objects
            ]),
    ?assertMatch(SpecialKey, riak_object:key(SminObj)),

    pass.

load_test_data(Nodes, Count) ->
    PBPid = rt:pbc(hd(Nodes)),
    [put_an_object(PBPid, N) || N <- lists:seq(0, Count)].

rpcmr(Node, Inputs, Query) ->
    SW = os:timestamp(),
    rpc:call(Node, application, set_env, [riak_kv, pipe_log_level, [info, warn, error]]),
    R = rpc:call(Node, riak_kv_mrc_pipe, mapred, [Inputs, Query]),
    lager:info("Map/Reduce query complete in ~w ms",
        [timer:now_diff(os:timestamp(), SW)/1000]),
    R.

put_an_object(Pid, N) ->
    Key = int_to_key(N),
    Data = lists:flatten(io_lib:format("data ~p", [N])),
    BinIndex = int_to_field1_bin(N),
    Indexes = [{"field1_bin", BinIndex},
               {"field2_int", N},
               {"field2_bin", <<0:8/integer, N:32/integer, 0:8/integer>>},
               {"psearch_bin", generate_peoplesearch_index()}
              ],
    put_an_object(Pid, Key, Data, Indexes).

put_an_object(Pid, Key, Data, Indexes) when is_list(Indexes) ->
    MetaData = dict:from_list([{<<"index">>, Indexes}]),
    Robj0 = riakc_obj:new(?BUCKET, Key),
    Robj1 = riakc_obj:update_value(Robj0, Data),
    Robj2 = riakc_obj:update_metadata(Robj1, MetaData),
    riakc_pb_socket:put(Pid, Robj2).

generate_peoplesearch_index() ->
    SurnameInt = rand:uniform(531),
    GivenNameCount = rand:uniform(3),
    RandomStreet = rand:uniform(10),
    RandomHouseNumber = rand:uniform(50),
    RandomTown = rand:uniform(6),
    RandomDoB =
        io_lib:format("~4..0B~2..0B~2..0B",
                        [1920 + rand:uniform(80),
                            rand:uniform(12),
                            rand:uniform(28)]),
    Surname = 
        element(1, hd(lists:dropwhile(fun({_T, I}) -> SurnameInt > I end, surname_list()))),
    GivenNames =
        lists:map(fun(_I) -> 
                        lists:nth(
                            rand:uniform(
                                length(givenname_list()) - 1) + 1, givenname_list()) end,
                lists:seq(1, GivenNameCount)),
    Address =
        io_lib:format("~w ~s, ~s",
                                [RandomHouseNumber,
                                    lists:nth(RandomStreet, streetname_list()),
                                    lists:nth(RandomTown, town_list())]),
    complete_peoplesearch_index(Surname, RandomDoB, GivenNames, Address).

complete_peoplesearch_index(Surname, DoB, GivenNames, Address) ->
    GNCodes =
        lists:foldl(fun(N, Acc) -> Acc ++ N end, "", GivenNames),
    AddressHash = base64:encode(riak_kv_index_prereduce:simhash(list_to_binary(Address))),
    iolist_to_binary(Surname ++ "|" ++ DoB ++ "|" ++ GNCodes ++ "|" ++ AddressHash).


int_to_key(N) ->
    list_to_binary(io_lib:format("obj~8..0B", [N])).

int_to_field1_bin(N) ->
    list_to_binary(io_lib:format("val~8..0B", [N])).

surname_list() ->
    [{"Smith", 126}, {"Jones", 201}, {"Taylor", 260}, {"Brown",	316},
        {"Williams", 355}, {"Wilson", 394}, {"Johnson", 431}, {"Davies", 467},
        {"Robinson", 499}, {"Wright", 531}].

givenname_list() ->
    ["O410", "A540", "E540", "I240", "A100", "J220", "I214", "L400", "E400", "M000"].

streetname_list() ->
    ["High Street", "Station Road", "Main Street", "Park Road", "Church Road",
        "Church Street", "London Road", "Victoria Road", "Green Lane",
        "Manor Road", "Church Lane"].

town_list() ->
    ["Leeds", "Liverpool", "Fulham", "Sheffield", "Manchester", "Wolverhampton"].