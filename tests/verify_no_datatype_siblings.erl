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

%% This test was written in response to a specific Riak bug found
%% in September 2016. Doing a `get` on a datatype should always
%% cause any siblings to be merged prior to returning an object,
%% but a scenario was found that broke that invariant.
%%
%% Normally datatype values should never have dots in their metadata,
%% but the function that removes dots before writing such values
%% was only called when merging two different objects. The very first
%% write of a new datatype would thus include a dot, as there would be
%% no existing object with which to perform a merge. Additionally, the
%% merge functions in `riak_object` depend on the absence of dots to
%% distinguish datatypes from other values, so if siblings were
%% present and one of the siblings was a freshly written datatype,
%% we would not merge the two values and instead return siblings
%% on a subsequent `get`.
%%
%% NB This is not an issue when using the `fetch_type` client functions,
%% which are normally used to retrieve datatypes; those functions
%% cause a merge to be performed regardless of the presence of dots.
%% To reproduce this bug we must use a plain `get` operation.
%%
%% I arbitrarily chose HLLs for this test just because it's the newest
%% datatype and hasn't had a lot of testing yet, but this could just
%% as well be implemented with any of the CRDTs we have available to us.

-module(verify_no_datatype_siblings).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HLL_TYPE, <<"hlls">>).
-define(BUCKET, {?HLL_TYPE, <<"testbucket">>}).
-define(KEY, <<"xyzzy">>).

confirm() ->
    {N1, N2} = setup(),

    PBC1 = rt:pbc(N1),
    PBC2 = rt:pbc(N2),

    lager:info("Partition cluster in two so we can do conflicting writes"),
    PartInfo = rt:partition([N1], [N2]),

    write_siblings(PBC1, PBC2),

    lager:info("Heal partition"),
    ?assertEqual(ok, rt:heal(PartInfo)),

    verify_no_siblings(PBC1),

    pass.

setup() ->
    Nodes = [N1, N2] = rt:build_cluster(2),

    ok = rt:create_activate_and_wait_for_bucket_type(Nodes, ?HLL_TYPE,
                                                     [{datatype, hll}, {hll_precision, 16}]),

    {N1, N2}.

write_siblings(PBC1, PBC2) ->
    lager:info("Write to one side of the partition"),
    ?assertEqual(ok, do_write(PBC1, <<"plugh">>)),

    lager:info("Write to other side of the partition"),
    ?assertEqual(ok, do_write(PBC2, <<"y2">>)).

do_write(PBC, Value) ->
    NewHLL = riakc_hll:new(),
    riakc_pb_socket:update_type(PBC, ?BUCKET, ?KEY,
                                riakc_hll:to_op(riakc_hll:add_element(Value, NewHLL))).

verify_no_siblings(PBC) ->
    {ok, Obj} = do_read(PBC),
    lager:info("Got object ~p", [Obj]),

    NumSiblings = length(riakc_obj:get_values(Obj)),
    ?assertEqual(1, NumSiblings).

do_read(PBC) ->
    riakc_pb_socket:get(PBC, ?BUCKET, ?KEY).
