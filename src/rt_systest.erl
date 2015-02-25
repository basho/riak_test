%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2014 Basho Technologies, Inc.
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
-module(rt_systest).
-include("rt.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([read/2,
         read/3,
         read/5,
         read/6,
         read/7,
         write/2,
         write/3,
         write/5,
         write/6,
         verify_systest_value/4]).

write(Node, Size) ->
    write(Node, Size, 2).

write(Node, Size, W) ->
    write(Node, 1, Size, <<"systest">>, W).

write(Node, Start, End, Bucket, W) ->
    write(Node, Start, End, Bucket, W, <<>>).

%% @doc Write (End-Start)+1 objects to Node. Objects keys will be
%% `Start', `Start+1' ... `End', each encoded as a 32-bit binary
%% (`<<Key:32/integer>>'). Object values are the same as their keys.
%%
%% The return value of this function is a list of errors
%% encountered. If all writes were successful, return value is an
%% empty list. Each error has the form `{N :: integer(), Error :: term()}',
%% where N is the unencoded key of the object that failed to store.
write(Node, Start, End, Bucket, W, CommonValBin)
  when is_binary(CommonValBin) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    F = fun(N, Acc) ->
                Obj = riak_object:new(Bucket, <<N:32/integer>>,
                                      <<N:32/integer, CommonValBin/binary>>),
                try C:put(Obj, W) of
                    ok ->
                        Acc;
                    Other ->
                        [{N, Other} | Acc]
                catch
                    What:Why ->
                        [{N, {What, Why}} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

read(Node, Size) ->
    read(Node, Size, 2).

read(Node, Size, R) ->
    read(Node, 1, Size, <<"systest">>, R).

read(Node, Start, End, Bucket, R) ->
    read(Node, Start, End, Bucket, R, <<>>).

read(Node, Start, End, Bucket, R, CommonValBin)
  when is_binary(CommonValBin) ->
    read(Node, Start, End, Bucket, R, CommonValBin, false).

%% Read and verify the values of objects written with
%% `systest_write'. The `SquashSiblings' parameter exists to
%% optionally allow handling of siblings whose value and metadata are
%% identical except for the dot. This goal is to facilitate testing
%% with DVV enabled because siblings can be created internally by Riak
%% in cases where testing with DVV disabled would not. Such cases
%% include writes that happen during handoff when a vnode forwards
%% writes, but also performs them locally or when a put coordinator
%% fails to send an acknowledgment within the timeout window and
%% another put request is issued.
read(Node, Start, End, Bucket, R, CommonValBin, SquashSiblings)
  when is_binary(CommonValBin) ->
    rt:wait_for_service(Node, riak_kv),
    {ok, C} = riak:client_connect(Node),
    lists:foldl(read_fold_fun(C, Bucket, R, CommonValBin, SquashSiblings),
                [],
                lists:seq(Start, End)).

read_fold_fun(C, Bucket, R, CommonValBin, SquashSiblings) ->
    fun(N, Acc) ->
            GetRes = C:get(Bucket, <<N:32/integer>>, R),
            Val = object_value(GetRes, SquashSiblings),
            update_acc(value_matches(Val, N, CommonValBin), Val, N, Acc)
    end.

object_value({error, _}=Error, _) ->
    Error;
object_value({ok, Obj}, SquashSiblings) ->
    object_value(riak_object:value_count(Obj), Obj, SquashSiblings).

object_value(1, Obj, _SquashSiblings) ->
    riak_object:get_value(Obj);
object_value(_ValueCount, Obj, false) ->
    riak_object:get_value(Obj);
object_value(_ValueCount, Obj, true) ->
    lager:debug("Siblings detected for ~p:~p", [riak_object:bucket(Obj), riak_object:key(Obj)]),
    Contents = riak_object:get_contents(Obj),
    case lists:foldl(fun sibling_compare/2, {true, undefined}, Contents) of
        {true, {_, _, _, Value}} ->
            lager:debug("Siblings determined to be a single value"),
            Value;
        {false, _} ->
            {error, siblings}
    end.

sibling_compare({MetaData, Value}, {true, undefined}) ->
    Dot = case dict:find(<<"dot">>, MetaData) of
              {ok, DotVal} ->
                  DotVal;
              error ->
                  {error, no_dot}
          end,
    VTag = dict:fetch(<<"X-Riak-VTag">>, MetaData),
    LastMod = dict:fetch(<<"X-Riak-Last-Modified">>, MetaData),
    {true, {element(2, Dot), VTag, LastMod, Value}};
sibling_compare(_, {false, _}=InvalidMatch) ->
    InvalidMatch;
sibling_compare({MetaData, Value}, {true, PreviousElements}) ->
    Dot = case dict:find(<<"dot">>, MetaData) of
              {ok, DotVal} ->
                  DotVal;
              error ->
                  {error, no_dot}
          end,
    VTag = dict:fetch(<<"X-Riak-VTag">>, MetaData),
    LastMod = dict:fetch(<<"X-Riak-Last-Modified">>, MetaData),
    ComparisonElements = {element(2, Dot), VTag, LastMod, Value},
    {ComparisonElements =:= PreviousElements, ComparisonElements}.

value_matches(<<N:32/integer, CommonValBin/binary>>, N, CommonValBin) ->
    true;
value_matches(_WrongVal, _N, _CommonValBin) ->
    false.
update_acc(true, _, _, Acc) ->
    Acc;
update_acc(false, {error, _}=Val, N, Acc) ->
    [{N, Val} | Acc];
update_acc(false, Val, N, Acc) ->
    [{N, {wrong_val, Val}} | Acc].

verify_systest_value(N, Acc, CommonValBin, Obj) ->
    Values = riak_object:get_values(Obj),
    Res = [begin
               case V of
                   <<N:32/integer, CommonValBin/binary>> ->
                       ok;
                   _WrongVal ->
                       wrong_val
               end
           end || V <- Values],
    case lists:any(fun(X) -> X =:= ok end, Res) of
        true ->
            Acc;
        false ->
            [{N, {wrong_val, hd(Values)}} | Acc]
    end.

