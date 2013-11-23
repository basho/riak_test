-module(riak_object_intercepts).
-export([skippable_index_specs/1,
         skippable_diff_index_specs/2]).
-include("intercept.hrl").

skippable_index_specs(Obj) ->
    case app_helper:get_env(riak_kv, skip_index_specs, false) of
        false ->
            riak_object_orig:index_specs_orig(Obj);
        true ->
            []
    end.

skippable_diff_index_specs(Obj, OldObj) ->
    case app_helper:get_env(riak_kv, skip_index_specs, false) of
        false ->
            riak_object_orig:diff_index_specs_orig(Obj, OldObj);
        true ->
            []
    end.
