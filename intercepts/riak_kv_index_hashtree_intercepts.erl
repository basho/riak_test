-module(riak_kv_index_hashtree_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_index_hashtree_orig).

%% @doc When attempting to get the lock on a hashtree, return the
%%      not_built atom which means the tree has not been computed yet.
not_built(_TreePid, _Type) ->
    not_built.

%% @doc When attempting to get the lock on a hashtree, return the
%%      already_locked atom which means the tree is locked by another
%%      process.
already_locked(_TreePid, _Type) ->
    already_locked.
