-module(riak_kv_index_hashtree_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_index_hashtree_orig).

%% @doc Perform a delayed compare, which delays the receipt of a
%%      message.
delayed_compare(_IndexN, _Remote, _AccFun, _TreePid) ->
    timer:sleep(1000000),
    [].

%% @doc When attempting to get the lock on a hashtree, return the
%%      not_built atom which means the tree has not been computed yet.
not_built(_TreePid, _Type) ->
    not_built.

%% @doc When attempting to get the lock on a hashtree, return the
%%      already_locked atom which means the tree is locked by another
%%      process.
already_locked(_TreePid, _Type) ->
    already_locked.
