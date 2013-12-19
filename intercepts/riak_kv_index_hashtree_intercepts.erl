-module(riak_kv_index_hashtree_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_index_hashtree_orig).

not_built(_TreePid, _Type) ->
    not_built.
