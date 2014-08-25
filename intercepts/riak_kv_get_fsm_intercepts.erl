-module(riak_kv_get_fsm_intercepts).
-compile(export_all).
-include("intercept.hrl").
-define(M, riak_kv_get_fsm_orig).

%count_init([From, Bucket, Key, Options0, Monitor]) ->
%    ?I_INFO("Counting init call"),
%    gen_server:call({global, overload_proxy}, increment_count, infinity),
%    ?M:init_orig([From, Bucket, Key, Options0, Monitor]).
 
%count_start_link_5(ReqId,Bucket,Key,R,Timeout,From) ->
%    ?I_INFO("sending startlink/5 through proxy"),
%    gen_server:call({global, overload_proxy}, increment_count),
%    ?M:start_link_orig(ReqId,Bucket,Key,R,Timeout,From).

count_start_link_4(From, Bucket, Key, GetOptions) ->
    ?I_INFO("sending startlink/4 through proxy"),
    case ?M:start_link_orig(From, Bucket, Key, GetOptions) of
        {error, overload} ->
            ?I_INFO("riak_kv_get_fsm not started due to overload.");
        {ok, _} ->
            gen_server:cast({global, overload_proxy}, increment_count)
    end.    
