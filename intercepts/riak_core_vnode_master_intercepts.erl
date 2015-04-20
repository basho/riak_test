-module(riak_core_vnode_master_intercepts).
-compile(export_all).
-include("intercept.hrl").

-record(riak_core_fold_req_v2, {
    foldfun :: fun(),
    acc0 :: term(),
    forwardable :: boolean(),
    opts = [] :: list()}).

-define(M, riak_core_vnode_master_orig).


stop_vnode_after_bloom_fold_request_succeeds(IndexNode, Req, Sender, VMaster) ->
    ?I_INFO("Intercepting riak_core_vnode_master:command_returning_vnode"),
    ReqFun = Req#riak_core_fold_req_v2.foldfun,

    case (ReqFun == fun riak_repl_aae_source:bloom_fold/3 orelse ReqFun == fun riak_repl_keylist_server:bloom_fold/3) of
        true ->
            random:seed(erlang:now()),
            case random:uniform(10) of
                5 ->
                    %% Simulate what happens when a VNode completes handoff between command_returning_vnode
                    %% and the fold attempting to start - other attempts to intercept and slow
                    %% certain parts of Riak to invoke the particular race condition were unsuccessful
                    ?I_INFO("Replaced VNode with spawned funciton in command_returning_vnode"),
                    VNodePid = spawn(fun() -> timer:sleep(100),
                                   exit(normal)
                          end),
                    {ok, VNodePid};
                _ ->
                    ?M:command_return_vnode_orig(IndexNode, Req, Sender, VMaster)
            end;
        false -> ?M:command_return_vnode_orig(IndexNode, Req, Sender, VMaster)
    end.