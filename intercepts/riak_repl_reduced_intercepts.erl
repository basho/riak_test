-module(riak_repl_reduced_intercepts).

-include("intercept.hrl").

-export([recv_get_report/0, recv_get_report/1, report_mutate_get/1,
    register_as_target/0, get_all_reports/0, get_all_reports/1]).
-export([report_mutate_put/5, recv_put_report/0, recv_put_report/1,
    put_all_reports/0, put_all_reports/1]).

-define(M, riak_repl_reduced_orig).

report_mutate_get(InObject) ->
    Node = node(),
    Pid = self(),
    TargetNode = 'riak_test@127.0.0.1',
    TargetProcess = reduced_intercept_target,
    {TargetProcess, TargetNode} ! {report_mutate_get, Node, Pid, InObject},
    ?M:mutate_get_orig(InObject).

report_mutate_put(InMeta, InVal, RevMeta, Obj, Props) ->
    Node = node(),
    Pid = self(),
    TargetNode = 'riak_test@127.0.0.1',
    TargetProcess = reduced_intercept_target,
    {TargetProcess, TargetNode} ! {report_mutate_put, Node, Pid, InMeta, InVal},
    ?M:mutate_put_orig(InMeta, InVal, RevMeta, Obj, Props).

register_as_target() ->
    Self = self(),
    case whereis(reduced_intercept_target) of
        Self ->
            true;
        undefined ->
            register(reduced_intercept_target, Self);
        _NotSelf ->
            unregister(reduced_intercept_target),
            register(reduced_intercept_target, Self)
    end.

recv_get_report() ->
    recv_get_report(5000).

recv_get_report(Timeout) ->
    receive
        {report_mutate_get, Node, Pid, InObject} ->
            {Node, Pid, InObject}
    after Timeout ->
        {error, timeout}
    end.

get_all_reports() ->
    get_all_reports(5000).

get_all_reports(Timeout) ->
    get_all_reports(Timeout, []).

get_all_reports(Timeout, Acc) ->
    case recv_get_report(Timeout) of
        {error, timeout} ->
            lists:reverse(Acc);
        Report ->
            get_all_reports(Timeout, [Report | Acc])
    end.

recv_put_report() ->
    recv_put_report(5000).

recv_put_report(Timeout) ->
    receive
        {report_mutate_put, Node, Pid, InMeta, InVal} ->
            {Node, Pid, InMeta, InVal}
    after Timeout ->
        {error, timeout}
    end.

put_all_reports() ->
    put_all_reports(5000).

put_all_reports(Timeout) ->
    put_all_reports(Timeout, []).

put_all_reports(Timeout, Acc) ->
    case recv_put_report(Timeout) of
        {error, timeout} ->
            lists:reverse(Acc);
        Report ->
            put_all_reports(Timeout, [Report | Acc])
    end.

