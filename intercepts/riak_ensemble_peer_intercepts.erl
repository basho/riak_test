-module(riak_ensemble_peer_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_ensemble_peer_orig).

-define(INTERCEPT_TAB, intercept_leader_tick_counts).

count_leader_ticks(State) ->
    Result = ?M:leader_tick_orig(State),
    case ets:lookup(?INTERCEPT_TAB, self()) of
        [] ->
            ets:insert(?INTERCEPT_TAB, {self(), 1});
        _ ->
            ets:update_counter(?INTERCEPT_TAB, self(), 1)
    end,
    Result.
