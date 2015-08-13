-module(init_intercepts).
-compile(export_all).
-include("intercept.hrl").
-define(M, init_orig).

get_status() ->
    {starting, starting}.
