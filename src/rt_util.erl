-module(rt_util).

-type error() :: {error(), term()}.
-type result() :: ok | error().

-export_type([error/0,
              result/0]).
