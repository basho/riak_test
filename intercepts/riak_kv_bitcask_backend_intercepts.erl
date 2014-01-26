-module(riak_kv_bitcask_backend_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_bitcask_backend_orig).

corrupting_put(Bucket, Key, IndexSpecs, Val0, ModState) ->
    Val = 
        case random:uniform(20) of 
            10 -> 
                corrupt_binary(Val0);
            _ -> Val0
        end,
    ?M:put_orig(Bucket, Key, IndexSpecs, Val, ModState).

corrupting_get(Bucket, Key, ModState) ->
    case ?M:get_orig(Bucket, Key, ModState) of
        {ok, BinVal0, UpdModState} ->
            BinVal =
                case random:uniform(20) of 
                    10 ->
                        corrupt_binary(BinVal0);
                    _ -> BinVal0
                end,
            {ok, BinVal, UpdModState};
        Else -> Else
    end.
            
corrupt_binary(O) ->
    crypto:rand_bytes(byte_size(O)).
