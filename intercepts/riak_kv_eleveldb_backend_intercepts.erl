-module(riak_kv_eleveldb_backend_intercepts).
-compile(export_all).
-include("intercept.hrl").

-define(M, riak_kv_eleveldb_backend_orig).

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

corrupting_from_index_key(B0) ->
    B = 
        case random:uniform(10) of 
            9 -> 
                ?I_INFO("corrupting an index key on read"),
                corrupt_binary(B0);
            _ -> B0
        end,
    ?M:from_index_key_orig(B).

corrupting_from_object_key(B0) ->
    B = 
        case random:uniform(10) of 
            9 -> 
                ?I_INFO("corrupting an object key on read"),
                corrupt_binary(B0);
            _ -> B0
        end,
    ?M:from_object_key_orig(B).

corrupting_to_object_key(A, B) ->
    Bin0 = ?M:to_object_key_orig(A, B),
    case random:uniform(10) of 
        9 -> 
            ?I_INFO("corrupting an object key on write"),
            corrupt_binary(Bin0);
        _ -> Bin0
    end.

corrupting_to_index_key(A, B, C, D) ->
    Bin0 = ?M:to_index_key_orig(A, B, C, D),
    case random:uniform(10) of 
        9 -> 
            ?I_INFO("corrupting an index key on write"),
            corrupt_binary(Bin0);
        _ -> Bin0
    end.


