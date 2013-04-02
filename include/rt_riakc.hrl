%% ===================================================================
%% Secondary index compatibility, copied from later riakc
%% ===================================================================
-define(MD_INDEX, <<"index">>).

set_secondary_index_compat(MD, Indexes) ->
    case lists:member({set_secondary_index,2}, riakc_obj:module_info(exports)) of
        true ->
            %% After 
            riakc_obj:set_secondary_index(MD, Indexes);
        false ->
            set_secondary_index(MD, Indexes)
    end.

set_secondary_index(MD, []) ->
    MD;
set_secondary_index(MD, {{binary_index, Name}, BinList}) ->
    set_secondary_index(MD, [{{binary_index, Name}, BinList}]);
set_secondary_index(MD, {{integer_index, Name}, IntList}) ->
    set_secondary_index(MD, [{{integer_index, Name}, IntList}]);
set_secondary_index(MD, [{{binary_index, Name}, BinList} | Rest]) ->
    IndexName = index_id_to_bin({binary_index, Name}),
    set_secondary_index(MD, [{IndexName, BinList} | Rest]);
set_secondary_index(MD, [{{integer_index, Name}, IntList} | Rest]) ->
    IndexName = index_id_to_bin({integer_index, Name}),
    set_secondary_index(MD, [{IndexName, [list_to_binary(integer_to_list(I)) || I <- IntList]} | Rest]);
set_secondary_index(MD, [{Id, BinList} | Rest]) when is_binary(Id) ->
    List = [{Id, V} || V <- BinList],
    case dict:find(?MD_INDEX, MD) of
        {ok, Entries} ->
            OtherEntries = [{N, V} || {N, V} <- Entries, N /= Id],
            NewList = lists:usort(lists:append(OtherEntries, List)),
            MD2 = dict:store(?MD_INDEX, NewList, MD),
            set_secondary_index(MD2, Rest);
        error ->
            NewList = lists:usort(List),
            MD2 = dict:store(?MD_INDEX, NewList, MD),
            set_secondary_index(MD2, Rest)
    end.

%% @doc  INTERNAL USE ONLY.  Convert index id tuple to binary index name
%% @private
index_id_to_bin({binary_index, Name}) ->
    list_to_binary([Name, "_bin"]);
index_id_to_bin({integer_index, Name}) ->
    list_to_binary([Name, "_int"]).
