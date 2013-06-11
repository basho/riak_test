#!/usr/bin/env escript -f 

-mode(compile).

main(_) ->
    {ok, Files0} = file:list_dir("."),
    Files = [File || File <- Files0, 
             lists:prefix("rstats", File)],
    io:format("Files: ~p ~p~n", [file:get_cwd(), Files]),
    Data0 = [begin
                 %%io:format("File: ~p~n", [File]),
                 case file:consult(File) of 
                     {ok, F} -> F;
                     Else ->
                         io:format("Failure in ~p: ~p~n",
                                   [File,Else]),
                         []
                 end
             end
             || File <- Files],

    Data1 =
        case all_same_len(Data0) of
            true -> Data0;
            false -> discard_short(Data0)
        end,

    %%lens(Data1),
    
    Data = lists:map(fun winnow/1, Data1),

    %%lens(Data),
    {Names, Avg} = avg_lists(Data),

    %%io:format("~p ~p~n", [Names, length(Avg)]),
    
    {ok, Fd} = file:open("rstats-digest", [write]),
    [file:write(Fd, io_lib:format("~p ", [Name]))
     || Name <- Names],
    file:write(Fd, io_lib:format("~n", [])),
    
    [begin 
         %%io:format("~p~n", [Item]),
         [file:write(Fd, io_lib:format("~p ", [Val]))
          || Val <- Item],
         file:write(Fd, io_lib:format("~n", []))
     end
     || Item <- Avg],
    file:close(Fd).


% lens(L) ->
%     [io:format("~p ", [length(Ln)]) 
%      || Ln <- L],
%     io:format("~n"). 

all_same_len([H|T]) ->
    all_same_len(T, length(H)).

all_same_len([], _) ->
    true;
all_same_len([H|T], Len) ->
    case length(H) of
        Len -> all_same_len(T, Len);
        _ -> false
    end.

discard_short(L) ->
    Longest = lists:max(lists:map(fun erlang:length/1, L)),
    lists:filter(fun(X) -> length(X) =:= Longest end, L).

avg_lists(LoL) ->
    %%io:format("~p~n", [length(LoL)]),
    avg_lists(LoL, []).

avg_lists(LoL, Acc) ->
    HaTs = lists:map(fun([H|T]) -> {H, T} end, LoL),
    {Heads, Tails} = lists:unzip(HaTs), 
    [First|_] = Heads,
    Names = [N || {N, _V} <- First],
    Avgs = avg_items(Heads, Names),
    Acc1 = [Avgs|Acc],
    case lists:any(fun(X) -> X =:= [] end, Tails) of 
        true -> {Names, lists:reverse(Acc1)};
        false -> avg_lists(Tails, Acc1)
    end.

avg_items(L, Names) ->
    %%io:format("~p~n", [length(L)]),
    Dicts = lists:map(fun orddict:from_list/1, L),
    [begin
         Vals = lists:map(fun(D) -> orddict:fetch(Name, D) end,
                          Dicts),
         case Name of
             %% vnode gets and puts are a per-minute rolling window
             vnode_gets -> 
                 (lists:sum(Vals)/length(Vals)) / 60;
             vnode_puts ->
                 (lists:sum(Vals)/length(Vals)) / 60;
             _ ->
                 lists:sum(Vals)/length(Vals)
         end
     end
     || Name <- Names].


%% get rid of timestamps an slim down the stats glob
winnow(Data0) ->
    [strip_stats(Glob) 
     || {struct, Glob} <- Data0].


strip_stats(Glob) ->
    Filter = [vnode_gets, vnode_puts, 
              node_get_fsm_time_median,
              node_get_fsm_time_95,
              node_get_fsm_time_99,
              node_put_fsm_time_median,
              node_put_fsm_time_95,
              node_put_fsm_time_99],
    [{binary_to_atom(NameBin), Val} 
     || {NameBin, Val} <- Glob, 
        lists:member(binary_to_atom(NameBin), Filter)].

binary_to_atom(Bin) ->
    list_to_atom(binary_to_list(Bin)).

