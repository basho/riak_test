#!/usr/bin/env escript -f

-mode(compile).

read_file(File, Owner) ->
    %%io:format("File: ~p~n", [File]),
    Out =
        case file:consult(File) of
            {ok, F} -> F;
            Else ->
                io:format("Failure in ~p: ~p~n",
                          [File,Else]),
                []
        end,
    Owner ! Out.

main(_) ->
    Start = now(),
    Timer = fun(Slogan) ->
                    T = trunc(timer:now_diff(now(), Start)/1000),
                    io:format("~p ~p ~n", [Slogan, T])
            end,

    {ok, Files0} = file:list_dir("."),
    Files = [File || File <- Files0,
             lists:prefix("cstats-1", File)],
    if Files =:= [] ->
	    error(no_files_found);
       true -> ok
    end,

    %% io:format("Files: ~p ~p~n", [file:get_cwd(), Files]),
    Main = self(),
    [spawn(fun() -> read_file(Fn, Main) end)|| Fn <- Files],
    Data00 = [receive L -> L end || _ <- Files],

    Data0 = lists:filter(fun(L) -> length(L) /= 0 end, Data00),

    Timer(consult_files),

    Data1 = normalize_len(Data0),

    Timer(normalize_lens),
    %% lens(Data1),

    Data = lists:map(fun winnow/1, Data1),
    Timer(winnow),
    %% lens(Data),
    {Names, Avg} = avg_lists(Data),
    Timer(average),
    %%io:format("~p ~p~n", [Names, length(Avg)]),

    {ok, Fd} = file:open("rstats-digest", [write]),
    [file:write(Fd, io_lib:format("~s ", [atom_to_list(Name)]))
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

%% lens(L) ->
%%     [io:format("~p ", [length(Ln)])
%%      || Ln <- L],
%%     io:format("~n").

normalize_len(L) ->
    Lengths = lists:map(fun erlang:length/1, L),
    Longest = lists:max(Lengths),
    Shortest = lists:min(Lengths),
    case (Longest - Shortest) < 4 of
        true ->
            trim_long(Shortest, lists:zip(Lengths, L), []);
        false ->
            discard_short(L)
    end.

trim_long(_, [], Acc) ->
    Acc;
trim_long(Len, [{L, List}|T], Acc) ->
    io:format("~p ~p ", [Len, L]),
    case L > Len of
        true ->
            io:format("trunc ~n"),
            {List1, _} = lists:split(Len, List),
            trim_long(Len, T, [List1| Acc]);
        false ->
            io:format("~n"),
            trim_long(Len, T, [List| Acc])
    end.

discard_short(L) ->
    Longest = lists:max(lists:map(fun erlang:length/1, L)),
    io:format("longest ~p ~n", [Longest]),
    lists:filter(fun(X) ->
                         Len = length(X),
                         io:format("len ~p ~n", [Len]),
                         Len =:= Longest
                 end, L).

avg_lists(LoL) ->
    avg_lists(LoL, []).

avg_lists(LoL, Acc) ->
    HaTs = lists:map(fun([H|T]) -> {H, T}
		     end, LoL),
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
     || Glob <- Data0,
	is_list(Glob)].


strip_stats(Glob) ->
    Filter = [
	      node_gets, node_puts,
	      vnode_gets, vnode_puts,
              node_get_fsm_time_median,
              node_get_fsm_time_95,
              node_get_fsm_time_99,
              node_put_fsm_time_median,
              node_put_fsm_time_95,
              node_put_fsm_time_99,
              message_queue_max,
	      cpu_utilization,
	      cpu_iowait,
	      memory_utilization,
	      memory_page_dirty,
	      memory_page_writeback,
	      dropped_vnode_requests_total,
	      node_get_fsm_objsize_median,
	      node_get_fsm_objsize_95,
	      node_get_fsm_objsize_99,
	      %% this is not at all portable
	      'dm-0_disk_utilization'
	     ],
    [begin
	 {Name, Val}
     end
     || {Name, Val} <- Glob,
        lists:member(Name, Filter)].

