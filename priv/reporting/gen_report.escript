#!/usr/bin/env escript -f

-mode(compile).

main([Dir1, Dir2]) ->
    main([Dir1, Dir2, "false"]);
main([Dir1, Dir2, Regen]) ->
    {ok, L10} = file:list_dir(Dir1),
    {ok, L20} = file:list_dir(Dir2),
    

    Len = erlang:min(length(L10), length(L20)),
    {L1, _} = lists:split(Len, L10),
    {L2, _} = lists:split(Len, L20),


    CL = lists:zip(lists:usort(L1), lists:usort(L2)),
    %%M = 
    lists:map(fun(X) ->
                      run_compare(X, Dir1, Dir2, Regen)
              end,
              CL);
    % lists:foreach(fun(M1) ->
    %                       io:format(M1)
    %               end, M);
main(_) ->
    io:format("incorrect number of arguments\n").


run_compare({A, B}, DirA, DirB, Regen) ->
    %%io:format("~p ~p ~p ~p ~p ~n", [DirA, A, DirB, B, Regen]),
    O= os:cmd("./compare.sh "++DirA++"/"++A++" "++
              DirB++"/"++B++" "++Regen),
    io:format("~p ~n", [O]).
%%os:cmd("mv summary.png "++A++"vs"++B++"-sumary.png").    
    
