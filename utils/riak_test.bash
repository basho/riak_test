# bash_completion for riak_test
_riak_test()
{
    local cur prev
    _get_comp_words_by_ref cur prev

    case $prev in
        riak_test)
            COMPREPLY=( $( compgen -W "-h -c -t -s -d -v -o -b -u -r" -- "$cur" ) )
            ;;
        -t)
            RT_TESTS=`grep -l confirm ./tests/*.erl | xargs basename -s .erl`
            COMPREPLY=( $( compgen -W "$RT_TESTS" -- "$cur") )
            ;;

    esac
}
complete -F _riak_test riak_test

