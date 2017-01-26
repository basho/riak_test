# bash_completion for riak_test
_riak_test()
{
    local cur prev tests
    if type _get_comp_words_by_ref &>/dev/null; then
        _get_comp_words_by_ref cur prev
    else
        for i in ${!COMP_WORDS[*]}; do
            prev=$cur
            cur=${COMP_WORDS[$i]}
        done
    fi

    case $prev in
        riak_test|*/riak_test)
            COMPREPLY=( $(compgen -W "-h -c -t -s -d -x -v -o -b -u -r -F \
                                      --help --conf --tests --suites \
                                      --dir --skip --verbose --outdir --backend \
                                      --upgrade --report --file" -- "$cur") )
            ;;
        -t|--tests)
            tests=$(grep -l confirm/0 ./tests/*.erl 2>/dev/null \
                | xargs basename -s .erl)
            COMPREPLY=( $(compgen -W "$tests" -- "$cur") )
            ;;
    esac
}
complete -F _riak_test riak_test ./riak_test
