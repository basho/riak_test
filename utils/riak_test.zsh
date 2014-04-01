#compdef riak_test

_riak_test() {
    local curcontext="$curcontext" state line
    typeset -A opt_args

    TESTS=$(ls ./tests/*.erl | xargs basename -s .erl | tr '\n' ' ')
    CONFIGS=$(cat ~/.riak_test.config | grep \^{ | sed s/{// | tr ', [\n' ' ')

    _arguments \
        "(-t -c -s -d -v -o -b -r)-h[print usage page]" \
        "-c+[specify the project configuraiton file]:config:($CONFIGS)" \
        "-t+[specify which tests to run]:tests:($TESTS)"
}

_riak_test "$@"
