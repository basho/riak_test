.PHONY: deps

all: deps compile testcases
	./rebar skip_deps=true escriptize
	SMOKE_TEST=1 ./rebar skip_deps=true escriptize

deps:
	./rebar get-deps

docsclean:
	@rm -rf doc/*.png doc/*.html doc/*.css edoc-info

compile: deps
	./rebar compile

clean: clean_testcases
	@./rebar clean

distclean: clean
	@rm -rf riak_test deps

quickbuild:
	./rebar skip_deps=true compile
	./rebar escriptize

testcases:
	@(cd search-corpus; tar fx spam.0.1.tar.gz)

clean_testcases:
	@rm -rf search-corpus/spam.0/

##################
# Dialyzer targets
##################

# Legacy target left for compatibility with any existing automation
# scripts ...
clean_plt:
	cleanplt

include tools.mk
