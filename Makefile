.PHONY: deps

APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler hipe mnesia public_key \
	observer wx gs
PLT = $(HOME)/.riak-test_dialyzer_plt

all: deps compile
	./rebar skip_deps=true escriptize
	SMOKE_TEST=1 ./rebar skip_deps=true escriptize

deps:
	./rebar get-deps

docs:
	./rebar skip_deps=true doc

docsclean:
	@rm -rf doc/*.png doc/*.html doc/*.css edoc-info

compile: deps
	./rebar compile

clean:
	@./rebar clean

distclean: clean
	@rm -rf riak_test deps

quickbuild:
	./rebar skip_deps=true compile
	./rebar escriptize

##################
# Dialyzer targets
##################

# public targets
dialyzer: compile $(PLT)
	dialyzer -Wno_return -Wunderspecs -Wunmatched_returns --plt $(PLT) ebin deps/*/ebin | \
		egrep -v -f ./dialyzer.ignore-warnings

clean_plt:
	@echo
	@echo "Are you sure?  It takes about 1/2 hour to re-build."
	@echo Deleting $(PLT) in 5 seconds.
	@echo
	sleep 5
	rm $(PLT)

# internal targets
# build plt file. assumes 'compile' was already run, e.g. from 'dialyzer' target
$(PLT):
	@echo
	@echo "Building dialyzer's plt file. This can take 1/2 hour."
	@echo "  Because it wasn't here:" $(PLT)
	@echo "  Consider using R15B03 or later for 100x faster build time!"
	@echo
	@sleep 1
	dialyzer --build_plt --output_plt $(PLT) --apps $(APPS)
