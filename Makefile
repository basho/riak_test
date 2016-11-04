HEAD_REVISION   ?= $(shell git describe --tags --exact-match HEAD 2>/dev/null)

.PHONY: deps

APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler hipe mnesia public_key \
	observer wx gs
PLT = $(HOME)/.riak-test_dialyzer_plt

all: deps compile
	./rebar skip_deps=true escriptize
	SMOKE_TEST=1 ./rebar skip_deps=true escriptize

deps:
	$(if $(HEAD_REVISION),$(warning "Warning: you have checked out a tag ($(HEAD_REVISION)) and should use the locked-deps target"))
	./rebar get-deps

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

##
## Lock Targets
##
##  see https://github.com/seth/rebar_lock_deps_plugin
lock: deps compile
	./rebar lock-deps

locked-all: locked-deps compile

locked-deps:
	@echo "Using rebar.config.lock file to fetch dependencies"
	./rebar -C rebar.config.lock get-deps

##################
# Dialyzer targets
##################

# Legacy target left for compatibility with any existing automation
# scripts ...
clean_plt:
	cleanplt

include tools.mk
