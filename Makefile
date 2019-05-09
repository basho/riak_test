HEAD_REVISION   ?= $(shell git describe --tags --exact-match HEAD 2>/dev/null)

.PHONY: deps

APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler hipe mnesia public_key \
	observer wx gs
PLT = $(HOME)/.riak-test_dialyzer_plt

REBAR=./rebar3

all: deps compile
	$(REBAR) as test compile
	$(REBAR) escriptize
	SMOKE_TEST=1 $(REBAR) escriptize

deps:
	$(if $(HEAD_REVISION),$(warning "Warning: you have checked out a tag ($(HEAD_REVISION)) and should use the locked-deps target"))
	$(REBAR) get-deps

docsclean:
	@rm -rf doc/*.png doc/*.html doc/*.css edoc-info

compile: deps
	$(REBAR) compile

clean:
	@$(REBAR) clean

distclean: clean
	@rm -rf riak_test deps

quickbuild:
	$(REBAR) compile
	$(REBAR) escriptize

##################
# Dialyzer targets
##################

# include tools.mk
