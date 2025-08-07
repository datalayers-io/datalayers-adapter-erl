SCRIPTS = $(CURDIR)/scripts

.PHONY: all
all: compile

.PHONY: compile
compile:
	rebar3 compile

.PHONY: clean
clean:
	rebar3 clean
	cargo clean

.PHONY: clean-all
clean-all:
	@rm -f rebar.lock
	@rm -rf _build
	@rm -rf target
	@echo "Cleaned all build artifacts."

.PHONY: ct
ct:
	rebar3 ct --readable true -c

.PHONY: dialyzer
dialyzer:
	rebar3 as check do dialyzer

.PHONY: fmt
fmt:
	rebar3 fmt
	cargo fmt --all -- --check
