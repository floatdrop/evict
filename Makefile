GO            ?= go
GOLANGCI_LINT ?= golangci-lint

# The benchmarks live in their own module so that the caches themselves stay
# free of dependencies.
MODULES := . bench

FUZZTIME   ?= 30s
CPUS       ?= 1,2,4,8
COUNT      ?= 5
CAPACITIES ?= 10000,100000,1000000,10000000

# The benchmarks measure time, and would get in each other's way.
.NOTPARALLEL:

.PHONY: all
all: lint test

.PHONY: fmt
fmt: ## Format the code.
	@for m in $(MODULES); do (cd $$m && $(GOLANGCI_LINT) fmt ./...) || exit 1; done

.PHONY: lint
lint: ## Run the linters.
	@for m in $(MODULES); do (cd $$m && $(GOLANGCI_LINT) run ./...) || exit 1; done

.PHONY: test
test: ## Run the tests with the race detector.
	@for m in $(MODULES); do (cd $$m && $(GO) test -race -shuffle=on -timeout=5m ./...) || exit 1; done

.PHONY: fuzz
fuzz: ## Fuzz every target for FUZZTIME (default 30s) each.
	$(GO) test -run='^$$' -fuzz=FuzzSlab -fuzztime=$(FUZZTIME) ./internal/slab
	$(GO) test -run='^$$' -fuzz=FuzzAgainstModel -fuzztime=$(FUZZTIME) ./lru
	$(GO) test -run='^$$' -fuzz=FuzzAgainstModel -fuzztime=$(FUZZTIME) ./twoq
	$(GO) test -run='^$$' -fuzz=FuzzAgainstModel -fuzztime=$(FUZZTIME) ./sieve
	$(GO) test -run='^$$' -fuzz=FuzzAgainstModel -fuzztime=$(FUZZTIME) ./s3fifo
	cd bench && $(GO) test -run='^$$' -fuzz=FuzzRead -fuzztime=$(FUZZTIME) ./trace

.PHONY: bench
bench: bench-hitratio bench-throughput bench-memory ## Run all benchmarks and rewrite bench/results.

.PHONY: bench-hitratio
bench-hitratio: ## Replay the public traces against the caches (downloads 130 MB the first time).
	cd bench && $(GO) run ./cmd/hitratio -out results
	cd bench && $(GO) run ./cmd/hitratio -sharding -out results

# The benchmarks are run COUNT times from start to end rather than each of
# them COUNT times in a row, so that a spell of noise on the machine does not
# land on all the runs of one cache. A run that fails ends it all.
.PHONY: bench-throughput
bench-throughput: ## Measure throughput on CPUS (default 1,2,4,8) CPUs, COUNT (default 5) times.
	cd bench && : > results/throughput.txt
	cd bench && for run in $$(seq $(COUNT)); do \
		$(GO) test -run='^$$' -bench=. -cpu=$(CPUS) -timeout=0 . >> results/throughput.txt || exit 1; \
	done
	cd bench && $(GO) run ./cmd/benchtable -charts results < results/throughput.txt > results/throughput.md

.PHONY: bench-memory
bench-memory: ## Measure memory per entry and the cost to the garbage collector, for caches of CAPACITIES entries.
	cd bench && $(GO) run ./cmd/memory -capacities $(CAPACITIES) -out results > /dev/null

# Not part of bench: it takes half an hour, and what it reports changes only
# when s3fifo does.
.PHONY: bench-tuning
bench-tuning: ## Replay the traces against s3fifo with 84 settings, to see what tuning it is worth.
	cd bench && $(GO) run ./cmd/tune -out results

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
