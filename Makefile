CC      = mpicc
MPIRUN      ?= mpirun
MPIRUN_ARGS ?=
STD    ?= $(shell $(CC) -std=c23 -E -x c /dev/null >/dev/null 2>&1 && echo c23 || ($(CC) -std=c2x -E -x c /dev/null >/dev/null 2>&1 && echo c2x || echo c11))
CFLAGS  = -std=$(STD) -Wall -Wextra -pedantic -O2 -pthread -Iinclude
BUILD   = build
BIN     = bin

ifeq ($(SIN_PROTECCION),1)
CFLAGS += -DSIN_PROTECCION
endif

OBJS_STORAGE = $(BUILD)/log.o $(BUILD)/fsutil.o
OBJS_CLUSTER = $(BUILD)/metadata.o $(BUILD)/mpi_layer.o $(BUILD)/replication.o $(BUILD)/job_queue.o
OBJS_NET     = $(BUILD)/net_util.o $(BUILD)/protocol.o
OBJS_CLIENT  = $(BUILD)/client.o $(OBJS_NET)

TESTS    = $(BIN)/test_log $(BIN)/test_log_recovery $(BIN)/test_log_retention \
           $(BIN)/test_metadata $(BIN)/test_race_log $(BIN)/test_replica_reset \
           $(BIN)/test_mpi_layer $(BIN)/mb_logdump

ALL_BINS = $(BIN)/brokerd $(BIN)/mbcli $(BIN)/mbbench $(TESTS)

all: $(ALL_BINS)

$(BIN)/brokerd: $(BUILD)/brokerd.o $(BUILD)/groups.o $(OBJS_NET) $(OBJS_CLUSTER) $(OBJS_STORAGE) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN)/mbcli: $(BUILD)/mbcli.o $(OBJS_CLIENT) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN)/mbbench: $(BUILD)/mbbench.o $(OBJS_CLIENT) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN)/test_log: $(BUILD)/test_log.o $(OBJS_STORAGE) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN)/test_log_recovery: $(BUILD)/test_log_recovery.o $(OBJS_STORAGE) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN)/test_log_retention: $(BUILD)/test_log_retention.o $(OBJS_STORAGE) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN)/test_metadata: $(BUILD)/test_metadata.o $(BUILD)/metadata.o $(OBJS_STORAGE) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN)/test_race_log: $(BUILD)/test_race_log.o $(OBJS_STORAGE) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN)/test_replica_reset: $(BUILD)/test_replica_reset.o $(OBJS_STORAGE) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN)/test_mpi_layer: $(BUILD)/test_mpi_layer.o $(BUILD)/mpi_layer.o | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN)/mb_logdump: $(BUILD)/mb_logdump.o $(OBJS_STORAGE) | $(BIN)
	$(CC) $(CFLAGS) -o $@ $^

# make test: unit tests, then the MPI layer, the clients and the cluster under $(MPIRUN).
# On a machine with fewer than 3 cores: make test MPIRUN_ARGS=--oversubscribe
test: all
	@echo "═══ TEST SUITE"
	@echo ""
	@$(BIN)/test_log && $(BIN)/test_log_recovery && $(BIN)/test_log_retention && \
	$(BIN)/test_metadata && $(BIN)/test_race_log && $(BIN)/test_replica_reset
	@echo ""
	@$(MAKE) --no-print-directory test-mpi
	@echo ""
	@$(MAKE) --no-print-directory test-integration
	@echo ""
	@$(MAKE) --no-print-directory test-cluster
	@echo ""
	@echo "═══ SUITE COMPLETE — any ✘ marks the exit code"

test-mpi: $(BIN)/test_mpi_layer
	@echo "═══ test_mpi_layer · capa MPI con mensajes de juguete (3 procesos)"
	$(MPIRUN) $(MPIRUN_ARGS) -np 3 $(BIN)/test_mpi_layer

test-integration: $(BIN)/brokerd $(BIN)/mbcli
	MPIRUN="$(MPIRUN)" MPIRUN_ARGS="$(MPIRUN_ARGS)" bash tests/test_integration.sh

test-cluster: $(BIN)/brokerd $(BIN)/mbcli $(BIN)/mb_logdump
	MPIRUN="$(MPIRUN)" MPIRUN_ARGS="$(MPIRUN_ARGS)" bash tests/test_cluster.sh



$(BUILD)/%.o: src/broker/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILD)/%.o: src/client/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILD)/%.o: src/cluster/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILD)/%.o: src/storage/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILD)/%.o: src/bench/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILD)/%.o: tests/%.c | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILD) $(BIN):
	mkdir -p $@

clean:
	rm -rf $(BUILD) $(BIN)

.PHONY: all test clean test-race test-mpi test-integration test-cluster


test-race:
	@echo "═══ race test · phase 1: WITH protection (every offset exactly once)"
	$(MAKE) clean > /dev/null
	$(MAKE) $(BIN)/test_race_log > /dev/null
	$(BIN)/test_race_log
	@echo ""
	@echo "═══ race test · phase 2: WITHOUT protection (-DSIN_PROTECCION) — a failure here is the expected evidence of the race"
	$(MAKE) clean > /dev/null
	$(MAKE) SIN_PROTECCION=1 $(BIN)/test_race_log > /dev/null
	-$(BIN)/test_race_log
	@echo ""
	@echo "✔ race test done: the race only shows up without protection"
	$(MAKE) clean > /dev/null
	$(MAKE) > /dev/null 2>&1
	@echo "  binaries rebuilt"