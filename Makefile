# We don't run these benchmarks under CI because they are largely benchmarking
# network latency and the underlying performance. However, we do want to be able
# to run them locally when making specific optimisations.
CI_RUN_BENCHMARKS ?= false

GENERATED_FILES += driver/sql/postgres/pgjournal/internal/xdb/queries.sql.go

-include .makefiles/Makefile
-include .makefiles/pkg/go/v1/Makefile

.makefiles/%:
	@curl -sfL https://makefiles.dev/v1 | bash /dev/stdin "$@"

%.sql.go: %.sql
	sqlc generate --file $(@D)/sqlc.yaml
