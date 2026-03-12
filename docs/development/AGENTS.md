# AI Quality Gates Policy

This document defines mandatory quality checks for any AI editor working in this repository.

## Required Checks

After every code change, run all checks in this order:

1. `make revive`
2. `make gosec`
3. `make test`

If `make` is unavailable in the local environment, run the equivalent commands:

1. `go tool revive -set_exit_status ./...`
2. `go tool gosec ./...`
3. `go test ./...`

## Mandatory Behavior

- Do not claim work is complete unless all required checks have been run for the current code state.
- If files change after checks ran, rerun the full check sequence.
- Before any commit is created, rerun the full check sequence and report the result.
- If a check fails, report the failure clearly and stop at a safe state.
