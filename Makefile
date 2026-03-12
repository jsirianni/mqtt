.PHONY: gosec revive test release-test

gosec:
	go tool gosec ./...

revive:
	go tool revive -set_exit_status ./...

test:
	go test ./...

release-test:
	goreleaser release --snapshot --clean --skip=publish
