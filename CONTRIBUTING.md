# Contributing

Thanks for your interest in improving brahmanda. This is a small project; the bar
is simply that changes build, pass tests, and stay gofmt-clean.

## Prerequisites

- Go 1.24+
- Linux or macOS (see the platform note in the README)

## Development loop

```sh
make build     # build brahma, srishti, chitra into ./bin
make test      # go test ./...
make vet       # go vet ./...
make fmt       # gofmt the tree
make tidy      # go mod tidy
```

Please run `make fmt vet test` before opening a pull request.

## Guidelines

- Keep changes focused; unrelated refactors make review harder.
- Add or update tests for any behaviour change — the repo favours functional
  tests that exercise a whole binary or package over narrow unit tests.
- Match the surrounding style. Comments explain *why*, not *what*; let
  well-named code speak for itself.
- Describe the motivation (the *why*) in the PR description.

## Licensing of contributions

By submitting a contribution you agree it is licensed under the project's
[MIT License](LICENSE).

## Reporting issues

Open a GitHub issue with what you did, what you expected, and what happened.
For a crash, include the relevant lines from the worker log
(`<state_dir>/workers/<pool>/<worker_id>.log`) and the journal entry if there is
one.
