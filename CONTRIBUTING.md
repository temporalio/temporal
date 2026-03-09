# Develop Temporal Server

This doc is for contributors to Temporal Server (hopefully that's you!)

**Note:** All contributors also need to fill out the [Temporal Contributor License Agreement](./docs/development/temporal-cla.md) before we can merge in any of your changes.

## Prerequisites

### Build prerequisites

- [Go Lang](https://go.dev/) (minimum version required listed in `go.mod` [file](go.mod)):
  - Install on macOS with `brew install go`.
  - Install on Ubuntu with `sudo apt install golang`.
- [Protocol buffers compiler](https://github.com/protocolbuffers/protobuf/) (only if you are going to change `proto` files):
  - Install on macOS with `brew install protobuf`.
  - Download all other versions from [protoc release page](https://github.com/protocolbuffers/protobuf/releases).
- [Temporal CLI](https://github.com/temporalio/cli)
  - Homebrew `brew install temporal`
  - Or download it from here https://github.com/temporalio/cli

### Runtime (server and tests) prerequisites

- [docker](https://docs.docker.com/engine/install/)

> Note: it is possible to run Temporal server without a `docker`. If for some reason (for example, performance on macOS)
> you want to run dependencies on the host OS, please follow the [doc](./docs/development/run-dependencies-host.md).

- Runtime dependencies are optional support services that can be helpful during development and testing, providing: 1) UI, 2) 
databases, and 3) metrics services via `docker compose`. By default, the server utilizes SQLite as an in-memory 
database, so the runtime dependencies are optional. To start dependencies, open new terminal window and run:

```bash
make start-dependencies
```

To stop the dependencies:
```bash
make stop-dependencies
```

### For Windows developers

For developing on Windows, install [Windows Subsystem for Linux 2 (WSL2)](https://aka.ms/wsl) and [Ubuntu](https://docs.microsoft.com/en-us/windows/wsl/install-win10#step-6---install-your-linux-distribution-of-choice). After that, follow the guidance for installing prerequisites, building, and testing on Ubuntu.

## Check out the code

Temporal uses go modules, there is no dependency on `$GOPATH` variable. Clone the repo into the preferred location:

```bash
git clone https://github.com/temporalio/temporal.git
```

## Build

For the very first time build `temporal-server` and helper tools with simple `make` command:

```bash
make
```

It will install all other build dependencies and build the binaries.

Further you can build binaries without running tests with:

```bash
make bins
```

Please check the top of our [Makefile](Makefile) for other useful build targets.

## Run tests

We defined three categories of tests.

- Unit test: Those tests should not have dependencies other than the test target and go mock. We should have unit test coverage as much as possible.
- Integration test: Those tests cover the integration between the server and the dependencies (Cassandra, SQL, ES etc.).
- Functional test: Those tests cover the E2E functionality of Temporal server. They are all under ./tests directory.

Integration and functional tests require [runtime dependencies](#runtime-server-and-tests-prerequisites), 
when running with a persistence option that is not SQLite. If running unit tests, no need to start the dependencies.

Run unit tests:

```bash
make unit-test
```

Run all integration tests:

```bash
make integration-test
```

Run all functional tests:

```bash
make functional-test
```

Or run all the tests at once:

```bash
make test
```

You can also run a single test:

```bash
go test -v <path> -run <TestSuite> -testify.m <TestSpecificTaskName>
```

for example:

```bash
go test -v github.com/temporalio/temporal/common/persistence -run TestCassandraPersistenceSuite -testify.m TestPersistenceStartWorkflow
```

When you are done, don't forget to stop `docker compose` (with `Ctrl+C`) and clean up all dependencies:

```bash
make stop-dependencies
```

## Run Temporal Server locally

First, start the optional [runtime dependencies](#runtime-server-and-tests-prerequisites) if needed for the desired persistence option.

Then run the server:

```bash
make start
```

This will start the server using SQLite as an in-memory database. You can choose other databases as well.

If you want to run with Cassandra and Elasticsearch, then run these commands:

```bash
make install-schema-cass-es
make start-cass-es
```

To run with SQLite with a persisted file:

```bash
make start-sqlite-file
```

To run with Postgres:
```bash
make install-schema-postgresql
make start-postgresql
```

To run with MySQL:
```bash
make install-schema-mysql
make start-mysql
```

Now you can create a namespace with the Temporal CLI (While you can select any name for a namespace, we reccomend using `default` while learning, because a number of samples assume there is a namespace named `default`):

```bash
temporal operator namespace create -n default
```

and run samples from the samples repos ([Go](https://github.com/temporalio/samples-go) | [Java](https://github.com/temporalio/samples-java) | [TypeScript](https://github.com/temporalio/samples-typescript) | [.NET](https://github.com/temporalio/samples-dotnet) | [Python](https://github.com/temporalio/samples-python) | [Ruby](https://github.com/temporalio/samples-ruby)). If you are new to Temporal, helloworld ([Go](https://github.com/temporalio/samples-go/tree/main/helloworld) | [Java](https://github.com/temporalio/samples-java/tree/main/core/src/main/java/io/temporal/samples/hello) | [TypeScript](https://github.com/temporalio/samples-typescript/tree/main/hello-world) | [.NET](https://github.com/temporalio/sdk-dotnet?tab=readme-ov-file#implementing-a-workflow-and-activity) | [Python](https://github.com/temporalio/samples-python/tree/main/hello) | [Ruby](https://github.com/temporalio/sdk-ruby?tab=readme-ov-file#implementing-a-workflow-and-activity)) is a very good sample to start with.  Also, if you have started the runtime dependencies, you can access the web UI at `localhost:8080` which is a good way to visualize work done by the server and deepen your knowledge of Temporal.

When you are done, press `Ctrl+C` to stop the server. 

If you started [runtime dependencies](#runtime-server-and-tests-prerequisites), don't forget to stop dependencies 
(with `Ctrl+C`) and clean up resources:

```bash
make stop-dependencies
```

See the [developer documentation on testing](./docs/development/testing.md) to learn more about writing tests.

## Debugging with the IDE

### GoLand

For general instructions, see [GoLand Debugging](https://www.jetbrains.com/help/go/debugging-code.html).

First, start the optional [runtime dependencies](#runtime-server-and-tests-prerequisites) if needed for the desired persistence option.

To run the server, ensure the Run Type is package. In "Package path", enter `go.temporal.io/server/cmd/server`. 
In the "Program arguments" field, add the following:

```
--env <database dev environment> --allow-no-auth start
```

For example, to run with Postgres:
```
--env development-postgres12 --allow-no-auth start
```

See Makefile for other environments.

## Working with merged API changes

gRPC / protobuf changes merged to the [api](https://github.com/temporalio/api) repo automatically trigger a commit in [api-go](https://github.com/temporalio/api-go).
To bring such changes into your feature branch, use `make update-go-api`.

## Working with local API changes

If you need to make changes to the gRPC / protobuf definitions while also working on code in this repo, do the following:

1. Checkout [api](https://github.com/temporalio/api), [api-go](https://github.com/temporalio/api-go), and [sdk-go](https://github.com/temporalio/sdk-go)
2. Make your changes to `api`, commit to a branch.
3. In your copy of `api-go`:
   1. Initialize submodules: `git submodule update --init --recursive`
   2. Point api submodule at your branch. If you make more commits to the api repo, run the last command again.
      ```bash
      git submodule set-url proto/api ../api
      git submodule set-branch --branch mystuff proto/api
      git submodule update --remote proto/api
      ```
   3. Compile protos: `make proto`
4. (Optional, if SDK changes are required:) In your copy of `sdk-go`:
   1. Point `go.mod` at local `api-go`:
      ```
      replace (
          go.temporal.io/api => ../api-go
      )
      ```
   2. Compile & fix errors: `make bins`
5. In this repo:
   1. Point `go.mod` at local `api-go` and `sdk-go`:
      ```
      replace (
          go.temporal.io/api => ../api-go
          go.temporal.io/sdk => ../sdk-go
      )
      ```
   2. Build & fix errors: `make proto && make bins`

## License headers

This project is Open Source Software, and requires a header at the beginning of
all source files. To verify that all files contain the header execute:

```bash
make copyright
```

## Commit Messages And Titles of Pull Requests

Overcommit adds some requirements to your commit messages. At Temporal, we follow the
[Chris Beams](http://chris.beams.io/posts/git-commit/) guide to writing git
commit messages. Read it, follow it, learn it, love it.

All commit messages are from the titles of your pull requests. So make sure follow the rules when titling them.
Please don't use very generic titles like "bug fixes".

All PR titles should start with Upper case and have no dot at the end.

## Go version update

To update the Go version, update the `go` directive in `go.mod`. CI workflows automatically pick up the version from `go.mod`.

## License

MIT License, please see [LICENSE](LICENSE) for details.
