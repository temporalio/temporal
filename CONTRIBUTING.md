# Develop Temporal Server
This doc is for contributors to Temporal Server (hopefully that's you!)

**Note:** All contributors also need to fill out the [Temporal Contributor License Agreement](develop/docs/temporal-cla.md) before we can merge in any of your changes.

## Prerequisites

### Build prerequisites 
* [Go Lang](https://golang.org/) (minimum version required is 1.18):
  - Install on macOS with `brew install go`.
  - Install on Ubuntu with `sudo apt install golang`.
* [Protocol buffers compiler](https://github.com/protocolbuffers/protobuf/) (only if you are going to change `proto` files):
  - Install on macOS with `brew install protobuf`.
  - Download all other versions from [protoc release page](https://github.com/protocolbuffers/protobuf/releases).
* [Temporal CLI tctl](https://github.com/temporalio/tctl)
  - Homebrew `brew install tctl`
  - Go install `make update-tctl`
  - Or download it from here https://github.com/temporalio/tctl


### Runtime (server and tests) prerequisites
* [docker](https://docs.docker.com/engine/install/)
* [docker-compose](https://docs.docker.com/compose/install/)

> Note: it is possible to run Temporal server without a `docker`. If for some reason (for example, performance on macOS)
> you want to run dependencies on the host OS, please follow the [doc](develop/docs/run_dependencies_host.md).

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
Tests require runtime dependencies. They can be run with `start-dependencies` target (uses `docker-compose` internally). Open new terminal window and run:
```bash
make start-dependencies
```

Before testing on macOS, make sure you increase the file handle limit:
```bash
ulimit -n 8192
```

Run unit tests:
```bash
make unit-test
```

Run all integration tests:
```bash
make integration-test
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

When you are done, don't forget to stop `docker-compose` (with `Ctrl+C`) and clean up all dependencies:
```bash
make stop-dependencies
```

## Run Temporal Server locally
First start runtime dependencies. They can be run with `start-dependencies` target (uses `docker-compose` internally). Open new terminal window and run:
```bash
make start-dependencies
```

then create database schema:
```bash
make install-schema
```
and then run the server:
```bash
make start
```

Now you can create default namespace with `tctl`:
```bash
tctl --namespace default namespace register
```
and run samples from [Go](https://github.com/temporalio/samples-go) and [Java](https://github.com/temporalio/samples-java) samples repos. Also, you can access web UI at `localhost:8088`.

When you are done, press `Ctrl+C` to stop the server. Don't forget to stop dependencies (with `Ctrl+C`) and clean up resources:
```bash
make stop-dependencies
```

## Working with pending API changes
If you need to make changes to the gRPC definitions while also working on code in this repo, do the following:

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
4. In your copy of `sdk-go`:
    1. Point `go.mod` at local `api-go`:
       ```
       replace (
           go.temporal.io/api => ../api-go
       )
        ```
    2. Compile & fix errors: `make bins`
5. In this repo:
    1. Initialize submodules: `git submodule update --init --recursive`
    2. Point api submodule at your branch. If you make more commits to the api repo, run the last command again.
       ```bash
       git submodule set-url proto/api ../api
       git submodule set-branch --branch mystuff proto/api
       git submodule update --remote proto/api
       ```
    3. Stage the change: `git add -u` (otherwise makefile will blow it away)
    4. Point `go.mod` at local `api-go` and `sdk-go`:
       ```
       replace (
           go.temporal.io/api => ../api-go
           go.temporal.io/sdk => ../sdk-go
       )
        ```
    5. Build & fix errors: `make proto && make bins`

## Licence headers
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
