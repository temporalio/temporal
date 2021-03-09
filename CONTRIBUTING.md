# Develop Temporal Server

This doc is for contributors to Temporal Server (hopefully that's you!)

**Note:** All contributors also need to fill out the [Temporal Contributor License Agreement](https://gist.github.com/samarabbas/7dcd41eb1d847e12263cc961ccfdb197) before we can merge in any of your changes.

## Prerequisites

### Build prerequisites 
* [Go Lang](https://golang.org/) (minimum version required is 1.16):
  - Install on macOS with `brew install go`.
  - Install on Ubuntu with `sudo apt install golang`.
* [Protocol buffers compiler](https://github.com/protocolbuffers/protobuf/) (only if you are going to change `proto` files):
  - Install on macOS with `brew install protobuf`.
  - Install on Ubuntu with `sudo apt install protobuf-compiler`.

### Runtime (server and tests) prerequisites
* [docker](https://docs.docker.com/engine/install/)
* [docker-compose](https://docs.docker.com/compose/install/)

### For Windows developers

For developing on Windows, install [Windows Subsystem for Linux 2 (WSL2)](https://aka.ms/wsl) and [Ubuntu](https://docs.microsoft.com/en-us/windows/wsl/install-win10#step-6---install-your-linux-distribution-of-choice). After that, follow the guidance for installing prerequisites, building, and testing on Ubuntu.

## Check out the code

Temporal uses go modules, there is no dependency on `$GOPATH` variable. Clone the repo into the preferred location:
```bash
$ git clone https://github.com/temporalio/temporal.git
```

## Build

For the very first time build `temporal-server` and helper tools with simple `make` command: 
```bash
$ make
```

It will install all other build dependencies and build the binaries.

Further you can build binaries without running tests with:
```bash
$ make bins
```

Please check the top of our [Makefile](Makefile) for other useful build targets.

## Run tests

Tests require runtime dependencies. They can be run with `start-dependencies` target (uses `docker-compose` internally). Open new terminal window and run:
```bash
$ make start-dependencies
```

Before testing on macOS, make sure you increase the file handle limit:
```bash
$ ulimit -n 8192
```

Run unit tests:
```bash
$ make unit-test
```

Run all integration tests:
```bash
$ make integration-test
```

Or run all the tests at once:
```bash
$ make test
```

You can also run a single test:
```bash
$ go test -v <path> -run <TestSuite> -testify.m <TestSpecificTaskName>
```
for example:
```bash
$ go test -v github.com/temporalio/temporal/common/persistence -run TestCassandraPersistenceSuite -testify.m TestPersistenceStartWorkflow
```

When you are done, don't forget to stop `docker-compose` (with `Ctrl+C`) and clean up all dependencies:
```bash
$ make stop-dependencies
```

## Run Temporal Server locally

First start runtime dependencies. They can be run with `start-dependencies` target (uses `docker-compose` internally). Open new terminal window and run:
```bash
$ make start-dependencies
```

then create database schema:
```bash
$ make install-schema
```
and then run the server:
```bash
$ make start
```

Now you can create default namespace with `tctl`:
```bash
$ make tctl
$ ./tctl --ns default namespace register
```
and run samples from [Go](https://github.com/temporalio/samples-go) and [Java](https://github.com/temporalio/samples-java) samples repos. Also, you can access web UI at `localhost:8088`.

When you are done, press `Ctrl+C` to stop the server. Don't forget to stop dependencies (with `Ctrl+C`) and clean up resources:
```bash
$ make stop-dependencies
```

## Release artifacts

Release binaries are created using [GoReleaser](https://goreleaser.com/)

GoReleaser github action is configured to attach release binaries on a Github release event [Release action](.github/workflows/goreleaser.yml)

GoReleaser configuration is at [.goreleaser.yml](.goreleaser.yml)


Locally build snapshot binaries
```shell
./.development/scripts/goreleaser.sh --snapshot --rm-dist
```

Locally build release binaries
```shell
./.development/scripts/goreleaser.sh
```


## Licence headers

This project is Open Source Software, and requires a header at the beginning of
all source files. To verify that all files contain the header execute:
```bash
$ make copyright
```

## Commit Messages And Titles of Pull Requests

Overcommit adds some requirements to your commit messages. At Temporal, we follow the
[Chris Beams](http://chris.beams.io/posts/git-commit/) guide to writing git
commit messages. Read it, follow it, learn it, love it.

All commit messages are from the titles of your pull requests. So make sure follow the rules when titling them. 
Please don't use very generic titles like "bug fixes". 

All PR titles should start with Upper case and have no dot at the end.
