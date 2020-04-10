# Developing Temporal

This doc is intended for contributors to `temporal` server (hopefully that's you!)

**Note:** All contributors also need to fill out the [Temporal Contributor License Agreement](https://gist.github.com/samarabbas/7dcd41eb1d847e12263cc961ccfdb197) before we can merge in any of your changes.

## Development Environment

* Go. Install on OS X with `brew install go`.
* Protobuf compiler. Install on OS X with `brew install protobuf`.

## Checking out the code

Clone the repo into the preffered location:
```bash
git clone https://github.com/temporalio/temporal.git
```

## Building

For the very first time compile `temporal` server wtih `make` command: 
```bash
make
```

it will install all build dependencies and build the project.

Futher you can build the `temporal` service and helper tools without running test:
```bash
make bins
```

## Testing

Before running the unit tests you must have `cassandra` running locally:
```bash
# for OS X
brew install cassandra

# start cassandra
/usr/local/bin/cassandra
```

Run unit tests:
```bash
make unit-test
```

To run integration tests you must have `kafka` running locally (in addition to `cassandra`).
o run kafka, follow kafka quickstart guide [here](https://kafka.apache.org/quickstart).

Run all integration tests:
```bash
make integration-test
```

Or run all the tests at once:
```bash
make test
```
Or run single test:
```bash
go test -v <path> -run <TestSuite> -testify.m <TestSpercificTaskName>
```
for example:
```bash
go test -v github.com/temporalio/temporal/common/persistence -run TestCassandraPersistenceSuite -testify.m TestPersistenceStartWorkflow
```

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

All PR titles should start with Upper case.
