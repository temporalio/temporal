# Developing Temporal

This doc is intended for contributors to `temporal` server (hopefully that's you!)

**Note:** All contributors also need to fill out the [Temporal Contributor License Agreement](https://gist.github.com/samarabbas/7dcd41eb1d847e12263cc961ccfdb197) before we can merge in any of your changes

## Development Environment

* Go. Install on OS X with `brew install go`.

## Checking out the code

Make sure the repository is cloned to the correct location:

```bash
cd $GOPATH
git clone https://github.com/temporalio/temporal.git src/github.com/temporalio/temporal
cd $GOPATH/src/github.com/temporalio/temporal
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

All PR titles should start with UPPER case.

## Building

You can compile the `temporal` service and helper tools without running test:

```bash
make bins
```

## Testing

Before running the tests you must have `cassandra` and `kafka` running locally:

```bash
# for OS X
brew install cassandra

# start cassandra
/usr/local/bin/cassandra
```

To run kafka, follow kafka quickstart guide [here](https://kafka.apache.org/quickstart)

Run all the tests:

```bash
make test

# `make test` currently do not include crossdc tests, start kafka and run 
make test_xdc

# or go to folder with *_test.go, e.g
cd service/history/ 
go test -v
# run single test
go test -v <path> -run <TestSuite> -testify.m <TestSpercificTaskName>
# example:
go test -v github.com/temporalio/temporal/common/persistence -run TestCassandraPersistenceSuite -testify.m TestPersistenceStartWorkflow
```
