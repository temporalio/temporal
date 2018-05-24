# Developing Cadence

This doc is intended for contributors to `cadence` server (hopefully that's you!)

**Note:** All contributors also need to fill out the [Uber Contributor License Agreement](http://t.uber.com/cla) before we can merge in any of your changes

## Development Environment

* Go. Install on OS X with `brew install go`.

## Checking out the code

Make sure the repository is cloned to the correct location:

```bash
cd $GOPATH
git clone https://github.com/uber/cadence.git src/github.com/uber/cadence
cd $GOPATH/src/github.com/uber/cadence
```

## Dependency management

Dependencies are tracked via `glide.yaml`. If you're not familiar with `glide`,
read the [docs](https://github.com/Masterminds/glide#usage).
After you install gide, run below command to get all dependencies into vendor folder.
```bash
glide up
```

## Licence headers

This project is Open Source Software, and requires a header at the beginning of
all source files. To verify that all files contain the header execute:

```bash
make copyright
```

## Commit Messages

Overcommit adds some requirements to your commit messages. At Uber, we follow the
[Chris Beams](http://chris.beams.io/posts/git-commit/) guide to writing git
commit messages. Read it, follow it, learn it, love it.

## Building

You can compile the `cadence` service and helper tools without running test:

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
go test -v github.com/uber/cadence/common/persistence -run TestCassandraPersistenceSuite -testify.m TestPersistenceStartWorkflow
```

