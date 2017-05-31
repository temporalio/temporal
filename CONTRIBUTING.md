# Developing Cadence

This doc is intended for contributors to `cadence` server (hopefully that's you!)

**Note:** All contributors also need to fill out the [Uber Contributor License Agreement](http://t.uber.com/cla) before we can merge in any of your changes

## Development Environment

* Go. Install on OS X with `brew install go`.
* `thrift`. Install on OS X with `brew install thrift`)
* `thrift-gen`. Install with `go get github.com/uber/tchannel-go/thrift/thrift-gen`

## Checking out the code

Make sure the repository is cloned to the correct location:

```bash
go get github.com/uber/cadence/...
cd $GOPATH/src/github.com/uber/cadence
```

## Dependency management

Dependencies are tracked via `glide.yaml`. If you're not familiar with `glide`,
read the [docs](https://github.com/Masterminds/glide#usage).

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

Before running the tests you must have `cassandra` running locally:

```bash
# for OS X
brew install cassandra

# start cassandra
/usr/local/bin/cassandra
``` 

Run all the tests:

```bash
make test
```
