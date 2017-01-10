#!/bin/bash

if [ -z $GOPATH ]; then
  export GOPATH=~/.gocode
  [ -d $GOPATH ] || mkdir $GOPATH
fi

export PATH="$PATH:$GOPATH/bin:/usr/local/go/bin"

export PACKAGE='code.uber.internal/devexp/minion'

rm -rf src/$PACKAGE
mkdir -p $(dirname src/$PACKAGE)
ln -s $PWD src/$PACKAGE

export GOPATH="$GOPATH:$PWD"

which go-junit-report >/dev/null || go get -u github.com/sectioneight/go-junit-report
which godep >/dev/null || go get github.com/tools/godep
GOPATH=$(godep path):$GOPATH

OS=`uname -s | tr [:upper:] [:lower:]`
if [ "$OS" == "linux" ]; then
    export TMPDIR="/dev/shm/test-cadence"
    rm -Rf $TMPDIR
    mkdir -p $TMPDIR
fi
