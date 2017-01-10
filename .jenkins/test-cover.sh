#!/bin/bash

set -e

SOURCE=${1:-.}
TARGET=${2:-profile.cov}

rm $TARGET &>/dev/null || true
echo "mode: count" > $TARGET

# Standard go tooling behavior is to ignore dirs with leading underscores
for DIR in $(find $SOURCE -maxdepth 10 -not -path '*/.git*' -not -path '*/_*' -type d);
do
    if ls $DIR/*.go &> /dev/null; then
        godep go test $RACE -v -coverprofile=$DIR/profile.tmp $DIR
        if [ -f $DIR/profile.tmp ]
        then
            cat $DIR/profile.tmp | tail -n +2 >> $TARGET
            rm $DIR/profile.tmp &>/dev/null
        fi
    fi
done
