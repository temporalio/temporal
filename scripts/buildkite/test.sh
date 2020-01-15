#!/bin/bash

set -ex

echo $GOPRIVATE
echo $SSH_AUTH_SOCK

echo "Known Hosts"
cat /root/.ssh/known_hosts

ssh-add -l

mkdir -p /tmp
cd /tmp
git clone git@github.com:temporalio/temporal.git
cat ./temporal/README.md

cd ./temporal

git config --global url."git@github.com:".insteadOf "https://github.com/"

go mod download

cat /root/.ssh/id_rsa