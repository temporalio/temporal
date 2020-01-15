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

cat /root/.ssh/id_rsa