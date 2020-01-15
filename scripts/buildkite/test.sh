#!/bin/bash

set -ex

echo $GOPRIVATE
echo $SSH_AUTH_SOCK

echo "Known Hosts"
cat /root/.ssh/known_hosts