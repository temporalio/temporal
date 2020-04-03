#!/bin/bash

set -exo pipefail

curl -H 'Cache-Control: no-cache' https://raw.githubusercontent.com/fossas/fossa-cli/master/install.sh | bash -s -- -b ~/

~/fossa init
~/fossa analyze

# Capture the exit status
EXIT_STATUS=$?

echo "fossa script exits with status $EXIT_STATUS"
exit $EXIT_STATUS