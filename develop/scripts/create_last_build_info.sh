#!/bin/sh

set -eu

git_revision=$(git rev-parse --short HEAD) # "6cbfa2a3a"
build_time_unix=$(date '+%s')              # seconds since epoch

echo '{"gitRevision":"'"${git_revision}"'","buildTimeUnix":'"${build_time_unix}"'}' > "./build/last_build_info.json"
