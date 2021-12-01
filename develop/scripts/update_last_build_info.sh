#!/bin/sh

set -eu

last_build_info_file="build/info/last.json"

git_revision=$(git rev-parse --short HEAD) # "6cbfa2a3a"
build_time_unix=$(date '+%s')              # seconds since epoch

echo '{"gitRevision":"'"${git_revision}"'","buildTimeUnix":'"${build_time_unix}"'}' > "${last_build_info_file}"
