#!/bin/sh

set -eu

build_info_data_file="build/info/data.json"

git_revision=$(git rev-parse --short HEAD) # "6cbfa2a3a"
build_time_unix=$(date '+%s')              # seconds since epoch

echo '{"gitRevision":"'"${git_revision}"'","buildTimeUnix":'"${build_time_unix}"'}' > "${build_info_data_file}"
