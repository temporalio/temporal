#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
destination="$repo_root/.bin/umpire-tools"
mkdir -p "$repo_root/.bin"

metadata="$(mktemp)"
staging="$(mktemp -d "$repo_root/.bin/umpire-tools.XXXXXX")"
cleanup() {
  rm -f "$metadata"
  if [[ -d "$staging" ]]; then
    rm -rf "$staging"
  fi
}
trap cleanup EXIT

go run -tags test_dep ./cmd/umpire-genmodels -mode tool-environment >"$metadata"
source "$metadata"

java_tool="$(command -v java)"
dotnet_tool="$(command -v dotnet)"
python_tool="$(command -v python)"
java_home="${JAVA_HOME:?mise did not provide JAVA_HOME}"
dotnet_root="${DOTNET_ROOT:?mise did not provide DOTNET_ROOT}"
metadata_checksum="$(shasum -a 256 "$metadata" | cut -d ' ' -f 1)"
installer_checksum="$(shasum -a 256 "${BASH_SOURCE[0]}" | cut -d ' ' -f 1)"
stamp="$(printf '%s\n' "$metadata_checksum" "$installer_checksum" "$java_tool" "$java_home" "$dotnet_tool" "$dotnet_root" "$python_tool" | shasum -a 256 | cut -d ' ' -f 1)"

if [[ -f "$destination/.stamp" ]] && [[ "$(<"$destination/.stamp")" == "$stamp" ]] &&
  [[ -x "$destination/$UMPIRE_TOOL_APALACHE_EXECUTABLE" ]] &&
  [[ -x "$destination/$UMPIRE_TOOL_FIZZBEE_EXECUTABLE" ]] &&
  [[ -x "$destination/$UMPIRE_TOOL_IVY_EXECUTABLE" ]] &&
  [[ -x "$destination/$UMPIRE_TOOL_P_EXECUTABLE" ]] &&
  [[ -f "$destination/$UMPIRE_TOOL_TLA2TOOLS_EXECUTABLE" ]]; then
  printf 'Umpire verification tools are already installed in %s\n' "$destination"
  exit 0
fi

mkdir -p "$staging/downloads" \
  "$staging/$UMPIRE_TOOL_APALACHE_EXTRACT_ROOT" \
  "$staging/$UMPIRE_TOOL_FIZZBEE_EXTRACT_ROOT" \
  "$staging/$UMPIRE_TOOL_P_EXTRACT_ROOT" \
  "$staging/$UMPIRE_TOOL_TLA2TOOLS_EXTRACT_ROOT"

download() {
  local url="$1"
  local output="$2"
  local checksum="$3"
  curl --fail --location --silent --show-error "$url" --output "$output"
  printf '%s  %s\n' "$checksum" "$output" | shasum -a 256 --check
}

tla_archive="$staging/downloads/$UMPIRE_TOOL_TLA2TOOLS_ARCHIVE"
download "$UMPIRE_TOOL_TLA2TOOLS_URL" "$tla_archive" "$UMPIRE_TOOL_TLA2TOOLS_SHA256"
[[ "$UMPIRE_TOOL_TLA2TOOLS_ARCHIVE_TYPE" == "file" ]]
cp "$tla_archive" "$staging/$UMPIRE_TOOL_TLA2TOOLS_EXECUTABLE"

apalache_archive="$staging/downloads/$UMPIRE_TOOL_APALACHE_ARCHIVE"
download "$UMPIRE_TOOL_APALACHE_URL" "$apalache_archive" "$UMPIRE_TOOL_APALACHE_SHA256"
[[ "$UMPIRE_TOOL_APALACHE_ARCHIVE_TYPE" == "zip" ]]
unzip -q "$apalache_archive" -d "$staging/$UMPIRE_TOOL_APALACHE_EXTRACT_ROOT"

p_archive="$staging/downloads/$UMPIRE_TOOL_P_ARCHIVE"
download "$UMPIRE_TOOL_P_URL" "$p_archive" "$UMPIRE_TOOL_P_SHA256"
[[ "$UMPIRE_TOOL_P_ARCHIVE_TYPE" == "nuget" ]]
"$dotnet_tool" tool install "$UMPIRE_TOOL_P_PACKAGE" \
  --version "$UMPIRE_TOOL_P_VERSION" \
  --tool-path "$staging/$UMPIRE_TOOL_P_EXTRACT_ROOT" \
  --add-source "$staging/downloads" \
  --ignore-failed-sources

ivy_archive="$staging/downloads/$UMPIRE_TOOL_IVY_ARCHIVE"
download "$UMPIRE_TOOL_IVY_URL" "$ivy_archive" "$UMPIRE_TOOL_IVY_SHA256"
[[ "$UMPIRE_TOOL_IVY_ARCHIVE_TYPE" == "wheel" ]]
"$python_tool" -m venv "$staging/$UMPIRE_TOOL_IVY_EXTRACT_ROOT"
"$staging/$UMPIRE_TOOL_IVY_EXTRACT_ROOT/bin/pip" install --quiet "$ivy_archive"
"$python_tool" -c 'import pathlib, sys
root, old, new = pathlib.Path(sys.argv[1]), sys.argv[2].encode(), sys.argv[3].encode()
for path in root.iterdir():
    if path.is_file():
        contents = path.read_bytes()
        if old in contents:
            path.write_bytes(contents.replace(old, new))' \
  "$staging/$UMPIRE_TOOL_IVY_EXTRACT_ROOT/bin" "$staging" "$destination"

fizz_archive="$staging/downloads/$UMPIRE_TOOL_FIZZBEE_ARCHIVE"
download "$UMPIRE_TOOL_FIZZBEE_URL" "$fizz_archive" "$UMPIRE_TOOL_FIZZBEE_SHA256"
[[ "$UMPIRE_TOOL_FIZZBEE_ARCHIVE_TYPE" == "tar.gz" ]]
tar -xzf "$fizz_archive" --strip-components=1 -C "$staging/$UMPIRE_TOOL_FIZZBEE_EXTRACT_ROOT"

[[ -x "$staging/$UMPIRE_TOOL_APALACHE_EXECUTABLE" ]]
[[ -x "$staging/$UMPIRE_TOOL_FIZZBEE_EXECUTABLE" ]]
[[ -x "$staging/$UMPIRE_TOOL_IVY_EXECUTABLE" ]]
[[ -x "$staging/$UMPIRE_TOOL_P_EXECUTABLE" ]]
[[ -f "$staging/$UMPIRE_TOOL_TLA2TOOLS_EXECUTABLE" ]]
[[ -x "$java_tool" ]]

{
  printf 'export UMPIRE_APALACHE_TOOL=%q\n' "$destination/$UMPIRE_TOOL_APALACHE_EXECUTABLE"
  printf 'export UMPIRE_FIZZ_TOOL=%q\n' "$destination/$UMPIRE_TOOL_FIZZBEE_EXECUTABLE"
  printf 'export UMPIRE_IVY_TOOL=%q\n' "$destination/$UMPIRE_TOOL_IVY_EXECUTABLE"
  printf 'export UMPIRE_JAVA_TOOL=%q\n' "$java_tool"
  printf 'export UMPIRE_P_TOOL=%q\n' "$destination/$UMPIRE_TOOL_P_EXECUTABLE"
  printf 'export UMPIRE_TLA_JAR=%q\n' "$destination/$UMPIRE_TOOL_TLA2TOOLS_EXECUTABLE"
  printf 'export JAVA_HOME=%q\n' "$java_home"
  printf 'export DOTNET_ROOT=%q\n' "$dotnet_root"
  printf 'export PATH=%q:%q:$PATH\n' "$java_home/bin" "$dotnet_root"
} >"$staging/env"
printf '%s\n' "$stamp" >"$staging/.stamp"

if [[ -d "$destination" ]]; then
  rm -rf "$destination"
fi
mv "$staging" "$destination"
printf 'Installed Umpire verification tools in %s\n' "$destination"
