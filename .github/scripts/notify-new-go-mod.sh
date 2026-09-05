#!/usr/bin/env bash
# Detect go.mod files added by the commits in a push and emit a Slack message
# describing them.
#
# The message is written to stdout, and to $GITHUB_OUTPUT as `message` when
# running under Actions. `found` is set to true or false so the caller can
# decide whether to post.

set -euo pipefail

readonly NULL_SHA=0000000000000000000000000000000000000000
readonly DELIMITER=EOF_NOTIFY_NEW_GO_MOD

usage() {
  cat <<EOF
Usage: ${0##*/} [-hv]

Detect newly added go.mod files in the current push and build a Slack message.

  -h  Show this message and exit.
  -v  Enable shell tracing.

Required environment:
  BEFORE_SHA         commit the push started from (github.event.before)
  GITHUB_REPOSITORY  owner/repo, set by GitHub Actions
  GITHUB_SHA         commit the push landed on, set by GitHub Actions

Optional environment:
  GITHUB_OUTPUT      step output file; outputs are skipped when unset
  GITHUB_SERVER_URL  defaults to https://github.com
  GITHUB_RUN_ID      omits the workflow-run link when unset
EOF
}

while getopts ':hv' opt; do
  case $opt in
    h)
      usage
      exit 0
      ;;
    v)
      set -x
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
done

: "${BEFORE_SHA:?must be set}"
: "${GITHUB_REPOSITORY:?must be set}"
: "${GITHUB_SHA:?must be set}"

server_url=${GITHUB_SERVER_URL:-https://github.com}
repo_url=$server_url/$GITHUB_REPOSITORY

emit() {
  local found=$1
  local message=${2:-}

  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    {
      echo "found=$found"
      echo "message<<$DELIMITER"
      echo "$message"
      echo "$DELIMITER"
    } >> "$GITHUB_OUTPUT"
  fi
}

if [[ "$BEFORE_SHA" == "$NULL_SHA" ]]; then
  echo "Push created the branch; there is no base to compare against."
  emit false
  exit 0
fi

# A force push leaves the previous tip unreachable. Failing the job here would
# page RelEng about something harmless.
if ! git cat-file -e "$BEFORE_SHA^{commit}" 2>/dev/null; then
  echo "$BEFORE_SHA is not reachable (force push?); skipping." >&2
  emit false
  exit 0
fi

new_modules=()
while IFS= read -r file; do
  if [[ -z "$file" || "$(basename "$file")" != go.mod ]]; then
    continue
  fi
  # Fixture modules under testdata/ and vendored trees are not real modules.
  if [[ "$file" == testdata/* || "$file" == */testdata/* ]]; then
    echo "Ignoring test fixture module: $file"
    continue
  fi
  if [[ "$file" == vendor/* || "$file" == */vendor/* ]]; then
    echo "Ignoring vendored module: $file"
    continue
  fi
  new_modules+=("$file")
done < <(git diff --name-only --diff-filter=A "$BEFORE_SHA" "$GITHUB_SHA")

if [[ ${#new_modules[@]} -eq 0 ]]; then
  echo "No new go.mod files in this push."
  emit false
  exit 0
fi

printf 'Found %d new go.mod file(s):\n' "${#new_modules[@]}"
printf '  %s\n' "${new_modules[@]}"

message=":package: *New Go module(s) added to $GITHUB_REPOSITORY*"$'\n'
for module in "${new_modules[@]}"; do
  message+="• <$repo_url/blob/$GITHUB_SHA/$module|$module>"$'\n'
done
message+=$'\n'
message+="<$repo_url/compare/$BEFORE_SHA...$GITHUB_SHA|Compare>"
if [[ -n "${GITHUB_RUN_ID:-}" ]]; then
  message+=" · <$repo_url/actions/runs/$GITHUB_RUN_ID|Workflow run>"
fi

echo "--- Slack message ---"
echo "$message"

emit true "$message"
