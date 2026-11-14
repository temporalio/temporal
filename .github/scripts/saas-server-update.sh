#!/usr/bin/env bash
set -euo pipefail

report() {
  printf '%s\n' "$*"
  if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
    printf '%s\n' "$*" >>"$GITHUB_STEP_SUMMARY"
  fi
}

fail() {
  report "Incomplete SaaS updater trigger: $*" >&2
  exit 1
}

enumerate() {
  local range before after revision resolved
  [[ ${GITHUB_EVENT_NAME:-} == push && ${GITHUB_REPOSITORY:-} == temporalio/temporal ]] ||
    fail "expected an upstream main push"
  range=$(jq -er '
    if .repository.full_name != "temporalio/temporal" or .ref != "refs/heads/main" or
       (.deleted // false) != false then error("expected an upstream main push") else . end |
    [.before, .after] |
    if all(.[]; type == "string" and test("^[0-9a-f]{40}$") and test("^0+$") == false)
    then @tsv else error("invalid immutable push range") end
  ' "${GITHUB_EVENT_PATH:?GITHUB_EVENT_PATH is required}") || fail "invalid main push payload or range"
  IFS=$'\t' read -r before after <<<"$range"
  for revision in "$before" "$after"; do
    resolved=$(git rev-parse --verify --end-of-options "${revision}^{commit}") || fail "unavailable commit ${revision}"
    [[ $resolved == "$revision" ]] || fail "not a commit: ${revision}"
  done
  git merge-base --is-ancestor "$before" "$after" || fail "non-forward or unresolved range ${before}..${after}"
  git rev-list --reverse --topo-order "${before}..${after}" || fail "cannot enumerate ${before}..${after}"
}

mode=${1:-}
[[ $mode == enumerate || $mode == dispatch ]] || fail "usage: $0 {enumerate|dispatch}"
export GIT_NO_REPLACE_OBJECTS=1
if [[ $mode == dispatch && ${SAAS_SERVER_UPDATES_ENABLED:-false} != true ]]; then
  report "SaaS updater dispatch is disabled"
  exit 0
fi
candidates=$(enumerate) || exit 1
if [[ $mode == enumerate ]]; then
  if [[ -n $candidates ]]; then printf '%s\n' "$candidates"; fi
  exit 0
fi
if [[ -z $candidates ]]; then
  report "No newly landed main commits to dispatch"
  exit 0
fi
: "${GH_TOKEN:?GH_TOKEN is required for enabled dispatch}"
report "OSS main range: $(jq -r '.before + ".." + .after' "$GITHUB_EVENT_PATH")"
result=0
while IFS= read -r candidate; do
  if gh workflow run build-server-next-branch.yml -R temporalio/saas-temporal \
    -r main -f "server_commit=${candidate}"; then
    report "Accepted dispatch: ${candidate}"
  else
    report "Dispatch not confirmed: ${candidate}"
    result=1
  fi
done <<<"$candidates"
if [[ $result -ne 0 ]]; then
  report "Incomplete: one or more dispatches were not confirmed; accepted SHAs are listed above"
fi
exit "$result"
