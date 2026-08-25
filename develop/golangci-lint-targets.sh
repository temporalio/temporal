#!/bin/bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <base-revision>" >&2
  exit 2
fi

BASE_REV="$1"
if ! git rev-parse --verify --quiet "${BASE_REV}^{commit}" >/dev/null; then
  echo "Unknown base revision: $BASE_REV" >&2
  exit 2
fi

if ! git diff --quiet "$BASE_REV" -- go.mod go.sum go.work go.work.sum .github/.golangci.yml; then
  echo "./..."
  exit
fi

if git diff --name-status --find-renames "$BASE_REV" -- '*.go' | grep -Eq '^[DR]'; then
  echo "./..."
  exit
fi

targets=()
while IFS= read -r -d '' file; do
  directory="$(dirname "$file")"
  if [[ "$directory" == "." ]]; then
    targets+=(".")
  else
    targets+=("./$directory")
  fi
done < <(
  git diff --name-only --diff-filter=ACM -z "$BASE_REV" -- '*.go'
  git ls-files --others --exclude-standard -z -- '*.go'
)

if [[ ${#targets[@]} -gt 0 ]]; then
  printf '%s\n' "${targets[@]}" | LC_ALL=C sort -u | paste -sd ' ' -
fi
