#!/usr/bin/env bash
#
# reverse_replay.sh - verify that a version-ceiling-capped scheduler history written by the CURRENT
# binary still replays on an OLDER rollback target. This is the reverse direction of the in-package
# forward TestReplays (old history -> new binary): here we take a history the new binary produced
# with a ceiling and replay it against the exact server revision we would roll back to.
#
# It materializes the rollback revision with `git archive` (no network beyond the checkout already
# in this repo, no third-party deps), drops the capped fixture(s) into that tree's scheduler
# testdata, and runs that tree's own TestReplays with its own scheduler code.
#
# Usage:
#   ROLLBACK_REF=v1.32.0-161.2 ./reverse_replay.sh [fixture ...]
#
#   ROLLBACK_REF  git tag or commit SHA of the rollback target (must be fetched into this repo).
#                 Default below is the current rollback target; update it as that target moves.
#   fixture       one or more capped fixtures under this testdata dir. Defaults to the capped
#                 compatibility fixtures. Only pass fixtures whose recorded version the rollback
#                 target actually supports (e.g. a v12-capped fixture for a v12-capable target).
#
# Known gap: the OSS-1.20 (v1) floor is not covered here unless you set ROLLBACK_REF to a v1-era
# release and pass a v1-capped fixture; by default this checks the realistic rollback target only.
set -euo pipefail

ROLLBACK_REF="${ROLLBACK_REF:-v1.32.0-161.2}"
TESTDATA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$TESTDATA_DIR" rev-parse --show-toplevel)"
PKG_REL="service/worker/scheduler"

FIXTURES=("$@")
if [[ ${#FIXTURES[@]} -eq 0 ]]; then
  FIXTURES=(replay_version_ceiling_v12_active.json.gz)
fi

if ! git -C "$REPO_ROOT" rev-parse --verify --quiet "${ROLLBACK_REF}^{commit}" >/dev/null; then
  echo "reverse_replay: ref '${ROLLBACK_REF}' not found locally; fetch it first (e.g. git fetch upstream --tags)" >&2
  exit 2
fi
ROLLBACK_SHA="$(git -C "$REPO_ROOT" rev-parse "${ROLLBACK_REF}^{commit}")"

WORKTREE="$(mktemp -d)"
trap 'rm -rf "$WORKTREE"' EXIT
git -C "$REPO_ROOT" archive "$ROLLBACK_SHA" | tar -x -C "$WORKTREE"

DEST="$WORKTREE/$PKG_REL/testdata"
if [[ ! -d "$DEST" ]]; then
  echo "reverse_replay: $PKG_REL/testdata does not exist at $ROLLBACK_REF; the scheduler layout differs there" >&2
  exit 1
fi

echo "reverse_replay: rollback target ${ROLLBACK_REF} (${ROLLBACK_SHA})"
for f in "${FIXTURES[@]}"; do
  cp "$TESTDATA_DIR/$f" "$DEST/$f"
  echo "reverse_replay:   testing fixture $f"
done

# Run only the rollback tree's forward TestReplays; it globs testdata/replay_*.json.gz and will now
# include the capped fixtures we copied in, replaying them with that revision's scheduler binary.
( cd "$WORKTREE" && go test -tags disable_grpc_modules,test_dep "./$PKG_REL" -run '^TestReplays$' -count=1 )
echo "reverse_replay: PASS on ${ROLLBACK_REF} for: ${FIXTURES[*]}"
