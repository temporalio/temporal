---
name: mutation-test
description: Help plan, run, and analyze Temporal mutation-testing runs. Use when the user wants to mutation test a feature, choose source files and test files, build the make mutation-test command, run it after confirmation, or triage surviving mutants into test-worthy gaps, equivalent mutants, and low-value skips.
allowed-tools: Bash, Read, Grep, Glob, LS
---

# Temporal Mutation Testing

Use this skill from the repository root.

## Workflow

1. Ask what behavior or feature the user wants to mutation test unless it is already clear.
2. Inspect the codebase to propose source files to mutate and tests to keep.
3. Present the exact CLI command and wait for explicit confirmation before running it.
4. Run the command, monitor it for a reasonable time, then analyze `.testoutput/mutations/summary.txt`, `.testoutput/mutations/survivors.diff`, and `.testoutput/mutations/uncovered.txt`.
5. Summarize survivors with concrete next actions.

Do not commit changes for the user. If the relevant feature changes are uncommitted, explain that the mutation runner creates temporary git worktrees at `MUTATION_REF` (default `HEAD`), so uncommitted source and test edits will not be included. Ask the user to provide a committed ref or confirm they want to run against the current `HEAD`.

## Scope Discovery

Start with cheap discovery commands:

```bash
git status --short
git diff --name-only --diff-filter=ACMR HEAD -- '*.go'
rg --files <candidate-dir> | rg '_test\.go$'
rg -n '<type|function|method|error|state|metric name>' <candidate-dir-or-test-dir>
```

Select mutation source files conservatively:

- Include production `.go` files whose behavior the feature changes or depends on.
- Prefer package directories for cohesive feature areas, and individual files for broad packages.
- Exclude generated code, mechanical wiring, test helpers, mocks, and files where mutants are mostly compile-time or constant-only noise.
- Common excludes include generated files and names like `gen`, `fx.go`, `mock`, `.pb.go`, `.pb.*.go`, and other generated artifacts.

Select test files based on observable behavior:

- Include nearby unit tests for the mutated package.
- Include cross-package tests only when they assert the behavior under mutation.
- Avoid selecting the whole repo unless the feature genuinely needs it.
- Remember that the runner renames unselected `_test.go` files to `.excluded` in its temporary worktree, so narrow test selection can create false survivors.
- For `MUTATION_TEST_FILES`, a non-test `.go` file path also causes the runner to consider its paired `_test.go` file.

## Command

Build commands in this form:

```bash
make mutation-test \
  MUTATION_SOURCE_FILES='<source file/dir/glob list>' \
  MUTATION_SOURCE_EXCLUDE_FILES='<exclude file/dir/glob list>' \
  MUTATION_TEST_FILES='<test file/dir/glob list>' \
  MUTATION_TEST_TAGS='test_dep' \
  MUTATION_SHARD_LEVEL=4 \
  MUTATION_TIMEOUT=3m \
  MUTATION_RUN_TIMEOUT=30m
```

Use `MUTATION_REF='<ref>'` only when the user wants a ref other than `HEAD`. Increase `MUTATION_SHARD_LEVEL` for many source files if the machine can handle it. `MUTATION_TIMEOUT` limits each mutant's tests; `MUTATION_RUN_TIMEOUT` limits the whole run and defaults to unlimited (`0`).

Before running, show:

- The proposed command.
- A short rationale for source includes, excludes, and test files.
- Any risks, especially uncommitted changes not included by `HEAD` or a very narrow test set.

Then ask for confirmation. Do not run until the user confirms.

## Running

Run the confirmed command from the repo root. Capture nonzero exit codes as useful outcomes:

```bash
<confirmed command>
echo "mutation-test exit=$?"
```

Exit code `0` means all covered mutants were killed. Exit code `1` means at least one mutant survived or a target block was uncovered. Exit code `2` means a mutant was skipped, the baseline failed, or the runner encountered an infrastructure error.

Mutation runs can take a while. After about 20 minutes, inspect progress and report the latest killed/survived/skipped counts and log path. If the run is still active after about 30 minutes, give the user a concise status and ask whether to keep waiting unless the user already asked you to continue unattended.

## Survivor Analysis

Analyze these files first:

```bash
cat .testoutput/mutations/summary.txt
rg -n '^=== |^@@|^[-+][^-+]' .testoutput/mutations/survivors.diff
```

If `survivors.diff` is large, group by file and mutation pattern before reading entire sections.

Classify each survivor:

- **TEST**: The mutation changes externally observable behavior, error propagation, state transitions, timeout math, retry decisions, persistence effects, validation, or returned values. Recommend a specific test.
- **SKIP (equivalent)**: The mutated code is observably identical for reachable inputs, or tie behavior is intentionally irrelevant.
- **SKIP (low value)**: A test could kill it but would pin a tuning constant, log-only behavior, tracing-only behavior, generated code, or an implementation detail with poor signal.
- **INVESTIGATE**: The behavior may matter, but more code reading is needed before prescribing a test.

Prefer grouped recommendations over one test per survivor when a single table-driven or negative-path test can kill several mutants.

## Response Format

After a run, respond with:

```markdown
**Result**
<killed/survived/skipped counts, exit code, summary path>

**Survivors**
| File | Mutation | Verdict | Action |
|---|---|---|---|
| path.go:line | short mutation | TEST/SKIP/INVESTIGATE | concrete next step |

**Recommended Tests**
1. <test name/location and exact assertion>
2. <test name/location and exact assertion>
```

Keep actions concrete: name the target test file, setup needed to trigger the behavior, and the assertion that should fail under the survivor. Do not edit code or tests unless the user explicitly asks you to implement the recommendations.
