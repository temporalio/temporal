# Claude Review Comment Format

## Scope

Update the Claude review workflow prompt in
`.github/workflows/claude-review-teams.yml`. Do not change the shared review
skill or the workflow's eligibility and comment-posting behavior.

## Review Comment Contract

Every inline finding uses this structure:

```markdown
`severity` **[category]**: One-line summary.

**Issue:** Detailed explanation of the problem — what's wrong and why it matters.

**Suggestion:** Concrete fix or alternative.
```

The prompt defines four severities:

- `nit`: a stylistic, trivial, preference-based, non-blocking improvement.
- `small`: a minor naming, readability, or best-practice issue that does not
  affect correctness and is non-blocking.
- `med`: a moderate error-handling, edge-case correctness, test coverage, or
  design issue that is blocking.
- `high`: a serious security, data-loss, crash, race, broken-functionality, or
  architectural issue that is blocking.

The bot reports findings at all four severity levels. `nit` and `small`
findings keep the same structure but use proportionally shorter issue and
suggestion text.

## Tone

Comments are direct and practical, without fluff. They reference relevant
codebase patterns and utilities, give concrete alternatives, and explain why a
change matters.

## Verification

Run the repository's YAML formatting and GitHub Actions lint checks. Do not add
a source-text test for prompt prose because it would only detect intentional
wording changes rather than exercise reviewer behavior.
