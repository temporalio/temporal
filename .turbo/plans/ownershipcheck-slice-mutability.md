---
status: done
---

# Plan: Ownershipcheck Slice Mutability

## Context

The ownership linter currently treats borrowed maps and borrowed non-byte slices the same way: if a borrowed collection is embedded into an escaping sink proto, it reports. That is correct for maps because map mutation happens through the shared map object. It is too broad for some slices because a slice field can be managed by whole-slice replacement only. In that pattern, a response that captured the old slice header does not observe writes to a newly allocated replacement slice.

The target change is to keep map diagnostics strict while making slice diagnostics depend on whether the borrowed slice source may be mutated in place. This should suppress false positives like `GeneratorState.FutureActionTimes`, where current production code builds a fresh slice and assigns it wholesale, while still reporting direct in-place slice hazards such as append-to-field, index assignment, copy into the field, sorting, or passing the slice to a known mutating callee.

## Pattern Survey

### Analogous Features

- `tools/ownershipcheck/ownershipcheck.go` already separates ownership inference from reporting. `inferCtx.classify`, `flowCtx.classify`, and the generic `flow[T]` walker model whether an expression is borrowed, while `checkLiteralFields` and `checkFieldAssign` decide whether a borrowed value at a sink should report.
- Existing precision rules live near `tracked`, `reachesLeaf`, and fixture-specific tests. `isByteSlice`, `reachesLeaf`, and input-set resolution are examples of suppressing known false positives without weakening map diagnostics.
- Escape gating is already separate from value classification. `escapingVars` and `escapingLits` decide whether the destination proto can escape, which means slice-mutability should be another gate in the sink checks rather than a change to base ownership classification.
- The test suite uses `analysistest` package fixtures under `tools/ownershipcheck/testdata/src/<case>` with `// want` comments for expected diagnostics. Noise-reduction features have dedicated tests such as `TestByteSlice`, `TestInputSet`, and `TestReachesLeaf`.

### Reusable Utilities

- Use the existing `flow[T]` walker style for a new pre-pass where useful, but keep the result as package-level metadata on `checker` rather than embedding it into `own` or `binding`.
- Reuse `rootVar`, `calleeFunc`, `kindOf`, `isByteSlice`, `unparen`, and `typeName` helpers to resolve slice expressions and diagnose or classify mutation sites.
- Reuse existing sanitizer configuration for owned values; do not add clone-specific logic to the mutability pass except where it needs to recognize fresh slice assignment.
- Reuse `analysistest.Run` and fixture packages rather than adding bespoke unit tests around analyzer internals.

### Convention Anchors

- Keep the analyzer conservative when it cannot prove a slice is replace-only. Unknown mutation behavior should still report.
- Keep configuration domain-agnostic. The mutability rule should be expressed in terms of Go slice operations, not protobuf-specific field names.
- Keep diagnostics stable for maps and for clearly mutable slices. If wording changes, update fixtures intentionally.
- Keep comments explanatory and sparse, matching the existing section-header style in `ownershipcheck.go`.

### Proposed Alignment

- Add a slice-source mutability summary that is consulted only for borrowed slice sink checks.
- Treat maps exactly as today.
- Treat borrowed slices as reportable when the source is unknown or may be mutated in place.
- Treat borrowed slices as safe when the linter can prove the field is only replaced with fresh slices and never mutated in place.
- Add fixture packages that cover replace-only suppression, in-place mutation positives, unknown mutation positives, field-assignment sinks, and cross-package/accessor propagation where relevant.

## Implementation Steps

1. **Model slice sources separately from ownership**
   - In `tools/ownershipcheck/ownershipcheck.go`, add a small source identifier type for slice values, for example `sliceSource`, that can represent at least:
     - receiver-rooted field selections such as `g.FutureActionTimes`
     - receiver-rooted getter results that resolve to a known field, if practical in the first pass
     - unknown borrowed slice sources
   - Add a mutability enum such as `sliceReplaceOnly`, `sliceMutatesInPlace`, and `sliceUnknown`.
   - Add a `checker.sliceMutability map[sliceSource]sliceMutability` field populated before `c.infer()` / `c.report()` or at the start of `run`.
   - Keep this model independent from `own` and `binding`; ownership answers "is it borrowed?", while mutability answers "can this borrowed slice backing array be changed in place?"

2. **Collect in-place slice mutations**
   - Add a package scan function, for example `c.collectSliceMutability()`, that walks all function bodies in `pass.Files`.
   - Mark a source `sliceMutatesInPlace` for direct mutation patterns:
     - `x.F = append(x.F, ...)`
     - `x.F[i] = ...`
     - `x.F[a:b]` followed by mutation of the derived slice when the derived slice is tracked through a local
     - `copy(x.F, src)` and `copy(dst, x.F)` only for the destination case where the borrowed slice is written
     - known mutators such as `sort.Slice(x.F, ...)`, `slices.Sort(x.F)`, `slices.SortFunc(x.F, ...)`, and `slices.Reverse(x.F)` if available in the project toolchain
   - Track simple local aliases in the scan:
     - `s := x.F`; then `s = append(s, ...)`, `s[i] = ...`, `copy(s, src)`, or `sort.Slice(s, ...)` marks `x.F` mutable.
     - `s := x.F[:n]` should preserve the same source because a subslice shares the backing array.
   - Mark sources as `sliceUnknown` when a borrowed slice is passed to a callee not known to be read-only or mutating. This preserves conservative behavior for cases such as `mutate(x.F)`.
   - Do not mark `x.F = fresh` as mutable by itself. Whole-field replacement is the pattern we want to allow when all observed writes are replacements.

3. **Recognize replace-only fresh assignments**
   - During the same scan, record whole-field assignments of fresh slices:
     - `x.F = make([]T, ...)`
     - `x.F = []T{...}`
     - `tmp := make([]T, ...)`; `tmp = append(tmp, ...)`; `x.F = tmp`
     - sanitizer calls configured as owned, if the RHS is a slice and the call is already recognized by `isSanitizer`
   - Treat a source as `sliceReplaceOnly` only when every observed write to that source is a whole-field replacement with a fresh/owned slice and no in-place mutation or unknown mutating call exists.
   - For no observed writes in the current package, keep the source `sliceUnknown` unless the source is package-local enough to prove all writes are visible. This avoids suppressing fields maintained in another package.
   - For embedded generated proto fields or exported structs used cross-package, default to unknown unless all reachable writes are visible in the same package and no external mutator can write the field through an exported value.

4. **Gate borrowed slice reports at sink sites**
   - Add a helper such as `fc.shouldReportBorrowed(vt types.Type, expr ast.Expr, env ownEnv) bool`.
   - For `kindOf(vt) == "map"`, return true exactly as today.
   - For `kindOf(vt) == "slice"`, classify the expression as borrowed first, then resolve the slice source from the expression and consult `checker.sliceMutability`.
   - Suppress only when the resolved source is confidently `sliceReplaceOnly`.
   - Report when the source is `sliceMutatesInPlace` or `sliceUnknown`, or when source resolution fails.
   - Use this helper in both `flowCtx.checkLiteralFields` and `flowCtx.checkFieldAssign`.
   - Keep call-argument leak checks conservative unless the callee leak summary can carry slice mutability. In the first version, borrowed slices passed to leaking params should still report.

5. **Document the precision boundary**
   - Update `tools/ownershipcheck/README.md` to explain that map findings are always strict, while slice findings are suppressed only for fields proven replace-only.
   - Add a short note to `tools/ownershipcheck/testdata/src/knowngaps/README.md` only if any cases are intentionally deferred, such as interprocedural mutating methods or mutation through reflection.
   - Adjust the top-of-file comment in `tools/ownershipcheck/ownershipcheck.go` to avoid over-claiming that all borrowed slices are necessarily current crash hazards.

6. **Run against the motivating case**
   - Run `make lint-ownership` on the linter branch and confirm the scheduler `FutureActionTimes` finding is gone while the callback `Links` and `Header` findings still behave as expected.
   - If callback `Links` is also suppressed by the new rule, inspect whether the callback link slice is actually replace-only. If it is not proven replace-only, tighten source resolution or mutability classification before accepting the result.

## Verification

- `go test -tags test_dep ./tools/ownershipcheck`
  - Expected: all analyzer fixtures pass.
  - This is the primary verification for the new slice-mutability model.
- Add `tools/ownershipcheck/testdata/src/replaceonlyslice/replaceonlyslice.go`.
  - Include a struct field that is assigned only from `make` / local append-then-assign.
  - Include an escaping proto literal that embeds the borrowed slice.
  - Expected: no `// want` diagnostic.
- Add `tools/ownershipcheck/testdata/src/mutableslice/mutableslice.go`.
  - Cover `c.items = append(c.items, item)`.
  - Cover `c.items[0] = item`.
  - Cover `copy(c.items, src)`.
  - Cover `sort.Slice(c.items, ...)` or `slices.SortFunc(c.items, ...)` if the fixture can import the package under the test toolchain.
  - Expected: each borrowed embed has a `// want` diagnostic.
- Add `tools/ownershipcheck/testdata/src/slicealiasmutation/slicealiasmutation.go`.
  - Cover `items := c.items; items[0] = item`.
  - Cover `items := c.items[:1]; items = append(items, item)` or `items[0] = item`.
  - Expected: borrowed embed reports because an alias can mutate the backing array.
- Add `tools/ownershipcheck/testdata/src/unknownslicemutation/unknownslicemutation.go`.
  - Cover passing `c.items` to an opaque local function without a known summary.
  - Expected: borrowed embed reports conservatively.
- Extend `tools/ownershipcheck/testdata/src/fieldassign/fieldassign.go` or add a new `slicefieldassign` fixture.
  - Cover `resp.Items = c.items` for both replace-only and mutable sources.
  - Expected: replace-only is silent; mutable source reports.
- Keep existing fixtures passing:
  - `subslice` should still report when a borrowed subslice is embedded and the source is not proven replace-only.
  - `byteslice` should still suppress `[]byte` and report non-byte mutable slices.
  - `directembed`, `samepackageaccessor`, `crosspackageaccessor`, and `genericfield` should keep existing map behavior unchanged.
- Run `make lint-ownership`.
  - Expected: map findings remain; proven replace-only slice findings are suppressed.
  - Use this as an integration check after the fixture-level tests.
- Optional broader check: run `go test -tags test_dep ./tools/ownershipcheck ./cmd/tools/ownershipcheck`.
  - Expected: analyzer package and standalone singlechecker wrapper compile and pass.

## Context Files

- `tools/ownershipcheck/ownershipcheck.go` — core analyzer; ownership classification, flow walker, sink checks, tracked-kind logic, and directives live here.
- `tools/ownershipcheck/ownershipcheck_test.go` — test grouping and analyzer flag setup.
- `tools/ownershipcheck/README.md` — public behavior contract for diagnostics, directives, rollout, and precision rules.
- `tools/ownershipcheck/testdata/src/subslice/subslice.go` — existing positive case showing borrowed subslices share backing arrays.
- `tools/ownershipcheck/testdata/src/byteslice/byteslice.go` — existing noise-reduction fixture for slice precision.
- `tools/ownershipcheck/testdata/src/fieldassign/fieldassign.go` — existing field-assignment sink fixture.
- `chasm/lib/scheduler/generator.go` — motivating replace-only slice write pattern for `FutureActionTimes`.
- `chasm/lib/scheduler/scheduler.go` — motivating sink sites in `Describe` and `ListInfo`.
