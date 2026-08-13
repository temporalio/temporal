# Umpire generated verification models

Umpire generates bounded TLA+, P, and Ivy models from the canonical Go protocol. Generated models
are derived verification views, not independently authored specifications.

```text
tests/umpire2/protocol
          |
          v
validated verification snapshot
    |          |          |
    v          v          v
  TLA+         P         Ivy
    \          |          /
     normalized verification result
```

The snapshot is the stable boundary. A pure Go interpreter and every exporter consume the same
immutable entities, identity bounds, relations, actions, choices, properties, refinements, and
provenance. Exporters reject unsupported semantics rather than weakening them silently.

## Source and generated artifacts

The source is the compiled default protocol in `tests/umpire2/protocol`, plus the explicit
verification properties and refinement mappings declared beside it. Go functions such as live
realizers and fact decoders remain outside the abstract kernel.

Deterministic generated files live under `tests/umpire2/genmodels`:

```text
manifest.json       bounds, versions, provenance, abstractions, and unsupported semantics
model.ir.json       normalized diagnostic form of the snapshot
tla/                TLA+ module and TLC/Apalache configurations
p/                  P world-machine model and project
ivy/                Ivy safety model
```

[`manifest.json`](./tests/umpire2/genmodels/manifest.json) is authoritative for the checked model's
current entity bounds, action and property inventory, tool versions, hashes, requested guarantee,
abstractions, and exclusions. Do not duplicate those mutable values in prose.

Native traces, schedules, logs, and normalized run results belong in CI or local result artifacts,
not in the generated source directory.

## Commands

```sh
make umpire-genmodels          # rewrite checked-in generated artifacts
make umpire-check-genmodels    # regenerate in a temporary directory and require no diff
make umpire-verify-smoke       # run the bounded pull-request profile
make umpire-verify-nightly     # run the larger scheduled profile
```

The runner reads pinned tools from:

```text
UMPIRE_TLA_JAR
UMPIRE_JAVA_TOOL
UMPIRE_APALACHE_TOOL
UMPIRE_P_TOOL
UMPIRE_IVY_TOOL
```

CI installs the pinned versions and checksums recorded in the manifest. A tool upgrade must update
the manifest and generated artifacts explicitly.

## Shared semantics

The snapshot represents a finite relational transition system:

- entity identities come from explicit finite pools;
- actions have typed bindings, guards, simultaneous state and relation effects, and finite choices;
- fresh creation consumes an unused identity;
- unspecified variables are framed as unchanged;
- observation-only `NoOp` classifications are not exported as protocol behavior;
- safety properties are evaluated over initial and successor states; and
- progress is bounded or quiescent unless the source declares the fairness needed for a stronger
  temporal claim.

Lifecycle and sparse-regression vocabularies overlap but are not assumed equivalent. Refinement
mappings connect them, and validation rejects inconsistent effects or missing source actions.
Actions without live realizers may remain explicit abstract environment actions; they are never
silently omitted.

The generated P model uses one world machine so mailbox semantics do not accidentally change the
atomic protocol. Actorized execution is a separate refinement problem. Ivy checks the supported
safety fragment. TLA+ is shared by TLC and Apalache for finite exploration and selected inductive
proof obligations.

## Result vocabulary

Verification never returns an unqualified `verified` Boolean.

| Status | Claim |
| --- | --- |
| `generated` | Files parsed or type-checked; no behavioral claim |
| `bounded-no-counterexample` | No violation found within recorded depth, schedule, and resource bounds |
| `finite-exhaustive` | Every state of the configured finite instance was explored |
| `invariant-proved` | An inductive invariant was proved under recorded assumptions |
| `counterexample` | A property violation was found |
| `unsupported` | The selected backend cannot preserve a source construct |
| `inconclusive` | A timeout, limit, crash, interruption, or parse failure prevented a claim |

Hitting a timeout, depth, state, step, memory, or schedule limit cannot produce a success status.
Normalized counterexamples retain the backend, failed property, bounds, action sequence, bindings,
state and relation deltas, native artifacts, and replay command.

## Assurance boundary

Formal generation checks the abstract protocol, not the running Temporal server. The complete
assurance chain remains:

1. the protocol declares abstract behavior and properties;
2. snapshot validation checks that declaration and its refinements;
3. the interpreter and foreign backends explore the abstraction;
4. live realizers execute abstract actions against Temporal;
5. the Monitor evaluates normalized observations; and
6. semantic and causal-footprint reconciliation compare the implementation with the abstraction.

Exporter agreement is also not automatic evidence of correctness when all exporters share a bad
lowering. Tests compare tiny reachable worlds with the Go interpreter and seed semantic mutations
that each backend must expose. High-risk mutations include missing guards, missing frame clauses,
reversed relation cardinality, omitted choices, identity reuse, weakened properties, and incorrect
refinement mappings.

## Code map

| Location | Responsibility |
| --- | --- |
| `common/testing/umpire/verify` | Snapshot types, validation, interpreter, results, and manifests |
| `common/testing/umpire/verify/tla` | TLA+ generation |
| `common/testing/umpire/verify/p` | P generation |
| `common/testing/umpire/verify/ivy` | Ivy generation |
| `common/testing/umpire/verify/runner` | Foreign-tool execution and result normalization |
| `tests/umpire2/protocol/verification.go` | Temporal protocol adapter and refinements |
| `cmd/umpire-genmodels` | Deterministic generation and verification entry point |

Run the pure Go verification tests before invoking foreign tools:

```sh
go test -tags test_dep ./common/testing/umpire/verify/... ./tests/umpire2/protocol/...
make umpire-check-genmodels
```
