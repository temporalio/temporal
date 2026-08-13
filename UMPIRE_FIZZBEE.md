# Umpire and FizzBee

Review date: 2026-08-13.

## Recommendation

FizzBee is a good candidate for a fourth generated Umpire verification language/target. Its
Python-like specifications and interactive state exploration could make the bounded protocol easier
to review; communication and sequence diagrams become useful only in a later role-based target. It
should enter Umpire as a generated `fizz-semantic` target beside TLA+, P, and Ivy, not as a new
authoritative model and not initially through FizzBee's separate model-based testing product.

There is no FizzBee integration in this repository today: the canonical protocol is compiled into
Umpire's [verification IR](./common/testing/umpire/verify/model.go), and the checked-in generated
targets are only [TLA+](./tests/umpire2/genmodels/tla), [P](./tests/umpire2/genmodels/p), and
[Ivy](./tests/umpire2/genmodels/ivy). The design described in
[UMPIRE.md](./UMPIRE.md), [UMPIRE_VERIFY.md](./UMPIRE_VERIFY.md), and
[UMPIRE_SPLIT.md](./UMPIRE_SPLIT.md) already provides the correct integration seam:

```text
tests/umpire2/protocol
          |
          v
validated verification snapshot
    |          |          |          |
    v          v          v          v
  TLA+         P         Ivy      FizzBee
                                      |
                                      v
                         normalized verification result
```

The first target should preserve the IR's current atomic transition semantics. A later,
deliberately separate `fizz-actors` target could use FizzBee roles, non-atomic actions, message
loss, and crashes to model the task-delivery foundation proposed in
[UMPIRE_SPLIT.md](./UMPIRE_SPLIT.md). That target would be an execution refinement, not a more
convenient spelling of the same atomic kernel.

## What FizzBee provides

FizzBee is an Apache-2.0-licensed formal specification language and model checker for distributed
systems. Its language is Python-like and its expressions are based on Starlark; the project
provides a browser playground, macOS/Linux binaries, Homebrew installation, and a local `fizz`
command. The latest published release at the time of this review is v0.5.2, dated May 22, 2026.
See the official [project README](https://github.com/fizzbee-io/fizzbee/tree/993a2caa70d25996717a1d99ef26e7e682320649#fizzbee),
[release](https://github.com/fizzbee-io/fizzbee/releases/tag/v0.5.2), and
[license](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/LICENSE).

The features relevant to Umpire are:

- Actions can be atomic, serial, parallel, or nondeterministic `oneof` blocks. A serial action has
  implicit yield points at which other operations can interleave and the current operation can
  stop, while an atomic action is one checker step. This makes atomicity visible in source, but it
  also makes an accidental missing `atomic` semantically significant. See FizzBee's
  [getting-started guide](https://fizzbee.io/design/tutorials/getting-started/) and
  [procedural two-phase-commit explanation](https://fizzbee.io/design/examples/two_phase_commit_procedural/).
- Roles group state, actions, and functions and may represent a service, process, thread, database,
  or abstract protocol participant. Roles can be created dynamically and communicate through
  function calls. See the official [roles guide](https://fizzbee.io/design/tutorials/roles/).
- `always` assertions express safety. FizzBee also supports a subset of LTL for liveness and
  distinguishes unfair, weakly fair, and strongly fair actions and choices. Fairness is not
  implicit. See the official [liveness and fairness guide](https://fizzbee.io/design/tutorials/liveness/).
- `require` is an enabling condition; `any` and `oneof` introduce nondeterministic choice. This is
  a close surface match for Umpire action guards, typed binding enumeration, and branches. See the
  official [guard-clause guide](https://fizzbee.io/design/tutorials/guard-clause/).
- Non-atomic execution can inject message loss, network partitions, thread/process crashes, and
  loss of state annotated as ephemeral. Message duplication, Byzantine behavior, disk corruption,
  and memory corruption still require explicit modeling. See the official
  [fault-injection guide](https://fizzbee.io/design/tutorials/fault-injection/).
- Symmetric values and symmetric roles can reduce equivalent states, and order-independent
  collections avoid artificial permutations. See the official
  [symmetry-reduction guide](https://fizzbee.io/design/tutorials/symmetry_reduction/).
- The explorer can render state graphs, role communication diagrams, and per-path sequence
  diagrams from the same specification. See the official
  [visualization guide](https://fizzbee.io/design/tutorials/visualizations/).
- State-space controls include maximum action occurrences, maximum concurrent actions,
  action-specific bounds, deadlock detection, crash-on-yield behavior, and whether checking
  continues after an invariant failure. The documented defaults include 100 actions and two
  concurrent actions. See the official
  [configuration guide](https://fizzbee.io/design/tutorials/frontmatter/) and the authoritative
  [`StateSpaceOptions` schema](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/proto/statespace_options.proto).

These features make FizzBee attractive for bounded design exploration and review. They do not
change Umpire's assurance boundary: a generated FizzBee run checks an abstraction, not the live
Temporal server or the correctness of Umpire's facts, realizers, observation windows, and
refinement mappings.

## Why FizzBee should not become the source model

Umpire's source is more than a transition system. The compiled protocol owns facts and entity
subscriptions, lifecycle declarations, typed relations, live actions and explicit action gaps,
sparse-regression capabilities, causal footprints, verification properties, and mappings between
abstract actions and observations. The verification snapshot is only one derived view.

A `.fizz` file can describe states, actions, assertions, and role interactions, but it does not by
itself declare Umpire's Go fact decoders, live `Realizer` implementations, observation schemas, or
regression vocabulary. Making it authoritative would therefore require a second metadata system or
leave part of the protocol authoritative in Go. Either result violates Umpire's one-semantic-source
principle.

FizzBee also permits general Starlark-like expressions and imperative bodies. Importing arbitrary
FizzBee source into the current typed verification IR would require a restricted front end and a
second semantic IR capable of representing local variables, program counters, role calls, yield
points, and fault behavior. It would not be a parser-only change. FizzBee's own
[current-limitations page](https://fizzbee.io/design/tutorials/limitations/) documents expression
and function-call restrictions and warns that extracting a local variable into its own statement
can change concurrency behavior.

Generation in the other direction is contained: Umpire already has a small, validated expression
algebra, finite identity pools, explicit effects, and deterministic normalization. A FizzBee
exporter can reject anything it cannot preserve, following the same rule as the existing exporters.

FizzBee could become an authoring front end later, but only as a deliberately restricted Umpire
dialect that compiles into the same validated IR and also supplies the runtime protocol metadata.
That route is credible only if it eliminates, rather than creates, companion Go declarations for
facts, realizers, relations, refinements, and regression capabilities. The canonical semantic
boundary would still be Umpire's validated compiler output; arbitrary `.fizz` programs should not
become inputs to the other exporters.

## Mapping the current IR

The initial generator should use top-level finite data and atomic actions rather than roles. That
shape most directly preserves the current Go interpreter and one-world P semantics.

| Umpire verification construct | Initial FizzBee lowering |
| --- | --- |
| Entity type and finite `IDs` | A fixed ID collection plus maps recording existence and lifecycle state |
| `InitiallyExists` and initial state | Assign the existence/state maps in `Init` |
| One/many relation | A set of source/target pairs; generated assertions enforce endpoint existence and cardinality |
| Input parameter | `oneof` value from the declared finite type, constrained by the generated action guard |
| Fresh parameter | `oneof` identity whose existence bit is false |
| Observed parameter | Treat as a typed abstract input in the formal target; retain its observation requirement in provenance |
| Guard expression | A generated predicate used by `require` |
| Deterministic effects | Compute the post-state from the pre-state, then commit it in one `atomic action` |
| Branches | `oneof` branches, each with its own precomputed post-state |
| Create effect | Set existence and the declared initial state for the selected fresh identity |
| State effect | Update the selected entity's state map |
| Relation add/remove | Add or remove the selected tuple |
| Safety property | `always assertion` plus implicit relation well-formedness/cardinality assertions |
| Quiescent property | `always assertion` guarded by a generated `CanStep` predicate; do not recast it as liveness |
| Progress property | Unsupported in the first slice; add it only when FizzBee can preserve the declared fairness explicitly |
| Unrealized action | A normal abstract environment action, retained rather than omitted |
| Refinement and provenance | Manifest/name maps and generated comments; checked by Umpire validation, not inferred by FizzBee |

Using post-state temporaries is important. Merely emitting a sequence of assignments inside an
atomic block prevents interleaving but still lets later expressions observe earlier writes. Umpire
effects are simultaneous and unspecified state is framed as unchanged, so the generator must
evaluate every right-hand side against the pre-state before committing any update.

The FizzBee model should not use serial or parallel actions in `fizz-semantic`. FizzBee's implicit
yield and crash behavior is valuable only when the Umpire profile intentionally represents those
steps. Enabling it in the semantic backend would add transitions absent from the shared IR and make
cross-backend comparison meaningless.

## Generated artifacts and runner

A natural checked-in layout is:

```text
tests/umpire2/genmodels/fizz/
  Umpire.fizz
  fizz.yaml
```

`common/testing/umpire/verify/fizz` should own deterministic generation, expression lowering, name
escaping, and unsupported-construct reporting. The existing generation command should add the two
files and include the FizzBee binary version and checksum in
[`manifest.json`](./tests/umpire2/genmodels/manifest.json). Suggested environment and command names
are consistent with the existing backends:

```text
UMPIRE_FIZZ_TOOL
make umpire-genmodels
make umpire-check-genmodels
make umpire-verify-smoke
make umpire-verify-nightly
```

The FizzBee shell command first parses `.fizz` to a JSON AST beside the source and then invokes the
Go checker. Its flags support a chosen output directory, BFS/DFS/random exploration, trace replay,
simulation, and symmetry controls. The wrapper source is authoritative for that behavior:
[`fizz`](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/fizz).
Umpire should invoke it on a temporary copy of the checked-in model so that the parser's adjacent
JSON file cannot dirty `genmodels`; native output should go to the requested result-artifact
directory.

The semantic target's generated configuration should set `max_concurrent_actions: 1`,
`crash_on_yield: false`, and `deadlock_detection: false`. Umpire actions are atomic, and terminal
quiescence is a valid condition checked by generated `CanStep` implications rather than a FizzBee
deadlock failure. `max_actions` should come from the selected Umpire profile and remain visible in
the normalized result.

The released wrapper has a critical CI behavior: the underlying model-checker process can print a
semantic `FAILED` or `DEADLOCK` result without returning a nonzero exit status. The v0.5.2 wrapper
works around this for parallel simulation by inspecting output, while its ordinary final invocation
does not. An Umpire runner must positively recognize the expected `PASSED` completion marker and
reject `FAILED` or `DEADLOCK`; process exit status alone is unsafe. See the official v0.5.2
[`fizz` parallel-result handling](https://github.com/fizzbee-io/fizzbee/blob/v0.5.2/fizz#L527-L530)
and [ordinary invocation](https://github.com/fizzbee-io/fizzbee/blob/v0.5.2/fizz#L609-L610).

Useful native artifacts include the parsed `spec_ast.json`, effective `state_config.json`,
`graph.dot`, `communication.dot`, `trace.txt`, and the error graph/HTML files produced on failure.
The FizzBee checker source shows these artifact names and also shows that CLI-level composition and
refinement take separate paths; currently only one refinement block is accepted. See the official
[`main.go`](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/main.go).

FizzBee also exposes a Go library that accepts its JSON AST and returns an in-memory state graph,
but the documented library performs seeded simulation, not the full CLI workflow, and explicitly
lacks composition/refinement and automatic artifact generation. Umpire should begin with a pinned
external CLI, as it does for the other foreign tools, rather than add a new third-party Go module.
See the official
[`pkg/modelchecker` documentation](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/pkg/modelchecker/README.md).

## Result normalization

FizzBee results must use Umpire's existing
[`Result`](./common/testing/umpire/verify/result.go) vocabulary rather than introduce a FizzBee
`passed` Boolean.

| FizzBee outcome | Umpire status |
| --- | --- |
| Parser/type-check only | `generated` |
| Invariant or deadlock violation with a recoverable path | `counterexample` |
| Clean run under explicit action/concurrency/identity bounds | `bounded-no-counterexample` |
| Timeout, memory exhaustion, interruption, malformed output, or checker crash | `inconclusive` |
| Source construct the exporter cannot preserve | `unsupported` |

Do not classify an ordinary clean FizzBee CLI run as `invariant-proved`. Also classify it as
`finite-exhaustive` only if a pinned checker version exposes a machine-readable completion signal
whose semantics Umpire tests. FizzBee's limits are part of the explored model, and its human output
uses `PASSED`/`FAILED`; conservatively reporting `bounded-no-counterexample` avoids claiming more
than Umpire can independently establish. The runner should always record identity bounds,
`max_actions`, `max_concurrent_actions`, crash-on-yield, deadlock policy, exploration strategy,
symmetry policy, fairness, tool version, native stdout/stderr, and the replay command.

Counterexample conversion needs a generated bidirectional name map. FizzBee action/role names and
IDs must map back to Umpire action names and typed bindings; state changes must map to
`StateDelta`s; and the native trace must remain attached when conversion is partial. FizzBee has
guided replay through `--trace-file`/`--trace`, implemented by its official
[`trace.go`](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/modelchecker/trace.go),
so Umpire can preserve both the normalized trace and an exact native replay command.

## Fit with Umpire's model family

FizzBee does not replace the existing backends; it adds a different review and exploration surface.

| Backend | Best Umpire role | FizzBee relationship |
| --- | --- | --- |
| Ivy | Parameterized relational safety and contracts in supported fragments | FizzBee does not replace an inductive Ivy proof |
| TLA+ | Reference transition semantics, bounded integration, refinement, fairness, and liveness | `fizz-semantic` should agree with it on shared finite safety worlds |
| P | Current atomic semantic cross-check; later actorized scheduling refinement | FizzBee roles provide another possible actor view, but with different RPC/crash semantics |
| FizzBee | Accessible bounded exploration, diagrams, and developer-facing counterexamples | Adds reviewability and an independent implementation of finite checking |

The model-family structure proposed in [UMPIRE_SPLIT.md](./UMPIRE_SPLIT.md) is still required.
FizzBee's current CLI contains composition and refinement implementations, but using those directly
would not supply Umpire's missing source-level `Module`, `Interface`, `RefinementMap`,
`Composition`, or `VerificationProfile` constructs. Umpire must first select and close a profile in
its own IR; a backend feature cannot decide which omitted actions stutter, remain environmental, or
invalidate a property.

For the later `fizz-actors` target, roles could correspond to a History shard, Matching partition,
poller, and synchronous response relay. Serial calls and durability annotations could then make
message loss, ambiguous results, retry, ownership changes, and crashes explicit. The target should
prove or bounded-check a refinement to the L0/L1 delivery contract and should retain the same split
used for `p-actors`: actorization is selected semantics, never an exporter side effect.

## FizzBee model-based testing

FizzBee Model-Based Testing is a separate product and binary. Its Go workflow generates a test
skeleton, maps model roles/actions to implementation adapters, optionally snapshots implementation
state, and runs sequential and concurrent scenarios. See the official
[MBT overview](https://fizzbee.io/testing/),
[mapping guide](https://fizzbee.io/testing/tutorials/getting-started/), and
[Go quick start](https://fizzbee.io/testing/tutorials/quick-start/).

That overlaps heavily with Umpire's existing Planner, Driver, `Realizer`, Monitor, fact log,
relation store, sparse regression compiler, reconciliation, and replay artifacts. Adopting it first
would create a second action/state adapter beside the canonical protocol and would not reuse
Umpire's richer observation model. It should therefore remain a later experiment.

MBT also treats `mbt.ErrNotImplemented` as a way to disable an unfinished action. That supports
incremental adoption, but it can make a run green while relevant behavior is absent. Any experiment
must fail a completeness/coverage gate when a selected Umpire action has no implemented adapter.
Moreover, FizzBee's CLI requires `--no-symmetry-reduction` when generating state graphs for MBT
replay from specifications that use symmetric roles or values, trading replayability for a larger
state space. See the official [Go MBT quick start](https://fizzbee.io/testing/tutorials/quick-start/)
and the wrapper's
[`--no-symmetry-reduction` documentation](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/fizz#L173-L177).

A useful experiment would export one small profile and implement adapters by delegating to existing
Umpire realizers, then compare scenario coverage and failure minimization. It is worth retaining
only if it finds concurrent live-system behavior that Umpire's own driver does not and the adapter
can be generated or mechanically validated. It should not become a hand-maintained second test
model.

## Implementation outline

### First deliverable

The first deliverable is one vertical `fizz-semantic` slice from the existing verification snapshot
to a normalized Umpire result. It includes deterministic generation, a pinned CLI runner, native
artifacts, counterexample normalization, tests, and an optional smoke job. It does not add roles,
non-atomic actions, implicit faults, symmetry, progress properties, FizzBee composition/refinement,
or MBT.

The slice is complete when:

- `make umpire-genmodels` creates a deterministic checked-in FizzBee target;
- `make umpire-check-genmodels` detects any drift in that target;
- a selected smoke run invokes the pinned FizzBee release and returns an honest Umpire `Result`;
- the FizzBee and Go interpreters agree on tiny reachable worlds;
- seeded semantic faults produce counterexamples; and
- every configured bound is recorded and cannot yield `finite-exhaustive`, while an ambiguous,
  timed-out, or malformed run cannot be reported as successful.

### Module and interface

Create a deep `common/testing/umpire/verify/fizz` module. Callers should not know FizzBee syntax,
configuration defaults, identifier rules, or file layout. Its small interface should mirror the
existing exporters:

```go
func Generate(model verify.Model) (map[string][]byte, []Diagnostic, error)
func RenderConfig(bounds verify.Bounds) ([]byte, error)
func ActionIdentifier(name string) string
func PropertyIdentifier(name string) string
```

`Generate` returns `Umpire.fizz` and diagnostics for constructs that the first slice cannot
preserve. `RenderConfig` requires a positive `MaxDepth`, maps it to `max_actions`, and always emits
the semantic controls `max_concurrent_actions: 1`, `crash_on_yield: false`,
`deadlock_detection: false`, and disabled native liveness. Identifier functions provide the single
name mapping used by generation, result parsing, and tests. Internally, the module should:

1. validate and canonically order the `verify.Model` before writing anything;
2. allocate every generated identifier in one pass and reject reserved-word or normalization
   collisions;
3. emit finite existence/state maps and relation sets in `Init`;
4. lower each source action to one top-level `atomic action`, including typed `oneof` bindings,
   freshness checks, guards, branches, and simultaneous framed effects;
5. emit relation well-formedness/cardinality assertions, declared safety assertions, and
   quiescent assertions guarded by a generated `CanStep`; and
6. report progress properties and any future unsupported expression/effect as diagnostics rather
   than omit or weaken them.

Do not introduce another general exporter or runner interface for this work. The exporter packages
already share a stable convention, and the runner's backend switch is an existing seam with several
adapters. A new abstraction used only by FizzBee would be shallow.

### Generated and runtime data flow

```text
verify.Model ---------------------> fizz.Generate ----------------> Umpire.fizz
verify.Bounds --------------------> fizz.RenderConfig ------------> fizz.yaml
Umpire.fizz + fizz.yaml ----------> pinned fizz CLI --------------> native output
native output + generated names --> runner normalization ---------> verify.Result
```

Check in the smoke configuration at `tests/umpire2/genmodels/fizz/fizz.yaml` so direct local runs
have explicit bounds. For every verification run, render a fresh profile-specific configuration
from `runner.Request.Bounds`; never rely on FizzBee defaults. The runner should assemble a
self-contained temporary directory containing the checked-in model and selected config because the
FizzBee wrapper writes a parsed JSON file beside its input. It should also retain a self-contained
copy in the result artifacts so the replay command does not point into a deleted temporary
directory.

### Generator command wiring

Update `cmd/umpire-genmodels` as follows:

- add FizzBee v0.5.2 and per-platform release checksums to `pinnedTools`;
- merge `fizz.Generate` output under `tests/umpire2/genmodels/fizz`;
- convert generator diagnostics to manifest `Unsupported` entries with source provenance;
- render the checked-in smoke `fizz.yaml` from the existing smoke `verify.Bounds`;
- add `fizz` as an explicit smoke backend, but leave it out of `all` while the job is experimental;
- add `-fizz-tool` and `UMPIRE_FIZZ_TOOL`, failing before execution when no tool is configured;
- populate action/property maps with the FizzBee identifier functions; and
- set the requested tool version and model directory in `runnerRequest`.

Keep the existing Make targets for generation and drift checking. The experimental CI job should
invoke the existing command with `-backend fizz -profile smoke`; add FizzBee to `all` only when that
job becomes required, and do not add a parallel generator command.

### Runner adapter

Add `runner.Fizz` and an internal `executeFizz` adapter in
`common/testing/umpire/verify/runner`. The adapter should:

1. create a temporary working directory and copy `Umpire.fizz` into it;
2. render the selected `fizz.yaml` from the request bounds;
3. invoke the pinned wrapper in BFS model-checking mode with an explicit native output directory;
4. capture stdout, stderr, effective config, parsed AST, graphs, failure pages, and native trace;
5. preserve the exact model/config pair in the artifact directory and build replay against that
   pair; and
6. clean up the temporary directory on success, failure, timeout, or interruption.

Classification order is part of the interface. Timeout, interruption, resource limits, and an
unknown tool version are `inconclusive`. A recognized `FAILED` invariant with a recoverable path is
a `counterexample`, even when the process exits zero. A run is `bounded-no-counterexample` only when
the expected `PASSED:` marker is present and no `FAILED` or `DEADLOCK` marker appears. Missing or
contradictory markers are `inconclusive`; the exit code alone never establishes success.

Normalize native action names and nondeterministic choices through the generated name maps. Convert
adjacent native states to Umpire `StateDelta`s and preserve the failed property name. If any action,
binding, property, or state cannot be mapped without guessing, retain the native evidence and return
`inconclusive` instead of emitting a partial counterexample as authoritative.

### Tests

Add tests at the same seams callers use:

| Location | Cases |
| --- | --- |
| `verify/fizz/generate_test.go` | Atomicity, simultaneous effects and framing, input/fresh bindings, branches, relation cardinality, `CanStep`, quiescence, deterministic ordering, escaping/collisions, and unsupported progress |
| `verify/fizz/generate_test.go` with `UMPIRE_FIZZ_TOOL` | Generated source parses; passing and failing tiny models produce the expected checker markers |
| `verify/runner/runner_test.go` | Command/config construction, temporary cleanup, zero-exit `FAILED`, positive `PASSED`, conflicting/missing markers, timeout/limits, artifact retention, trace normalization, and replay |
| `cmd/umpire-genmodels/main_test.go` | Complete artifact set, manifest tool/unsupported entries, backend selection, missing-tool errors, checked-in drift, seeded mutations, and Go/FizzBee reachable-world agreement |

Use the smallest models that distinguish semantics. In particular, include an action with multiple
state and relation effects, two identities for freshness/cardinality, a terminal state with a
failing quiescent property, and action names that collide after naive normalization.
Tool-backed tests should skip only when `UMPIRE_FIZZ_TOOL` is unset; pure generator and classifier
tests must always run.

The focused verification commands are:

```sh
go test -tags test_dep ./common/testing/umpire/verify/fizz/...
go test -tags test_dep ./common/testing/umpire/verify/runner/... ./cmd/umpire-genmodels/...
make umpire-check-genmodels
make lint-code
```

### Rollout and deferred work

Land generation and pure tests before enabling foreign-tool CI. Add a non-blocking smoke job with
the pinned binary next; promote it to a required check only after reachable-world equivalence,
mutation tests, result parsing, and at least one tool upgrade have remained stable. Enable a nightly
profile only after smoke resource use is measured and its additional bound exercises states that
smoke does not.

`fizz-actors`, role diagrams, native fault injection, progress/fairness, composition/refinement,
symmetry, and MBT remain separate follow-up decisions. None should enter the first slice merely
because FizzBee supports it.

## Validation plan

The integration is ready to trust only after all of the following pass:

1. Generate a tiny model containing creation, state changes, relations, input/fresh bindings,
   multiple branches, and framing; compare its reachable states and transitions with the pure Go
   interpreter.
2. Lower safety and quiescent properties and verify both passing and deliberately failing models.
   Confirm that quiescence means no source action is enabled, not merely that the FizzBee scheduler
   stops at a configured bound.
3. Seed the same semantic mutations used for the current exporters: missing guard, sequential
   instead of simultaneous effects, reversed cardinality, missing branch, reused identity, weakened
   property, and incorrect relation endpoint. FizzBee must expose each relevant mutation.
4. Test deterministic generation, identifier escaping, reserved words, stable ordering, and exact
   checked-in output.
5. Test runner classification for parse failure, invariant violation, deadlock, timeout, killed
   process, clean bounded completion, and output from an unknown tool version.
6. Normalize a counterexample and replay it with the recorded native command. Keep the native
   artifact if any action, binding, or delta cannot be translated.
7. Run the smallest non-vacuous Umpire profiles: two identities for duplicate/aliasing properties,
   two owner generations for fencing, and two queues or namespaces for isolation. A one-identity
   smoke model is useful for wiring but cannot establish these claims.
8. Pin the exact FizzBee binary and checksum in CI; regenerate the model and re-run mutation and
   reachable-world equivalence tests on every upgrade.

Documentation-only findings do not warrant running Temporal's Go unit or lint suite. Implementing
the backend would require targeted generator/runner tests with `-tags test_dep`,
`make umpire-check-genmodels`, the FizzBee verification profile, and `make lint-code`.

## Risks and controls

| Risk | Consequence | Required control |
| --- | --- | --- |
| A generated action is accidentally non-atomic | FizzBee explores crashes/interleavings absent from the shared IR | Emit `atomic action` mechanically and reject backend-native bodies in `fizz-semantic` |
| Sequential assignments weaken simultaneous effects | FizzBee and Umpire reach different states while action names agree | Compute a complete post-state from pre-state before committing |
| Default bounds are accepted silently | A green run appears broader than it is | Generate explicit per-profile configuration and record it in every result |
| FizzBee `PASSED` is overclassified | A bounded exploration becomes a proof claim | Default to `bounded-no-counterexample`; require tested evidence for `finite-exhaustive` |
| Symmetry identifies semantically distinct IDs | Routing, ownership, or relation bugs disappear | Enable symmetry only for source-declared interchangeable sorts and cross-check without it on tiny models |
| Implicit fault injection changes the kernel | Cross-backend disagreements reflect different semantics rather than bugs | Disable crash-on-yield for `fizz-semantic`; isolate faults in `fizz-actors` |
| General Starlark leaks into generation | Escaping bugs or unexpected evaluation alter the model | Generate from typed IR only, validate identifiers/literals, and never interpolate untrusted runtime data |
| State-space growth at 10x identities/actions | Memory and time rise combinatorially | Keep independent bounded profiles, use source-justified symmetry, and separate semantic from actor targets |
| Checker/tool behavior changes | Results or parsers silently drift | Pin release plus checksum, retain native output, and gate upgrades on equivalence/mutation tests |
| FizzBee remains a young, changing tool | Diagnostics, APIs, or unsupported language corners impede CI | Begin as optional experimental backend and treat malformed/unknown output as `inconclusive` |

FizzBee's own documentation calls the implementation a work in progress and currently notes
incorrect diagnostic line numbers plus restrictions on where Fizz functions may be called. The
official reference material is also evolving; generated code must target a pinned release rather
than assume that the website's current syntax matches every installed binary. See the official
[limitations](https://fizzbee.io/design/tutorials/limitations/) and
[language gotchas](https://github.com/fizzbee-io/fizzbee/blob/993a2caa70d25996717a1d99ef26e7e682320649/examples/references/GOTCHAS.md).

Security exposure is limited if FizzBee remains a build-time tool: it need not run in Temporal
production or receive credentials. CI should execute the pinned binary with a timeout in a clean
working directory, without network access or secrets, and publish only bounded generated state and
semantic identifiers. Umpire's existing rule that causal artifacts contain no secret payloads
still applies.

The practical verdict is **adopt experimentally as a generated semantic backend**. FizzBee's
accessibility and visual counterexamples complement Umpire well, while Umpire's canonical Go
protocol, verification IR, live observation, and normalized assurance vocabulary should remain in
control of what is modeled and what a result is allowed to claim.
