# Replacing Uber Fx in Temporal with generated startup wiring

Research date: 2026-08-15.

## Executive summary

Temporal can replace most production use of Uber Fx and Dig with generated, ordinary Go startup code. This would remove most dependency-graph reflection, reflective constructor calls, and runtime container bookkeeping from startup.

The replacement should not try to compile arbitrary `fx.Option` values. That surface is dynamic and opaque: options may be composed in functions, selected from runtime state, supplied by tests, or used to transform an existing graph. Reproducing all of it would recreate a runtime container.

The recommended boundary is:

- generate direct constructor calls for each production service graph;
- retain a small runtime lifecycle module for start, stop, rollback, cancellation, timeouts, logging, and error aggregation;
- replace arbitrary graph mutation with typed root inputs and typed overrides;
- commit generated output so ordinary builds do not depend on a working generator;
- isolate codegen specifications from generated implementations with complementary build tags;
- generate and validate replacements before touching the last known-good output.

There are external tools worth studying, but no maintained, production-proven package provides all the Fx behavior Temporal currently needs. The practical choices are either a maintained Wire fork plus Temporal-specific adapters, or a small Temporal-owned generator and lifecycle.

## Current Temporal architecture

Temporal pins [`go.uber.org/fx v1.24.0`](go.mod), which uses Dig for runtime dependency resolution.

The production boot path is a graph of graphs:

1. [`NewServerFx`](temporal/fx.go) constructs a top-level `fx.App`.
2. The top-level graph creates child apps for requested services.
3. History, matching, frontend, internal frontend, and worker each have a service graph.
4. [`ServiceProviderParamsCommon.GetCommonServiceOptions`](temporal/fx.go) realizes shared dependencies in the parent graph and supplies them into each child graph. The existing source describes this as a workaround.
5. [`ServerImpl`](temporal/server_impl.go) starts service apps in an explicit order and stops them in reverse order.

There are five `fx.New` call sites: the top-level app plus history, matching, generic frontend, and worker. The generic frontend builder may be called separately for frontend and internal frontend, so a process can construct up to six apps.

This nesting is useful for migration. A generated child service can continue exposing the same `Start(context.Context) error` and `Stop(context.Context) error` boundary while other services remain on Fx.

Fx also appears in generated-code infrastructure. [`cmd/tools/protoc-gen-go-chasm/main.go`](cmd/tools/protoc-gen-go-chasm/main.go) emits an Fx import, an `fx.Lifecycle` constructor parameter, and `fx.StopHook`. Fully removing the Fx module therefore requires updating and rerunning the CHASM generator.

## Usage inventory

An initial working-tree inventory, excluding `*_test.go` and the top-level `tests/**` directory, found:

| Construct | Count |
| --- | ---: |
| Go files importing Fx | 82 |
| `fx.Provide` | 264 |
| `fx.Invoke` | 42 |
| `fx.Options` | 40 |
| `fx.Module` | 11 |
| `fx.In` | 53 |
| `fx.Out` | 10 |
| `fx.Lifecycle` references | 37 |
| Optional input tags | 14 |
| `fx.Annotate` | 8 |
| `fx.Supply` | 5 |
| Actual production `fx.Decorate` calls | 2 |
| `fx.Populate` | 2 |

That filter includes a few nested test-helper packages. A stricter filter excluding every `tests` and `testing` directory found 37 `fx.Options` calls across 30 production files, 262 `fx.Provide` calls, and 41 `fx.Invoke` calls. The precise count is less important than the shape: the graph is large, but overwhelmingly static.

Temporal uses seven value-group names:

- `services`
- `deadlockDetectorRoots`
- `workerComponent`
- `perNamespaceWorkerComponent`
- `queueFactory`
- `additionalAllowedMethodsDuringHandover`
- `TaskHookFactories`

Production does not materially use several advanced Fx features: `fx.Private`, `fx.RecoverFromPanics`, `fx.ErrorHook`, `fx.Shutdowner`, `Done`, `Wait`, `Run`, `ValidateApp`, and `DotGraph`. There is no production `fx.Replace` call.

### Static composition versus dynamic options

The many `fx.Options(...)` calls are mostly fixed module declarations and are straightforward code-generation inputs. There are only four actual production signatures that accept or return `fx.Option`:

1. [`NewServerFx(topLevelModule fx.Option, ...)`](temporal/fx.go), although Temporal itself passes `TopLevelModule`.
2. [`ServiceProviderParamsCommon.GetCommonServiceOptions(...) fx.Option`](temporal/fx.go), which rebuilds a fixed common child graph.
3. [`static.MembershipModule(...) fx.Option`](common/membership/static/fx.go), which closes over static host data.
4. [`AnnotateWorkerComponentProvider(...) fx.Option`](service/worker/common/fx.go), an intentional worker-component extension point.

The more difficult dynamic surface is the one-box test harness. [`tests/testcore/onebox.go`](tests/testcore/onebox.go) stores arbitrary per-service `[]fx.Option` values and uses `Decorate` and `Populate`. Upstream PR [#10863](https://github.com/temporalio/temporal/pull/10863) removed test-only Fx population from DLQ tests, and PR [#10966](https://github.com/temporalio/temporal/pull/10966) removed per-service Fx test options. This checkout still contains the older fields.

`ServerOption` is not an Fx extension mechanism. Temporal already parses `opts ...ServerOption` into ordinary server configuration, so most of that API can remain unchanged.

## What Fx currently provides

The replacement has to preserve more than constructor ordering.

| Fx behavior | Static replacement |
| --- | --- |
| Reachable, once-per-app providers | Generate only the reachable graph and store each result in one local or field. |
| Constructor errors | Direct calls with immediate propagation and contextual wrapping. |
| `fx.In` and `fx.Out` | Typed input, parameter, and result structs. |
| Named values | Explicit fields or distinct wrapper types. |
| Interface projection | Compile-checked Go assignments or adapter providers. |
| Value groups and flattened groups | Generated typed slice assembly. |
| Optional inputs | Explicit nil, zero, or default selection at the root. |
| `Supply` | Root builder arguments. |
| `Populate` | Root builder results. |
| `Decorate` | Typed decorator functions at declared seams. |
| `Replace` | Typed override fields or alternate roots. |
| `Invoke` | Generated direct calls after dependencies are ready. |
| Modules | Generator-only composition and visibility boundaries. |
| Lifecycle | A small runtime registry. |

Generated group order would be deterministic. Fx group order is unspecified, so valid consumers must not depend on the current sequence; migration tests should verify membership and semantic behavior rather than assuming an undocumented order.

Behavior that should not be preserved as a general API:

- arbitrary runtime `fx.Option` injection;
- arbitrary `Decorate`, `Replace`, or `Populate` against internal graph nodes;
- runtime graph mutation;
- a generic service locator or `map[reflect.Type]any` escape hatch.

Runtime graph visualization and provider-event logging can be replaced with a generated graph manifest or offline DOT output if they are valuable. Temporal does not currently depend on Fx signal handling or programmatic shutdown features.

## Where the startup cost comes from

[`fx.New`](https://github.com/uber-go/fx/blob/v1.24.0/app.go#L425-L522) creates a Dig container, applies modules, registers providers and decorators, configures event logging, and invokes graph roots.

Dig then:

- inspects constructors with `reflect.TypeOf`;
- creates parameter and result graph metadata;
- allocates provider, decorator, result, and group maps;
- constructs `[]reflect.Value` arguments;
- recursively resolves providers and cached values;
- invokes constructors through reflection;
- builds `fx.In` values through `reflect.New` and field-by-field `Set`;
- stages results in temporary maps before committing them to the container.

Relevant implementation sources include Dig's [`constructor.go`](https://github.com/uber-go/dig/blob/v1.19.0/constructor.go), [`param.go`](https://github.com/uber-go/dig/blob/v1.19.0/param.go), and [`invoke.go`](https://github.com/uber-go/dig/blob/v1.19.0/invoke.go). `fx.Annotate` can additionally create function and struct types with `reflect.FuncOf`, `reflect.StructOf`, and `reflect.MakeFunc`.

This establishes that the allocation opportunity exists. It does not establish that DI dominates total Temporal startup time; many constructors and lifecycle hooks perform real persistence, network, configuration, and service initialization.

## Directional profile

The existing server test passed under a coarse CPU and memory profile:

```sh
go test -tags test_dep ./temporal -run '^TestNewServer$' -count=1 \
  -memprofile /tmp/temporal-fx-startup.mem \
  -cpuprofile /tmp/temporal-fx-startup.cpu
```

The test completed in 6.282 seconds. Coarse 10 ms CPU samples showed approximately 60 ms cumulative under `fx.New` and approximately 70 ms cumulative under Dig-plus-reflection frames. Those sets overlap and must not be added.

A second run with `-memprofilerate=1` completed graph construction but later failed because profiling overhead triggered timing-sensitive warning logs. Its profile attributed approximately 5.77 MB flat directly to Fx/Dig entries out of 52.65 MB total, plus approximately 0.64 MB to associated reflection, function construction, and Temporal's Fx event adapter.

These figures are directional evidence, not a clean benchmark. They mix provider work with container work, and the high-resolution memory run did not pass. Before deciding on a migration, add a focused benchmark that stops immediately after graph construction and separately measures:

- top-level graph construction;
- construction of each child service graph;
- lifecycle start;
- complete cold start including provider I/O.

Use `ReportAllocs`, repeated runs, and identical provider sets for Fx and generated paths.

## Proposed design

### Compile-time graph declarations

Each service would have a constrained codegen-only declaration. The exact syntax is a design choice, but it should contain references that Go tooling can rename and type-check:

```go
//go:build di_codegen

func buildMatchingService(in MatchingInputs) (*ServiceApp, error) {
	return di.Build(
		di.Provide(
			NewTaskQueueConfig,
			NewMatchingEngine,
			NewMatchingService,
		),
		di.Group("TaskHookFactories",
			NewMetricsTaskHook,
			NewTracingTaskHook,
		),
		di.Invoke(RegisterHealthChecks),
	)
}
```

These marker calls are parsed rather than executed. The generator resolves provider signatures with `go/types`, rejects missing or ambiguous providers and cycles, computes reachability, and produces a stable order.

This declaration language must remain deliberately smaller than Fx. It should support only the behavior Temporal has chosen to preserve.

### Generated code

The normal build sees direct Go:

```go
//go:build !di_codegen

func buildMatchingService(
	in MatchingInputs,
	overrides MatchingOverrides,
) (*ServiceApp, error) {
	lc := lifecycle.New()

	taskQueueConfig := NewTaskQueueConfig(in.DynamicConfig)
	store, err := NewMatchingStore(in.Persistence)
	if err != nil {
		return nil, fmt.Errorf("create matching store: %w", err)
	}

	taskHooks := []TaskHookFactory{
		NewMetricsTaskHook(in.Metrics),
		NewTracingTaskHook(in.Tracer),
	}
	taskHooks = append(taskHooks, overrides.AdditionalTaskHooks...)

	engine, err := NewMatchingEngine(MatchingEngineParams{
		Lifecycle:       lc,
		Config:          taskQueueConfig,
		Store:           store,
		TaskHookFactory: taskHooks,
	})
	if err != nil {
		return nil, fmt.Errorf("create matching engine: %w", err)
	}

	service := NewMatchingService(engine)
	RegisterHealthChecks(in.HealthServer, service)
	return NewServiceApp(service, lc), nil
}
```

There is no runtime dependency container. Locals provide singleton semantics, slices provide groups, direct assignments provide interface bindings, and normal Go calls propagate errors.

### Package boundaries

A single generated file in `temporal` cannot call unexported constructors in other packages. The generator should therefore emit package-local builders and compose them through small typed input/result structs.

For example:

```go
type MatchingInputs struct {
	Persistence   persistence.Client
	DynamicConfig dynamicconfig.Collection
	Metrics       metrics.Handler
	Lifecycle     lifecycle.Registry
}

type MatchingRuntime interface {
	Start(context.Context) error
	Stop(context.Context) error
}
```

Internal dependencies remain encapsulated inside their packages. Only actual cross-package capabilities become builder inputs or outputs. This creates deep, testable module boundaries instead of exporting implementation details solely for DI.

### Runtime service selection

Generate one builder per service rather than every combination of service names:

```go
common, err := buildCommon(inputs)
if err != nil {
	return nil, err
}

if serviceNames.Contains(primitives.MatchingService) {
	matching, err := buildMatchingService(common.MatchingInputs(), overrides.Matching)
	// ...
}
```

The top-level builder constructs common dependencies once, conditionally invokes requested service builders, and retains Temporal's existing service start and stop order.

Runtime choices such as static versus Ringpop membership become explicit typed branches or inputs rather than dynamically selected modules.

## Lifecycle design

Dependency resolution is static; lifecycle execution remains runtime behavior.

The lifecycle module should expose a small interface:

```go
type Lifecycle interface {
	Append(Hook)
}

type Hook struct {
	OnStart func(context.Context) error
	OnStop  func(context.Context) error
}
```

Required invariants:

- hook registration follows generated constructor/dependency order;
- `OnStart` hooks run serially in registration order;
- start stops on the first error;
- a failed start rolls back only hooks that were reached successfully;
- rollback and normal stop run in reverse order;
- stop continues after individual errors and aggregates them;
- cancellation and configured timeouts remain effective;
- start, stop, rollback, and error events remain observable through existing logging conventions;
- nested service apps retain the current explicit service-level order.

As a migration step, the implementation can satisfy `fx.Lifecycle` and accept `fx.Hook`. Existing constructor signatures and CHASM-generated clients then remain unchanged even though no `fx.App` or Dig container is used. Once all graph construction has migrated, Temporal can introduce its own `Hook` type and update CHASM generation.

Fx starts hooks in constructor registration order, rolls back successful starts after failure, and stops in reverse order. See the official [`fx.App.Start` and `Stop` implementation](https://github.com/uber-go/fx/blob/v1.24.0/app.go#L643-L731) and [internal lifecycle implementation](https://github.com/uber-go/fx/blob/v1.24.0/internal/lifecycle/lifecycle.go).

## Typed overrides instead of arbitrary options

Each service should expose only supported variation points:

```go
type MatchingOverrides struct {
	Store               MatchingStore
	DecorateLogger      func(log.Logger) log.Logger
	AdditionalTaskHooks []TaskHookFactory
}
```

Production typically passes the zero value. Tests can replace specific providers, append group members, or install declared decorators without knowing the entire internal graph.

This is intentionally less flexible than `[]fx.Option`. The benefit is that supported extension points are documented, statically typed, and resilient to unrelated internal refactors.

The worker-component helper deserves special treatment because its comment describes library users dynamically registering system workers. Its replacement may be an explicit `[]WorkerComponentFactory` root input rather than a closed override field.

## Preventing codegen lockout

The generator must be able to run when normal generated startup code is stale or does not compile.

### Complementary build tags

Use two mutually exclusive views of every generated entry point:

```go
// wiring_spec.go
//go:build di_codegen

func buildMatchingService(in MatchingInputs) (*ServiceApp, error) {
	return di.Build(/* current declarations */)
}
```

```go
// wiring_gen.go
//go:build !di_codegen

func buildMatchingService(in MatchingInputs) (*ServiceApp, error) {
	// generated implementation
}
```

Production code can refer to `buildMatchingService` in either view. Under `di_codegen`, the specification stub supplies the symbol and every stale generated implementation is excluded.

The generator command must be a standalone package that does not import generated startup packages. It uses `go/packages` or `go/types` to analyze explicitly requested roots with `di_codegen` enabled.

### Removed-module scenario

Suppose the old generated graph imports `SearchModule` and the developer removes it.

If the developer removes both the module implementation and its graph registration:

1. The normal build may temporarily fail because stale generated code still imports the removed package.
2. Codegen runs with `di_codegen`.
3. The stale generated file is excluded.
4. The current specification contains no `SearchModule` reference.
5. A complete new graph is generated and validated without the removed module.
6. The old output is replaced only after validation.

If the implementation is deleted but the specification still references it, codegen should fail with a precise error. That is a genuine inconsistent source graph, not a generated-code deadlock.

No generator can type-check arbitrary invalid Go. A syntax or type error in a reachable ordinary provider package can still block generation. Loading only the requested root and its current dependency closure reduces that failure surface.

### Generation contract

The in-repository generation driver should:

1. Treat current specifications as the only source of truth; never inspect old output to recover the desired graph.
2. Exclude every old generator-owned output during analysis.
3. Generate all requested output in memory or a staging directory.
4. Format it with `go/format`.
5. Type-check the normal build through an overlay in which old outputs are absent and new outputs are present.
6. Leave existing output untouched if graph solving, generation, formatting, or validation fails.
7. Commit generated files to Git.
8. Provide a `-check` mode that regenerates without writes and fails on any diff.
9. Record generator version and input fingerprint in generated headers for diagnostics.
10. Track generator-owned output paths so successful generation can remove obsolete files.

Filesystem rename is atomic per file, not across an arbitrary set of files. For strong crash consistency, keep each generation unit to one owned output file where possible, or generate into a wholly owned directory that can be swapped as one unit. Otherwise validate the complete set first, replace files only during a short commit phase, and rely on Git for recovery from a process or machine failure during that phase.

Ordinary `go build` and `go test` must never invoke generation automatically. The Go tool explicitly treats [`go generate`](https://go.dev/cmd/go/#hdr-Generate_Go_files_by_processing_source) as a separate operation.

## Alternatives surveyed

As of the research date, there is no mature direct replacement for Fx's complete Temporal subset.

| Option | Model | Assessment for Temporal |
| --- | --- | --- |
| [Google Wire](https://github.com/google/wire) | Direct generated Go | Archived in 2025. Useful reference, not a dependency. |
| [`goforj/wire`](https://github.com/goforj/wire) | Maintained Wire-compatible direct generation | Strongest external graph-compiler base; still lacks lifecycle, groups, optionals, decorators, and overrides. Writes outputs directly, so Temporal needs a safer driver or fork. |
| [`almondoo/wire`](https://github.com/almondoo/wire) | Conservative maintained Wire fork | Credible maintenance-oriented alternative, with the same semantic gaps as Wire. |
| [`libtnb/wire`](https://github.com/libtnb/wire) | Generic DSL with multibindings, modules, decorators, overrides, scopes, and cleanup | Closest generated design to Fx, but its first release was August 2026. Too new for foundational adoption; useful prior art. |
| [`gendi-org/gendi`](https://github.com/gendi-org/gendi) | YAML to a typed lazy container | Has collections, decorators, and generation-time overrides, but no Fx lifecycle or cleanup; retains a small runtime container and is young. |
| [`mazrean/kessoku`](https://github.com/mazrean/kessoku) | Direct generation with optional parallel providers | Lacks cleanup, groups, optionals, decorators, overrides, and lifecycle. Its README documents a chicken-and-egg generation problem. |
| [`shanjunmei/dig`](https://github.com/shanjunmei/dig) | Fx-shaped declarations to direct Go | Good build-tag isolation, but no cleanup, lifecycle, groups, optionals, or decorators; provider errors panic. Very new and unrelated to Uber Dig. |
| [`ramory-l/easydi`](https://github.com/ramory-l/easydi) | Annotated providers to direct Go with `Start`/`Close` | Lifecycle order, rollback, and reverse close are relevant prior art, but the project is extremely young and lacks Temporal's groups, optionals, and decorators. |
| [`soner3/flora`](https://github.com/soner3/flora) | Auto-discovery layered over archived Wire | Temporarily overwrites real output with stubs while generating; unsafe for the failure model under discussion. |
| [`samber/do/v2`](https://github.com/samber/do) | Generic runtime service container | Mature and lifecycle-rich, but uses runtime maps, reflection, and service-locator providers. It does not meet the static-wiring goal. |
| Hand-written roots | Plain Go | No generator or reflection; highest maintenance cost at Temporal's graph size. |

### Maintained Wire forks

`goforj/wire` is the most active Wire continuation surveyed. It retains Wire's complementary `wireinject` and `!wireinject` model, so stale generated output is normally excluded during regeneration. It supports providers returning both `error` and a cleanup function, and generated code unwinds already-created resources after a later constructor failure.

Wire provider sets are composition units, not Fx value groups. Wire has no application lifecycle, optional dependency primitive, decorator, or runtime replacement facility. Temporal would still need explicit slice providers, typed defaults, wrapper providers, alternate roots or override structs, and its own lifecycle.

Wire's own documentation warns that checked-in injectors can retain old provider-set behavior until regenerated. See [Wire best practices](https://github.com/google/wire/blob/main/docs/best-practices.md). That reinforces the need for a mandatory CI drift check even when generated code still compiles.

### Runtime generic containers

`samber/do/v2` is the strongest maintained runtime alternative found. It supports names, aliases, scopes, lazy/eager/transient services, overrides, health checks, lifecycle hooks, and dependency-aware parallel shutdown.

It does not eliminate runtime graph resolution. Providers accept an injector and explicitly resolve their dependencies, while the implementation uses maps and reflection for type discovery and assignability. It may avoid Dig's exact reflective constructor-call path, but adopting it would require wrapping ordinary Temporal constructors in service-locator functions and would retain much of the startup bookkeeping this work aims to remove.

### Hand-written and hybrid wiring

Hand-written service composition roots provide the strongest compiler guarantees and no generation failure mode. At roughly 262 production providers, one monolithic root would be costly to maintain and prone to merge conflicts.

A useful hybrid is:

- hand-write stable service-level builders and lifecycle boundaries;
- generate only repetitive package-local constructor plumbing;
- expose small typed inputs and results between packages.

This reduces the amount of generator logic while keeping generated details behind deep module interfaces.

## Migration plan

1. **Measure first.** Add isolated construction benchmarks for the top-level and each child graph. Establish an allocation and CPU target.
2. **Extract lifecycle.** Implement and parity-test an Fx-compatible lifecycle without changing dependency resolution.
3. **Define typed inputs and overrides.** Replace unsupported dynamic graph mutations at the selected pilot boundary.
4. **Pilot matching.** Matching is a smaller production root than history. Generate or hand-wire it behind the existing service `Start`/`Stop` interface.
5. **Run both paths.** Keep the Fx root behind an explicit `fxruntime` build tag or test-only adapter and compare behavior.
6. **Migrate remaining child graphs.** Frontend, worker, and then history. Treat groups and decorators explicitly.
7. **Migrate the top-level graph.** Replace top-level `Populate`, service grouping, and nested common-option propagation after child apps share the new boundary.
8. **Migrate tests.** Convert common one-box options to typed overrides and group additions. Keep test-only Fx only for unclassified rare cases.
9. **Update CHASM generation.** Replace generated `fx.Lifecycle` and `fx.StopHook` references and regenerate affected code.
10. **Remove Fx and Dig.** Do this only after production and test imports are gone and all parity and benchmark gates pass.

### Required parity tests

- every requested service combination;
- graph reachability and once-only construction;
- constructor errors at different depths;
- missing and present optional inputs;
- named dependency selection;
- group membership and flattened group behavior;
- declared decorators and overrides;
- invokes and populated roots;
- deterministic generation;
- lifecycle registration and start order;
- failed-start rollback;
- reverse stop and stop-error aggregation;
- cancellation and timeout behavior;
- nested service start/stop order;
- removed-module regeneration while normal generated output is stale.

## Trade-offs

### Performance

Direct calls remove container maps, graph traversal, reflective argument/result objects, and `reflect.Call`. They may also improve inlining and escape analysis. Provider work and lifecycle execution remain.

The likely benefit is several megabytes of startup allocations and tens of milliseconds of CPU for an all-service graph, but that remains a hypothesis until isolated benchmarking. The value is higher for tests, development loops, scale-to-zero, short-lived processes, and frequent restarts than for a server that starts once and runs for weeks.

### Build and repository cost

Generated roots increase source volume, compile work, and review diffs. Splitting by service and package reduces merge contention and makes generated output navigable. Removing Fx and Dig may offset some binary and dependency cost.

### Complexity and ownership

A generator becomes build infrastructure. A small intermediate representation, deterministic readable output, precise diagnostics, and a strict recovery contract are more valuable than reproducing every Fx feature.

Using a third-party graph solver reduces compiler implementation work but does not remove Temporal's need to own lifecycle semantics and the generation transaction.

### Dynamic extensibility

Runtime service selection remains easy. Arbitrary late-bound providers do not. New extension points must be intentionally represented as typed inputs, group contributions, or decorators.

This is a compatibility cost, but it also prevents tests and embedders from depending on arbitrary internal graph nodes.

### Scalability

At ten times as many providers, generated runtime construction remains a sequence of direct calls. Generation and type-check cost, output size, and build time grow with graph size. Per-service outputs and cached package analysis keep that manageable.

This work does not improve steady-state request throughput. It targets process construction cost.

### Failure and security

Generated constructor errors remain normal returned errors. Lifecycle start failures roll back, and stop remains best-effort with aggregation.

Keeping the generator in-tree and pinning its dependencies reduces reliance on a mutable globally installed binary. Generated output remains reviewable Go. The generator itself becomes part of the build supply chain and should receive normal code review, tests, and deterministic CI verification.

## Decision

Proceed only as a measured prototype, not a repository-wide rewrite.

The best prototype compares two implementations behind the same matching-service builder interface:

1. a maintained Wire fork plus a Temporal lifecycle and handwritten adapters for groups, optionals, decorators, and overrides;
2. a minimal Temporal-owned generator with the full removed-module recovery and output-validation contract.

The go/no-go gate is a matching-service result that:

- materially reduces graph-construction allocations or CPU;
- preserves constructor and lifecycle failure behavior;
- supports required groups, optionals, and overrides without a generic runtime container;
- remains regenerable when old generated code or a removed module breaks the normal build;
- produces readable, deterministic Go that maintainers are willing to review.

If neither prototype provides a meaningful measured benefit at acceptable maintenance cost, retain Fx and focus on reducing nested graphs, test-only graph coupling, and unnecessary startup instrumentation instead.

## Reproduction commands

Broad inventory:

```sh
rg -l 'go[.]uber[.]org/fx' --glob '*.go' --glob '!*_test.go' --glob '!tests/**' | wc -l
rg -o 'fx[.][A-Za-z0-9_]+' --glob '*.go' --glob '!*_test.go' --glob '!tests/**' \
  | sed 's/.*fx[.]//' | sort | uniq -c | sort -nr
```

Stricter production inventory:

```sh
rg -o --glob '*.go' --glob '!*_test.go' --glob '!**/tests/**' --glob '!**/testing/**' \
  'fx[.]Options\(' . | wc -l
```

Directional server profile:

```sh
go test -tags test_dep ./temporal -run '^TestNewServer$' -count=1 \
  -memprofile /tmp/temporal-fx-startup.mem \
  -cpuprofile /tmp/temporal-fx-startup.cpu
```

## Supporting research

- [Generated startup wiring for Temporal](.turbo/research/fx-generated-wiring.md)
- [Alternatives to Uber Fx for Temporal startup wiring](.turbo/research/fx-alternatives.md)
