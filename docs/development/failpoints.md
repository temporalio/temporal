# Failpoints

Failpoints let tests and tools make server code fail, stall or return values from outside that
code. The server declares each in the package that executes it, except the keys of the gRPC, HTTP
and persistence boundaries, which `common/testing/serverfailpoints` declares, and executes them at
the points in its code that tests need to reach. Tests inject them with `testcore.InjectFailpoint`; see
the [testing guide](testing.md#failpoint-package).

## Enabling failpoints

Failpoints are compiled into every build, including release binaries; there is no separate test
image. A server process only installs them if the static server config (`config.Failpoints`) sets
`failpoints.enabled: true`, which it reads once at startup:

```yaml
failpoints:
  enabled: true
```

Neither environment variables nor dynamic config can enable failpoints. With the switch off, nothing
is installed: there is no failpoint registry, the gRPC fault interceptor, the HTTP fault wrapper and
the persistence failpoint injector are not installed, the rules (`testing.failpointRules`) do
nothing, BUGGIFY does nothing and buggifiable settings are not randomized, the `failpoints.buggify`
block is not read, the dynamic config file client ignores the rules setting (it doesn't convert,
validate or warn about its value), and call sites in server code cost a nil check.

When the switch is on, the server logs a warning at startup that failpoints are enabled and not
meant for production use. With the switch off, it logs nothing about failpoints.

Test clusters (`testcore`) run the normal server with failpoints enabled. They give each server a
registry with `temporal.WithFailpointSource` instead of the switch, so that each cluster of a
test process has its own. A cluster runs BUGGIFY only if its test asks for it; see
[BUGGIFY](#buggify).

The server does not serve a gRPC API to configure failpoints. Tests configure the failpoints of
their cluster in process, and a server or test cluster is configured from outside through the
`testing.failpointRules` dynamic config setting. The `go.temporal.io/testx/failpoint/failpointgrpc`
package can serve a registry over gRPC for a process that needs it, but nothing in the server
installs it.

## Failpoint rules

With failpoints enabled, a server process applies the failpoint rules of the `testing.failpointRules`
dynamic config setting to its failpoints. This configures failpoints without code, for example in a
development server's dynamic config file:

```yaml
testing.failpointRules:
  - value:
      - key: "grpc:/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution"
        scope: "<namespace ID>"   # Optional. Or namespace: "<namespace name>", not both.
        action: Return            # Return, Panic, Block, Pause or On, in any case.
        error: InvalidArgument    # A name from the failpoint error catalog, for Return and Panic.
        message: "injected"
        trigger: {probability: 0.5, skipFirst: 1, maxFires: 10}  # Optional; maxFires: 1 fires once.
      - key: "grpc:/temporal.server.api.matchingservice.v1.MatchingService/AddWorkflowTask"
        action: Pause
        delay: 2s
```

A rule applies to one namespace with `scope`, the namespace ID, or with `namespace`, its name, which
is easier to know on a development server. A rule has one of them, and `namespace` is only for the
gRPC and HTTP failpoints (the keys with the prefix `grpc:`, `grpc-response:`, `http:` and
`http-response:`, and the keys of any operation): the server looks the name up when a request
executes, so the rule applies to a namespace that is created, or recreated with another ID, after
the rule. It fires only for a namespace that exists, as the namespace cache of the service knows
it: a request for a name that no namespace has, such as the one that registers it, is not failed, so
a misspelled name never fires. An empty or null `namespace` is an error, so a rule for every
namespace omits the field. If a test registered a failpoint for the same key and the ID of the
namespace, that one fires instead (see the
[testing guide](testing.md#failpoint-package)).

The key is the name of a declared failpoint key, or a name with the prefix of the keys of a
transport: `grpc:`, `grpc-response:`, `http:`, `http-response:` or `persistence:`. A rule for any
other name is invalid, and the error lists the declared names. The method of a `grpc:` or
`grpc-response:` key must be the full name of a unary method of a gRPC service of the server, with
the leading slash (`/package.Service/Method`); streaming methods run no failpoints. `On` makes the
failpoint fire without a value, for call sites that call `failpoint.ExecuteSwitch`.

Changing the setting creates, reconfigures and removes the failpoints of the rules; a rule that
didn't change keeps its failpoint's state. The server logs each rule that a change applied and each
rule that it removed. A rule with a probability and no seed picks one; the log line of the rule has
it, so a run can be repeated with `seed` in the trigger.

Rules and other injections stay separate: if a rule names the key of a failpoint that a test
registered, the server skips that rule, applies the others, and logs a warning with its key. It
tries the skipped rule again with the next change of the setting. `testcore.InjectFailpoint` refuses
to change a failpoint of a rule.

A server doesn't start with an invalid `testing.failpointRules` value. If the value becomes invalid
while the server runs, the server logs an error and keeps the previous rules. A dynamic config file
with a non-string map key in a rule is rejected as a whole by the file loader, which keeps the
previous dynamic config and logs an error. `Setting.Validate` rejects an invalid value, so a test
that sets one with `testcore.WithDynamicConfig` fails when it starts.

## BUGGIFY

BUGGIFY runs a server "mostly normal, with a few weird things" by installing a seeded random
`failpoint.Policy` on the failpoint registry. It is meant for test clusters (a development server
or a test cell with failpoints enabled) and runs of external test harnesses, not for the functional
tests, which run without it, except `TestBuggify`. With a seed, each failpoint key name that has
nothing registered is activated for the whole run with some probability, and each execution of an
activated key fires with another probability. Both decisions are pure functions of the seed, the key
name and the number of the execution of the key in its namespace, so the same seed activates the
same keys in every namespace, and one namespace's traffic doesn't change which executions fire in
another. Which executions fire also depends on the order of executions of a key in a namespace, so
a seed alone reproduces the set of activated keys, not the whole run; a harness that schedules the
requests deterministically also reproduces which executions fire. The scope of an
execution is the name of its namespace, so a namespace that is created again with the same name and
another ID gets the same decisions. Replaying a run therefore needs the same seed, the same
`keys`, the same namespace names and the same order of requests. The scope is the name for the
gRPC and HTTP keys and `matching.skipSyncMatch`; the server looks up the name of a request that only
carries an ID in the in-memory state of the namespace registry, and uses the ID if it is not there
(a namespace that was created a moment ago). Failpoints that a test or a rule registers match the
namespace by its ID (a rule can name it instead, see [Failpoint rules](#failpoint-rules)). The persistence keys are not in the standard list, so BUGGIFY
doesn't decide for them, and a key you add for them is scoped by namespace ID. The
activated keys and the randomized settings don't depend on the scope.

BUGGIFY applies only to the keys that you list. The `failpoints.buggify.keys` allowlist of the static
config is the whole selection: its entries are `{match, activationProbability, fireProbability,
error}`, `match` is a key name in which `*` matches any text, anywhere in the name (there are no
regular expressions), and the first entry that matches the name of a key sets its probabilities and
error. A key that matches no entry is never buggified. There is no built-in list, no default
probability and no other way to include or exclude a key.

What an activated key does when it fires depends on its entry and its call site:

- With an `error`, the key fails with that registered error, such as `Unavailable`, wherever the
  code executes it with `failpoint.CheckError` or another `Execute` function that returns an error.
  The standard list does that for `grpc:` and `http:` keys: it fails the request before it is
  handled with `Unavailable`, which callers retry. Only calls between services are failed, the
  requests that history, matching, worker and the internal frontend receive, and outgoing HTTP
  requests. Requests from clients to the public frontend are not: an error there would test the
  client's retries, not the server (`service/frontend/fx.go` passes `explicitOnly` to
  `grpcfailpoints.NewGenerator` for it). Registered failpoints still apply to the frontend. BUGGIFY
  names an outgoing HTTP request by its method, such as `http:POST`, not by its path, so the keys
  stay few and which are activated doesn't depend on the paths.
- Without an `error`, the key fires at its `failpoint.ExecuteSwitch` call site, such as
  `matching.SkipSyncMatchKey`, which persists a task to the backlog instead of offering
  it to a waiting poller. A switch branch selected for BUGGIFY must only make legal but unlikely
  states more likely, never produce one that cannot happen in production.
- Dynamic config settings of `serverbuggify.BuggifiableSettings` are named `dynamicconfig:<key>`.
  If an entry matches the name, the setting is activated with the entry's `activationProbability`
  and gets one of its alternative values, such as a batch size of 1, for the whole run.

A failpoint registered for a key, by a test or a rule, takes precedence over BUGGIFY even while it
is disabled, and so does an explicitly set dynamic config value over a randomized one. For gRPC and
HTTP requests, a failpoint registered for any method or request also does: for example
`InjectRequestFault` keeps BUGGIFY away from the gRPC requests of the test's namespace. The
failpoints of any method or request (`grpc-any:request` and so on) are never buggified.

### Configure it

Enable BUGGIFY on a server with failpoints enabled with the `failpoints.buggify` block of the static
server config (`config.FailpointsBuggify`). The server reads no environment variable for BUGGIFY and
no dynamic config setting changes it: to change the keys or the seed, restart the server. The block
is read once at startup, after all config files are loaded, and only if `failpoints.enabled` is
true, so with the switch off a malformed block does not stop the server:

```yaml
failpoints:
  enabled: true
  buggify:
    seed: 42     # A positive number enables BUGGIFY and needs keys; 0 or absent leaves it off.
    trace: true  # Print one line per decision, see "BUGGIFY trace". Default false.
    keys:        # The allowlist, see below; the standard list is further down.
      - {match: "grpc:*", activationProbability: 0.1, fireProbability: 0.01, error: Unavailable}
```

A server doesn't start with a seed but no keys, an entry without `match`, two entries with the same
`match`, a probability that is missing or outside of [0, 1], an `error` that no one registered, a
field that the block doesn't have (such as the former top-level `activationProbability`, `include`
or `exclude`), a `keys` that is not a list (`keys:` without a value is null, which is not a list), or
a seed that is not an unsigned integer, such as `1.5`. A later config file that has `keys` replaces
the list; `keys: []` clears it, which then needs `seed: 0`.

The standard list is the one that test clusters of the repository use when they pass
`testcore.StandardBuggifyKeys()` to `testcore.WithBuggify`; `TestBuggify` supplies a short list of
its own. Copy the standard list into your server config:

<!-- standardBuggifyKeys: testcore.StandardBuggifyKeys has the same entries, a test compares them. -->
```yaml
failpoints:
  enabled: true
  buggify:
    seed: 42
    keys:
      # A narrow entry comes before the broad one that would match it: health checks are never failed.
      - {match: "grpc:/grpc.health.v1.Health/*", activationProbability: 0, fireProbability: 0}
      # Calls between services: 10% of the methods are activated, 1% of their executions fire.
      - {match: "grpc:*", activationProbability: 0.1, fireProbability: 0.01, error: Unavailable}
      - {match: "http:*", activationProbability: 0.1, fireProbability: 0.01, error: Unavailable}
      # Hand-placed keys, which take the branch of their failpoint.ExecuteSwitch call site.
      - {match: "matching.skipSyncMatch", activationProbability: 0.25, fireProbability: 0.25}
      # Dynamic config settings that BuggifiableSettings lists: 25% of them get another value.
      - {match: "dynamicconfig:*", activationProbability: 0.25, fireProbability: 0.25}
```

The list names no persistence, response or health key, so BUGGIFY never fails a persistence method
(`persistence:*`), a response (`grpc-response:*`, `http-response:*`) or a health check. To keep a key
out of a broad entry, put an entry for it with an `activationProbability` of 0 before the broad one,
as the first entry does for the health service. Add an entry to buggify a key that the list doesn't
cover, such as a hand-placed key of your own. To randomize no dynamic config setting, drop the last
entry.

The server logs `BUGGIFY enabled` with the seed in the `buggify-seed` tag, every key it activates,
and every randomized setting. Settings are randomized once, when the server starts. To reproduce a
run, start the server with the logged seed and the same `keys`.

Test clusters run BUGGIFY only if their test asks for it, with `testcore.WithBuggify`, which sets
the `failpoints.buggify` block of the cluster's servers (`Keys: testcore.StandardBuggifyKeys()` for
the standard list). It gives the test a dedicated cluster, since BUGGIFY applies to the whole
cluster (see `TestBuggify`, which passes its own keys). A suite that owns its clusters, such as an XDC suite, passes
`testcore.WithClusterBuggify` to its setup instead.

### BUGGIFY trace

With `failpoints.buggify.trace: true`, the server prints one line to its standard output per BUGGIFY
decision, so a harness can follow which keys BUGGIFY activated and which executions it failed. The
format is stable: the prefix `BUGGIFY `, then space-separated `key=value` fields in this order,
where string values are quoted like Go strings (`strconv.Quote`) and the others are bare. Fields
are only ever added at the end of a line.

```text
BUGGIFY event=activation key="<failpoint name>" active=<true|false>
BUGGIFY event=fire key="<failpoint name>" scope="<scope>" execution=<n> action=<action>
```

- An `activation` line is the decision whether a name is activated, printed once per name and
  BUGGIFY run when the name first executes.
- A `fire` line is an execution of an activated name that fires. `scope` is the scope of the
  execution (a namespace name, or an ID where the name is not known, or empty), `execution` is its number in that scope,
  starting at 1, and `action` is what it does, such as `Return`.

The `serverbuggify.TraceLine` function formats the lines. The server also logs the activated names
as `BUGGIFY activated failpoint` entries. The trace is written from the goroutine of any execution,
possibly after the execution returned. When the server stops, it writes the lines of the decisions
that BUGGIFY already made before it stops the trace, for as long as the shutdown allows
(`Buggify.StopObserving`); a trace writer that blocks past that loses the remaining lines.

A test can observe the decisions with `OnDecision` on the `*buggify.Buggify` policy installed on
`testcore.FailpointRegistry(cluster)`; that replaces the observer that logs them. BUGGIFY
delivers events from the goroutine of any execution and possibly after the execution returned, so
a test waits for an event instead of assuming it arrived. Unit tests that need exact behavior
register a failpoint for the key, which takes precedence even while it is disabled, or leave
BUGGIFY off.
