# Constraint-based configuration: what it buys and what it costs

*A prototype exists and works. This is the case for and against adopting it. The engineering
detail is in [constraint-expression-dynamicconfig-prototype.md](./constraint-expression-dynamicconfig-prototype.md).*

---

## The problem in one paragraph

Temporal has roughly 677 runtime settings. An operator can change any of them without a
deploy — but only along dimensions the code was written to support. There are eight of them:
namespace, task queue, shard, and five more. Anything else — "only in this region", "only on
the canary ring", "only for workers on an old SDK" — is not a configuration change. It is a
code change, a code-generation step, a review, and a release.

## What that costs us today

**Rollouts are coarser than we want them to be.** A change is on for a namespace or off for
it. We cannot turn something on in one region first, or for one deployment ring, or hold it
back from the customers running a client version we know has a bug. So changes go out wider
than is comfortable, or they wait.

**Incidents take a deploy to contain.** When a setting needs to change for one zone or one
cohort of callers, the honest options today are to change it everywhere or to ship code. Both
are slower and riskier than the situation deserves.

**Every new dimension is a project.** Adding one means editing a code generator, regenerating
~2000 lines, and updating the config parser and its validation. The result is that nobody
adds one for a single rollout, so the rollout is shaped around the dimensions we already have
rather than the ones the situation calls for.

## What the prototype changes

Configuration becomes a default plus a list of conditions, evaluated at runtime against
whatever the server knows about itself and the request:

```yaml
matching.getTasksBatchSize:
  defaultValue: 1000
  overrides:
    - matchString: '"env" = "staging" and ("zone" = "us-west-1" or "zone" = "us-west-2")'
      matchResult: 250

system.enableEagerWorkflowStart:
  defaultValue: true
  overrides:
    # a known bug in one SDK range: turn the feature off for those workers only
    - matchString: '"sdkName" = "temporal-go" and "sdkMajor" = 1 and "sdkMinor" < 28'
      matchResult: false
```

Neither of these can be expressed today at any price. Both are now edits to a file.

Three things follow.

**New dimensions stop needing engineering.** Region, cluster, deploy ring, host, SDK version,
caller identity — and anything a team invents for its own component — become configuration.
The prototype demonstrates a dimension (`deployRing`) that exists nowhere in Temporal's code:
an operator named it in a file and used it.

**Conditions can express what operators actually mean.** `and`, `or`, comparisons and
grouping, instead of a flat list of exact matches resolved by a precedence order documented
only in the code generator. Which rule wins is visible in the file.

**Nothing changes until someone opts in.** A server with no expression file configured is
byte-for-byte the server we run today, and settings that use the file are enabled one at a
time.

## What it costs

**It is not free to finish.** The prototype is real, tested and measured, but it is a
prototype. Two capabilities are missing before it could replace anything, and one of them is
significant — see below.

**It is another place to look.** For a while there will be two configuration files with two
formats. A setting lives in one or the other, never both, which keeps the rules simple, but
"where is this configured" gains a second answer until a migration finishes.

**Mistakes are quiet.** The failure mode of a conditional system is a condition that never
matches: the value silently stays at its default. We have mitigated the common cases —
misspelled dimensions are now rejected when the file loads, and values are type-checked
against the setting they configure — but expressive configuration is inherently easier to get
subtly wrong than a flat list.

**Performance is a non-issue, and that was not a given.** Reads cost the same as today, or
less. Nothing allocates. This was the main technical risk going in and it did not
materialise.

## The honest blocker

Temporal already does two things this system cannot yet do:

- **Percentage rollouts** — "enable for 5% of task queues, by a stable hash, then dial up".
- **Time-phased ramps** — the same, spread over a window automatically.

Two shipping features rely on them. Until the new system can express them it is strictly a
complement, not a replacement. Adding them is understood and estimated (see the engineering
doc), but it is real work and it is a prerequisite for the larger prize.

## The larger prize

The eight fixed dimensions are not just a limitation, they are a tax. Because every setting
must be declared against one of them, the code generator produces ~2000 lines of variants and
every call site is shaped by the dimension it was assigned at birth. The prototype includes a
uniform way to read configuration that makes those variants unnecessary.

If we migrate call sites to it, we can delete the dimension machinery entirely: fewer
generated lines, one way to read a setting instead of fifty-six, and no future change ever
again needing a code-generation step to add a dimension.

That migration is mechanical but large — on the order of a thousand call sites — and it does
not need deciding now. It is where the compounding benefit is, and it is available only if we
go down this road.

## What we are asking for

A decision on direction, not a commitment to the whole programme:

| | Cost | Buys |
| --- | --- | --- |
| **Close the rollout gap** | ~1–2 weeks | Feature parity. The new system becomes strictly better than the old, and adoption can begin. |
| **Adopt incrementally** | Ongoing, per team | Region-, ring-, and SDK-scoped rollouts on the settings that need them. |
| **Retire the old dimension system** | Larger, later | Deletes the code generation, simplifies every call site. Only worth scheduling once the above have proven themselves. |

The first is the only one that needs a decision now. The prototype is on a branch, isolated,
and costs nothing while it sits there.

## Risk summary

| Risk | Assessment |
| --- | --- |
| Performance regression | **Resolved.** Measured; reads are at or below today's cost with zero allocation. |
| Destabilising existing config | **Low.** Off by default; unconfigured servers are unchanged; the two systems do not interact per setting. |
| Missing rollout primitives | **Open, and the gating item.** Estimated, understood, not yet built. |
| Silent misconfiguration | **Partly mitigated.** Unknown dimensions and wrong value types now fail at load. Logically-wrong-but-valid conditions still fail quietly, as they do in any rules system. |
| Dependency on outside code | **Resolved.** The expression library is a small, vendored copy we own outright and have already modified. |
