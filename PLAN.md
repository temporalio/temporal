# Plan: Dirty-Tracking for Entity Queries

## Problem

Every rule currently scans all entities of a given type on every `Check` cycle, even
if nothing changed. For safety rules that run on every gRPC call, this means re-evaluating
hundreds of unchanged entities. Rules shouldn't need to know about change tracking.

## Goal

Both `QuerySafetyEntities` and `QueryLivenessEntities` automatically return only
entities that have received a fact since the rule's last check. Rules see fewer
entities, do less work, and need no code changes beyond using `Resolve` in liveness rules.

## Design

### Registry: generation counter per entity

```go
type entityRecord struct {
    entity     Entity
    generation uint64  // bumped on each RouteFacts call
}
```

`Registry.RouteFacts` increments `generation` for every entity it delivers facts to.
A global counter ensures generations are monotonically increasing across all entities.

### Rulebook: per-rule generation watermark

```go
type ruleState struct {
    // ... existing fields ...
    lastGeneration uint64                // highest generation seen
    pending        map[string]Violation  // unresolved liveness conditions
}
```

After a rule runs, `lastGeneration` is set to the registry's current max generation.

### Query functions: both filter by generation

```go
func QuerySafetyEntities[T any](c *SafetyContext) []*T {
    return queryEntities[T](c.Registry, c.sinceGeneration)
}

func QueryLivenessEntities[T any](c *LivenessContext) []*T {
    return queryEntities[T](c.Registry, c.sinceGeneration)
}
```

Both only return entities where `record.generation > sinceGeneration`.

### Liveness: Pending/Resolve instead of full scan

Liveness rules no longer need to scan all entities at teardown. Instead:

```go
// LivenessContext methods:
func (c *LivenessContext) Pending(key string, v Violation)  // adds to pending set
func (c *LivenessContext) Resolve(key string)               // removes from pending set
```

Flow:
1. Rule runs on dirty entities only
2. For a stuck entity → `Pending(key, v)` adds it to the pending set
3. Entity later receives a fact → entity becomes dirty → rule re-runs on it
4. If resolved → `Resolve(key)` removes it from pending
5. If still stuck → `Pending(key, v)` again (no-op, already tracked)
6. At teardown → `Rulebook.Check` returns all remaining `pending` items as violations

An entity that never receives another fact stays pending forever. This is correct:
it's stuck, which is exactly what liveness rules detect.

### Safety rules: unchanged

Safety rules already use `Eval`/`Pass` which are immediate. They just see fewer
entities per check cycle.

### Rule changes needed

Liveness rules need a small update: add `c.Resolve(key)` in the "condition met" branch.
Currently most liveness rules simply skip entities where the condition is met (early
`continue`). They'd add an explicit `Resolve` call before continuing:

```go
// Before (current):
if wu.FSM.Current() == "completed" {
    continue  // not stuck
}
c.Pending(key, violation)

// After:
if wu.FSM.Current() == "completed" {
    c.Resolve(key)
    continue
}
c.Pending(key, violation)
```

## Steps

### 1. Add generation tracking to Registry

- Add `globalGeneration uint64` to `Registry`
- Wrap entity storage: `entities map[string]*entityRecord`
- In `RouteFacts`, after `OnFact`, bump: `record.generation = atomic.AddUint64(&r.globalGeneration, 1)`
- Add `CurrentGeneration() uint64` method
- `QueryEntities` gains `sinceGeneration uint64` param (0 = return all)

### 2. Add Resolve + pending tracking to LivenessContext

- Add `pending map[string]Violation` to `ruleState`
- `Pending(key, v)` → `c.state.pending[key] = v`
- `Resolve(key)` → `delete(c.state.pending, key)`
- `Rulebook.Check` at teardown: collect all `state.pending` values as violations

### 3. Thread generation through rule contexts

- Add `sinceGeneration uint64` to `ruleContext`
- `Rulebook.Check` sets it from `ruleState.lastGeneration`
- After rule runs, update `ruleState.lastGeneration = registry.CurrentGeneration()`

### 4. Update query functions

- Both `QuerySafetyEntities` and `QueryLivenessEntities` pass `c.sinceGeneration`
- `queryEntities` filters by `record.generation > sinceGeneration` when non-zero

### 5. Update liveness rules

- Add `c.Resolve(key)` calls where conditions are met (10 rules, small diff each)

### 6. Update test helpers

- `CheckSafetyRule`/`CheckLivenessRule` set `sinceGeneration = 0` (scan all in tests)
- No unit test changes needed

## Trade-offs

- **Slightly more memory**: one `uint64` per entity + one per rule + pending map
- **Monotonic only**: generations never reset — fine for test scope
- **Both rule types benefit**: no full scans anywhere
- **Small rule changes**: liveness rules need explicit `Resolve` calls (~1 line each)
- **No entity removal**: entities are never removed, so generation tracking is simple

## Verification

- Existing tests pass unchanged (test helpers use sinceGeneration=0)
- Add unit test: route facts to entity A, check rule (sees A), route facts to B only,
  check rule again (sees only B, not A)
- Add unit test: Pending then Resolve removes from pending set
- Add unit test: at teardown, unresolved pending items become violations
