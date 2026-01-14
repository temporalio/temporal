# Race Condition Testing Guide

This document explains how to test for race conditions in the Temporal codebase.

## Quick Start

```bash
# Run specific race tests
go test -race ./tests -run TestAdminRebuildRaceTestSuite -timeout 5m

# Run all tests with race detection (WARNING: Very slow!)
go test -race ./... -timeout 30m

# Run race tests for a specific package
go test -race ./service/history/... -timeout 10m
```

## What is the Race Detector?

Go's race detector instruments all memory accesses and detects when:
- Two or more goroutines access the same memory location
- At least one access is a write
- The accesses are not synchronized (no proper locking)

## Race Tests in This Codebase

### 1. **Workflow Rebuilder Race Tests** (`admin_rebuild_race_test.go`)

These tests target the race conditions identified in `workflow_rebuilder.go`:

#### **TOCTOU Race (Time-of-Check-Time-of-Use)**
```go
// workflow_rebuilder.go lines 71-75
rebuildSpec, err := r.getRebuildSpecFromMutableState(ctx, &workflowKey) // Read DB (no lock)
wfContext, releaseFn, err := wfCache.GetOrCreateWorkflowExecution(...)  // Acquire lock

// Problem: State can change between these two operations!
```

**Test**: `TestConcurrentRebuildOperations`
- Launches 10 concurrent rebuilds on the same workflow
- Detects if stale data is used or if state corruption occurs

**Test**: `TestRebuildWhileWorkflowProgressing`
- Rebuilds while the workflow is actively progressing
- Workflow tasks are completing while rebuilds are reading state

#### **Clear-After-Unlock Race**
```go
// workflow_rebuilder.go lines 88-91 (BEFORE FIX)
defer func() {
    releaseFn(retError)  // Unlocks
    wfContext.Clear()    // Clears AFTER unlock - RACE!
}()
```

**Test**: `TestConcurrentCacheAccessDuringRebuild`
- Mixes rebuild, describe, and query operations
- Tests if cache operations conflict during release

## How Race Tests Work

### 1. **Concurrent Stress Testing**
```go
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        // Perform operation that might race
    }()
}
wg.Wait()
```

### 2. **Interleaving Operations**
```go
// Mix different types of operations
switch index % 3 {
case 0: Rebuild()
case 1: Describe()
case 2: Query()
}
```

### 3. **Timing Variations**
```go
// Stagger operations to increase chances of hitting race window
time.Sleep(50 * time.Millisecond)
```

## Expected Results

### ✅ **With Race Detector Enabled**
If a race exists, you'll see output like:
```
==================
WARNING: DATA RACE
Write at 0x00c0001a8000 by goroutine 47:
  go.temporal.io/server/service/history.(*workflowRebuilderImpl).rebuild()
      /path/to/workflow_rebuilder.go:89 +0x123

Previous read at 0x00c0001a8000 by goroutine 45:
  go.temporal.io/server/service/history/workflow.(*ContextImpl).Clear()
      /path/to/context.go:105 +0x456

Goroutine 47 (running) created at:
  ...
==================
```

### ✅ **Without Races**
All tests pass with no warnings:
```
PASS
ok      go.temporal.io/server/tests    15.234s
```

## Common Race Patterns

### 1. **TOCTOU (Time-of-Check-Time-of-Use)**
```go
// ❌ BAD: Check before lock
if someCondition() {  // Check
    lock.Lock()
    doSomething()     // Use (state may have changed!)
    lock.Unlock()
}

// ✅ GOOD: Check after lock
lock.Lock()
if someCondition() {  // Check
    doSomething()     // Use
}
lock.Unlock()
```

### 2. **Deferred Cleanup Order**
```go
// ❌ BAD: Clear after unlock
defer func() {
    unlock()
    clear()  // Other goroutines can access now!
}()

// ✅ GOOD: Clear before unlock
defer func() {
    clear()
    unlock()
}()
```

### 3. **Cache Races**
```go
// ❌ BAD: Cache operations without synchronization
value := cache.Get(key)
// ... value might be invalidated here ...
use(value)

// ✅ GOOD: Hold lock/pin while using cached value
lock.Lock()
value := cache.Get(key)
use(value)
lock.Unlock()
```

## Performance Considerations

Race detector adds overhead:
- **Memory**: 5-10x increase
- **CPU**: 2-20x slower
- **Time**: Tests take much longer

**Recommendations**:
- Run race tests separately from normal test suite
- Use in CI for specific packages or changed code
- Run full race suite nightly, not on every commit
- Focus race tests on concurrent/shared-state code

## CI Integration

```yaml
# Example GitHub Actions workflow
- name: Run Race Tests
  run: |
    go test -race ./service/history/... -timeout 15m
    go test -race ./tests -run Race -timeout 10m
```

## Debugging Race Conditions

### 1. **Reproduce Locally**
```bash
# Run test multiple times to trigger race
for i in {1..20}; do
    echo "Run $i"
    go test -race ./tests -run TestConcurrentRebuildOperations || break
done
```

### 2. **Increase Concurrency**
```go
// Make race more likely by increasing concurrent operations
const numConcurrent = 100  // Instead of 10
```

### 3. **Add Strategic Sleep**
```go
// Force specific interleaving
time.Sleep(1 * time.Millisecond)
```

### 4. **Use Go's Happens-Before Analysis**
Study the race detector output to understand:
- Which goroutines are involved
- What memory location is accessed
- The stack traces of conflicting accesses

## Additional Tools

### 1. **go-deadlock** (Deadlock Detection)
```bash
go get github.com/sasha-s/go-deadlock
```

### 2. **go-fuzz** (Fuzzing for Concurrency)
```bash
go get github.com/dvyukov/go-fuzz
```

### 3. **Delve** (Debugger with Goroutine Support)
```bash
dlv test ./tests -- -test.run TestConcurrentRebuildOperations
```

## References

- [Go Race Detector Documentation](https://go.dev/doc/articles/race_detector)
- [Effective Go - Concurrency](https://go.dev/doc/effective_go#concurrency)
- [The Go Memory Model](https://go.dev/ref/mem)
- [Data Race Patterns in Go](https://www.uber.com/blog/data-race-patterns-in-go/)
