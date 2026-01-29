# Test Suite Conversion Plan: Migrating to testcore.NewEnv

## Overview

This document describes how to convert test suites from the suite-based `FunctionalTestBase` pattern to the function-based `testcore.NewEnv` pattern, following the example of `tests/update_workflow_test.go`.

## Top 5 Shortest Tests in `tests/`

| File | Lines | Status |
|------|-------|--------|
| `user_timers_test.go` | 102 | Converted |
| `tls_test.go` | 103 | Skipped (needs MTLS support in NewEnv) |
| `relay_task_test.go` | 124 | Converted |
| `user_metadata_test.go` | 135 | Converted |
| `namespace_interceptor_test.go` | 136 | Converted |

---

## Key Differences

| Aspect | Old (Suite-based) | New (NewEnv-based) |
|--------|-------------------|---------------------|
| Test structure | `type MySuite struct { testcore.FunctionalTestBase }` | `func TestXxx(t *testing.T)` |
| Environment | Suite setup/teardown | `s := testcore.NewEnv(t)` |
| Parallelism | Manual `t.Parallel()` in `TestXxxSuite` | Automatic (NewEnv calls it) |
| Test vars | `testvars.New(s.T())` | `s.Tv()` (built-in) |
| Namespace | Shared across suite | Dedicated per test |

---

## Conversion Steps

### Step 1: Convert Suite to Function

**Old pattern:**
```go
type UserTimersTestSuite struct {
    testcore.FunctionalTestBase
}

func TestUserTimersTestSuite(t *testing.T) {
    t.Parallel()
    suite.Run(t, new(UserTimersTestSuite))
}

func (s *UserTimersTestSuite) TestUserTimers_Sequential() {
    // test code using s.FrontendClient(), s.Namespace(), etc.
}
```

**New pattern:**
```go
func TestUserTimers(t *testing.T) {
    t.Run("Sequential", func(t *testing.T) {
        s := testcore.NewEnv(t)
        // test code using s.FrontendClient(), s.Namespace(), etc.
    })
}
```

---

### Step 2: Use Built-in Test Variables

**Old pattern:**
```go
func (s *MySuite) TestSomething() {
    id := "functional-my-test"
    wt := "functional-my-test-type"
    tl := "functional-my-test-taskqueue"

    request := &workflowservice.StartWorkflowExecutionRequest{
        WorkflowId:   id,
        WorkflowType: &commonpb.WorkflowType{Name: wt},
        TaskQueue:    &taskqueuepb.TaskQueue{Name: tl},
    }
}
```

**New pattern:**
```go
func TestSomething(t *testing.T) {
    s := testcore.NewEnv(t)

    request := &workflowservice.StartWorkflowExecutionRequest{
        WorkflowId:   s.Tv().WorkflowID(),
        WorkflowType: s.Tv().WorkflowType(),
        TaskQueue:    s.Tv().TaskQueue(),
    }
}
```

---

### Step 3: Handle Dynamic Config

**Old pattern (suite SetupSuite):**
```go
func (s *MySuite) SetupSuite() {
    s.FunctionalTestBase.SetupSuiteWithCluster(
        testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
            dynamicconfig.SomeKey.Key(): true,
        }),
    )
}
```

**New pattern (NewEnv options):**
```go
func TestSomething(t *testing.T) {
    s := testcore.NewEnv(t,
        testcore.WithDynamicConfig(dynamicconfig.SomeKey, true),
    )
}
```

---

### Step 4: Handle Cluster-Global Tests

For tests that need cluster-global isolation (e.g., metrics capture):

```go
func TestSomethingWithMetrics(t *testing.T) {
    s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
    // This test gets its own cluster instance
}
```

---

## Complete Example: Converting user_timers_test.go

### Before:
```go
type UserTimersTestSuite struct {
    testcore.FunctionalTestBase
}

func TestUserTimersTestSuite(t *testing.T) {
    t.Parallel()
    suite.Run(t, new(UserTimersTestSuite))
}

func (s *UserTimersTestSuite) TestUserTimers_Sequential() {
    id := "functional-user-timers-sequential-test"
    wt := "functional-user-timers-sequential-test-type"
    tl := "functional-user-timers-sequential-test-taskqueue"
    identity := "worker1"

    request := &workflowservice.StartWorkflowExecutionRequest{
        Namespace:    s.Namespace().String(),
        WorkflowId:   id,
        WorkflowType: &commonpb.WorkflowType{Name: wt},
        TaskQueue:    &taskqueuepb.TaskQueue{Name: tl},
        Identity:     identity,
    }

    we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
    s.NoError(err)
    // ...
}
```

### After:
```go
func TestUserTimers(t *testing.T) {
    t.Run("Sequential", func(t *testing.T) {
        s := testcore.NewEnv(t)

        request := &workflowservice.StartWorkflowExecutionRequest{
            Namespace:    s.Namespace().String(),
            WorkflowId:   s.Tv().WorkflowID(),
            WorkflowType: s.Tv().WorkflowType(),
            TaskQueue:    s.Tv().TaskQueue(),
            Identity:     s.Tv().WorkerIdentity(),
        }

        we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
        s.NoError(err)
        // ...
    })
}
```

---

## Checklist for Each Conversion

- [ ] **Count tests before migration**: Run `go test -v ./tests/... -run "^TestXxxSuite$" -list ".*"` and record the number of tests
- [ ] Remove suite struct definition
- [ ] Remove `TestXxxSuite` function with `suite.Run`
- [ ] Convert each `func (s *Suite) TestXxx()` to `t.Run("Xxx", func(t *testing.T) { s := testcore.NewEnv(t); ... })`
- [ ] Replace hardcoded test variables with `s.Tv().WorkflowID()`, `s.Tv().TaskQueue()`, etc.
- [ ] Move dynamic config from `SetupSuite` to `testcore.WithDynamicConfig()` option
- [ ] Remove `"github.com/stretchr/testify/suite"` import
- [ ] **Count tests after migration**: Run `go test -v ./tests/... -run "^TestXxx$" -list ".*"` and verify the count matches
- [ ] Run tests to verify functionality is preserved

---

## Benefits of NewEnv

1. **Simpler structure**: No suite boilerplate, just functions
2. **Better isolation**: Each test gets its own namespace automatically
3. **Built-in testvars**: Access via `s.Tv()` without manual creation
4. **Automatic parallelism**: `NewEnv` calls `t.Parallel()` automatically
5. **Flexible configuration**: Dynamic config and dedicated clusters via options
