# Test Suite Conversion Plan

## Objective
Convert remaining test files in `tests/` from testify suite pattern to `testcore.NewEnv` pattern.

## Conversion Order (by line count, shortest first)

| # | File | Lines | Status |
|---|------|-------|--------|
| 1 | `admin_test.go` | 155 | Completed |
| 2 | `workflow_visibility_test.go` | 177 | Completed |
| 3 | `links_test.go` | 196 | Completed |
| 4 | `purge_dlq_tasks_api_test.go` | 214 | Completed |
| 5 | `workflow_alias_search_attribute_test.go` | 228 | Skipped (complex versioning setup) |
| 6 | `activity_api_batch_unpause_test.go` | 230 | Completed |
| 7 | `workflow_memo_test.go` | 233 | Completed |
| 8 | `add_tasks_test.go` | 237 | Completed |
| 9 | `max_buffered_event_test.go` | 237 | Completed |
| 10 | `activity_api_batch_update_options_test.go` | 242 | Completed |
| 11 | `acquire_shard_test.go` | 252 | Skipped (requires custom logger before cluster startup) |
| 12 | `client_data_converter_test.go` | 257 | Completed |
| 13 | `admin_batch_refresh_workflow_tasks_test.go` | 261 | Completed |
| 14 | `workflow_timer_test.go` | 272 | Completed |
| 15 | `eager_workflow_start_test.go` | 274 | Completed |
| 16 | `workflow_task_reported_problems_test.go` | 285 | Completed |
| 17 | `describe_test.go` | 308 | Completed |
| 18 | `worker_registry_test.go` | 311 | Completed |
| 19 | `workflow_failures_test.go` | 382 | Completed |
| 20 | `update_workflow_sdk_test.go` | 401 | Completed |
| 21 | `stickytq_test.go` | 411 | Completed |
| 22 | `transient_task_test.go` | 414 | Completed |
| 23 | `activity_api_update_test.go` | 427 | Completed |
| 24 | `archival_test.go` | 433 | Completed |
| 25 | `poller_scaling_test.go` | 435 | Completed |
| 26 | `workflow_delete_execution_test.go` | 435 | Completed |
| 27 | `namespace_delete_test.go` | 451 | Completed |
| 28 | `http_api_test.go` | 452 | Pending |
| 29 | `query_workflow_test.go` | 457 | Pending |

## Skipped Files

| File | Reason |
|------|--------|
| `tls_test.go` | Needs MTLS support in NewEnv |
| `tests/xdc/*` | XDC tests have special requirements |
| `tests/ndc/*` | NDC tests have special requirements |

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

### Step 5: Set Up SDK Client/Worker (if needed)

Tests using `s.Worker()` or `s.SdkClient()` need manual SDK setup:

```go
func TestSomething(t *testing.T) {
    s := testcore.NewEnv(t)

    // Set up SDK client and worker for the test-specific namespace
    sdkClient, err := sdkclient.Dial(sdkclient.Options{
        HostPort:  s.FrontendGRPCAddress(),
        Namespace: s.Namespace().String(),
        Logger:    log.NewSdkLogger(s.Logger),
    })
    s.NoError(err)
    defer sdkClient.Close()

    taskQueue := s.Tv().TaskQueue().Name
    worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
    worker.RegisterWorkflow(workflowFn)
    s.NoError(worker.Start())
    defer worker.Stop()

    // Now use sdkClient and worker...
}
```

---

## Checklist for Each Conversion

- [ ] read tests/testcore/test_env.go
- [ ] **Count tests before migration**: Run `go test -v ./tests/... -run "^TestXxxSuite$" -list ".*"` and record the number of tests
- [ ] Remove suite struct definition
- [ ] Remove `TestXxxSuite` function with `suite.Run`
- [ ] Convert each `func (s *Suite) TestXxx()` to `t.Run("Xxx", func(t *testing.T) { s := testcore.NewEnv(t); ... })`
- [ ] Replace hardcoded test variables with `s.Tv().WorkflowID()`, `s.Tv().TaskQueue()`, etc.
- [ ] Move dynamic config from `SetupSuite` to `testcore.WithDynamicConfig()` option
- [ ] Set up SDK client/worker manually if the test uses `s.Worker()` or `s.SdkClient()`
- [ ] Remove `"github.com/stretchr/testify/suite"` import
- [ ] **Count tests after migration**: Run `go test -v ./tests/... -run "^TestXxx$" -list ".*"` and verify the count matches
- [ ] Run tests to verify functionality is preserved
- [ ] run `make fmt-imports`

---

## Benefits of NewEnv

1. **Simpler structure**: No suite boilerplate, just functions
2. **Better isolation**: Each test gets its own namespace automatically
3. **Built-in testvars**: Access via `s.Tv()` without manual creation
4. **Automatic parallelism**: `NewEnv` calls `t.Parallel()` automatically
5. **Flexible configuration**: Dynamic config and dedicated clusters via options
