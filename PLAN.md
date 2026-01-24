# Test Suite Conversion Plan

## Objective
Convert remaining test files in `tests/` from testify suite pattern to `testcore.NewEnv` pattern.

## Conversion Order (by line count, shortest first)

| # | File | Lines | Status |
|---|------|-------|--------|
| 1 | `admin_test.go` | 155 | Completed |
| 2 | `workflow_visibility_test.go` | 177 | Pending |
| 3 | `links_test.go` | 196 | Pending |
| 4 | `purge_dlq_tasks_api_test.go` | 214 | Pending |
| 5 | `workflow_alias_search_attribute_test.go` | 228 | Pending |
| 6 | `activity_api_batch_unpause_test.go` | 230 | Pending |
| 7 | `workflow_memo_test.go` | 233 | Pending |
| 8 | `add_tasks_test.go` | 237 | Pending |
| 9 | `max_buffered_event_test.go` | 237 | Pending |
| 10 | `activity_api_batch_update_options_test.go` | 242 | Pending |
| 11 | `acquire_shard_test.go` | 252 | Pending |
| 12 | `client_data_converter_test.go` | 257 | Pending |
| 13 | `admin_batch_refresh_workflow_tasks_test.go` | 261 | Pending |
| 14 | `workflow_timer_test.go` | 272 | Pending |
| 15 | `eager_workflow_start_test.go` | 274 | Pending |
| 16 | `workflow_task_reported_problems_test.go` | 285 | Pending |
| 17 | `describe_test.go` | 308 | Pending |
| 18 | `worker_registry_test.go` | 311 | Pending |
| 19 | `workflow_failures_test.go` | 382 | Pending |
| 20 | `update_workflow_sdk_test.go` | 401 | Pending |
| 21 | `stickytq_test.go` | 411 | Pending |
| 22 | `transient_task_test.go` | 414 | Pending |
| 23 | `activity_api_update_test.go` | 427 | Pending |
| 24 | `archival_test.go` | 433 | Pending |
| 25 | `poller_scaling_test.go` | 435 | Pending |
| 26 | `workflow_delete_execution_test.go` | 435 | Pending |
| 27 | `namespace_delete_test.go` | 451 | Pending |
| 28 | `http_api_test.go` | 452 | Pending |
| 29 | `query_workflow_test.go` | 457 | Pending |

## Skipped Files

| File | Reason |
|------|--------|
| `tls_test.go` | Needs MTLS support in NewEnv |
| `tests/xdc/*` | XDC tests have special requirements |
| `tests/ndc/*` | NDC tests have special requirements |

## Conversion Process

For each file:
1. Read the current test file
2. Count tests before migration
3. Convert suite struct to `testcore.NewEnv` function pattern
4. Replace hardcoded test variables with `s.Tv()` helpers
5. Move dynamic config to `testcore.WithDynamicConfig()` options
6. Run tests to verify functionality
7. Update status in this plan
