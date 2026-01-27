# Bug: Race Condition in Auto-Enable with Inline RPC

## Summary

`TestFairnessAutoEnableSuite` fails on the `fake-network` branch due to a race condition between auto-enable and task writing when using inline RPC.

## Symptoms

- Test times out waiting for `GetTaskQueueTasks(MinPass: 1)` to return tasks
- Error in logs: `"task queue shutting down"` during `AddWorkflowTask`
- Test passes on `main` (with real gRPC) but fails on `fake-network` (with inline RPC)

## Root Cause

The race condition occurs in `task_queue_partition_manager.go`:

1. `AddTask` is called for a workflow task with priority 3
2. `autoEnableIfNeeded` is called **within** `AddTask` (line 315)
3. `autoEnableIfNeeded` calls `UpdateFairnessState` via the matching client
4. With **inline RPC**, this call is synchronous and returns immediately
5. `UpdateFairnessState` updates user data, which triggers a change notification
6. The notification handler detects the fairness state change (lines 1898-1900):
   ```go
   if to.GetData().GetPerType()[taskType].GetFairnessState() != pm.fairnessState {
       pm.logger.Debug("unloading partitionManager due to change in FairnessState")
       pm.unloadFromEngine(unloadCauseConfigChange)
       return
   }
   ```
7. `unloadFromEngine` cancels `tqCtx`, causing the partition manager to shut down
8. Back in `AddTask`, the task write fails because `tqCtx.Done()` is now set
9. Returns `errShutdown` ("task queue shutting down")

## Why It Works on Main

With real gRPC, the `UpdateFairnessState` RPC has network latency. The sequence becomes:

1. `AddTask` calls `autoEnableIfNeeded`
2. `UpdateFairnessState` RPC is sent over the network (non-blocking from task write perspective)
3. Task write completes successfully **before** the user data change notification arrives
4. Later, the notification triggers unload, but the task is already persisted

The network latency creates a natural delay that prevents the race.

## Affected Code

- `service/matching/task_queue_partition_manager.go:315` - `autoEnableIfNeeded` called during `AddTask`
- `service/matching/task_queue_partition_manager.go:277-306` - `autoEnableIfNeeded` implementation
- `service/matching/task_queue_partition_manager.go:1898-1900` - unload on fairness state change

## Possible Fixes

1. **Make auto-enable asynchronous**: Don't block on `UpdateFairnessState` - fire and forget or use a goroutine
2. **Reorder operations**: Complete the task write before triggering auto-enable
3. **Defer unload**: Don't unload immediately on fairness state change - let the current operation complete first (e.g., use a short delay or wait for in-flight operations)
4. **Separate the concerns**: Move auto-enable detection out of the `AddTask` path entirely

## Reproduction

```bash
# On fake-network branch
go test -v -race -run "TestFairnessAutoEnableSuite/Test_Activity_Basic" ./tests/... -timeout 2m
```

The test will timeout with repeated `GetTaskQueueTasks` calls returning no tasks.
