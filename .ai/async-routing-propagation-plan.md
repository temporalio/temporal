# Async Routing Config Propagation Plan

## Overview
This plan outlines the implementation of async routing config propagation from the deployment workflow to version workflows and task queues. The key distinction is between sync and async propagation modes based on version compatibility.

---

## 1. Modify `SyncWorkerDeploymentVersion` Activity (workflow.go)

### Location
`service/worker/workerdeployment/workflow.go:1007-1044` (syncVersion function)

### Changes Required

**Current Behavior:**
- Activity only sends version state update args to version workflow
- No routing config is included

**New Behavior:**
- Check if deployment workflow `hasMinVersion(AsyncSetCurrentAndRamping)` (line 618/912)
- If true, include full routing config in the sync call
- Pass routing config as part of `SyncVersionStateUpdateArgs` (revision_number is already inside routing_config)

### Implementation Steps

1. **Add routing config field to `SyncVersionStateUpdateArgs` proto:**
   ```protobuf
   // In api/deployment/v1/message.proto
   message SyncVersionStateUpdateArgs {
     // ... existing fields ...
     temporal.api.deployment.v1.RoutingConfig routing_config = X;
     // Note: revision_number is already part of routing_config, no separate field needed
   }
   ```

   **Activity Implementation Note:**
   The `SyncDeploymentVersionUserData` activity should:
   - When routing config doesn't change for a task queue, set `max_version = -1` in the response
   - This signals to the workflow that propagation check can be skipped for that task queue

2. **Modify syncVersion function (line 1007):**
   ```go
   func (d *WorkflowRunner) syncVersion(ctx workflow.Context, targetVersion string, versionUpdateArgs *deploymentspb.SyncVersionStateUpdateArgs, activated bool) (*deploymentspb.VersionLocalState, error) {
       // Check if we should send routing config (async mode)
       if d.hasMinVersion(AsyncSetCurrentAndRamping) {
           // Send entire routing config (which includes revision_number)
           versionUpdateArgs.RoutingConfig = d.State.RoutingConfig
       }

       activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
       var res deploymentspb.SyncVersionStateActivityResult
       err := workflow.ExecuteActivity(activityCtx, d.a.SyncWorkerDeploymentVersion, &deploymentspb.SyncVersionStateActivityArgs{
           DeploymentName: d.DeploymentName,
           Version:        targetVersion,
           UpdateArgs:     versionUpdateArgs,
           RequestId:      d.newUUID(ctx),
       }).Get(ctx, &res)

       // ... rest of function
   }
   ```

3. **Update both SetCurrent and SetRamping handlers** to use the modified syncVersion function (already does, no change needed)

---

## 2. Modify Version Workflow `handleSyncState` (version_workflow.go)

### Location
`service/worker/workerdeployment/version_workflow.go:495-572`

### Changes Required

**Current Behavior:**
- Receives `SyncVersionStateUpdateArgs` with routing info for this version only
- Syncs to all task queues using `update_version_data` field (old sync propagation)
- Waits for propagation to complete before returning

**New Behavior:**
- Check if `routing_config` is present in input args
- **If routing config present (async mode):**
  - Use `update_routing_config` + `upsert_versions_data` fields in proto
  - Start async propagation (don't wait for completion)
  - Launch background goroutine to track propagation completion
  - Signal deployment workflow when propagation finishes
- **If no routing config (sync mode):**
  - Keep existing behavior with `update_version_data` field
  - Wait for propagation to complete

### Implementation Steps

1. **Add new proto fields to sync request:**
   ```protobuf
   // In api/deployment/v1/message.proto
   message SyncDeploymentVersionUserDataRequest {
     // Existing field (sync mode):
     DeploymentVersionData data = X;

     // New fields (async mode):
     temporal.api.deployment.v1.RoutingConfig update_routing_config = Y;
     temporal.api.deployment.v1.WorkerDeploymentVersionData upsert_version_data = Z;
   }
   ```

2. **Modify handleSyncState function:**
   ```go
   func (d *VersionWorkflowRunner) handleSyncState(ctx workflow.Context, args *deploymentspb.SyncVersionStateUpdateArgs) (*deploymentspb.SyncVersionStateResponse, error) {
       // ... existing lock acquisition and validation ...

       state := d.GetVersionState()
       newStatus := d.findNewVersionStatus(args)

       // Determine propagation mode based on routing config presence
       isAsyncMode := args.RoutingConfig != nil

       if isAsyncMode {
           // ASYNC MODE: propagate full routing config
           // Revision number is extracted from routing_config.revision_number
           err = d.syncRoutingConfigToTaskQueues(ctx, args.RoutingConfig)
       } else {
           // SYNC MODE: propagate only version data (existing behavior)
           versionData := &deploymentspb.DeploymentVersionData{
               Version:           d.VersionState.Version,
               RoutingUpdateTime: args.RoutingUpdateTime,
               CurrentSinceTime:  args.CurrentSinceTime,
               RampingSinceTime:  args.RampingSinceTime,
               RampPercentage:    args.RampPercentage,
               Status:            newStatus,
           }
           err = d.syncVersionDataToTaskQueues(ctx, versionData)
       }

       if err != nil {
           return nil, err
       }

       // ... update local state ...

       return &deploymentspb.SyncVersionStateResponse{
           VersionState: state,
       }, nil
   }
   ```

3. **Add new syncRoutingConfigToTaskQueues function:**
   ```go
   // syncRoutingConfigToTaskQueues performs async propagation of routing config
   func (d *VersionWorkflowRunner) syncRoutingConfigToTaskQueues(
       ctx workflow.Context,
       routingConfig *deploymentpb.RoutingConfig,
   ) error {
       state := d.GetVersionState()

       // Build WorkerDeploymentVersionData for this version from current state
       versionData := &deploymentpb.WorkerDeploymentVersionData{
           Status:            state.Status,
       }

       // Build sync request with routing config (async mode)
       syncReq := &deploymentspb.SyncDeploymentVersionUserDataRequest{
           Version:             state.GetVersion(),
           UpdateRoutingConfig: routingConfig,
           UpsertVersionData:   versionData,
       }

       // Send to task queues in batches (similar to existing logic)
       batches := d.buildBatchesForTaskQueues(syncReq)

       // Start async propagation - DON'T WAIT
       // Extract revision number from routing config
       revisionNumber := routingConfig.RevisionNumber
       workflow.Go(ctx, func(ctx workflow.Context) {
           d.trackAsyncPropagation(ctx, batches, revisionNumber)
       })

       return nil
   }
   ```

4. **Add propagation tracking function:**
   ```go
   // trackAsyncPropagation monitors propagation and signals completion
   func (d *VersionWorkflowRunner) trackAsyncPropagation(
       ctx workflow.Context,
       batches [][]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData,
       revisionNumber int64,
   ) {
       var maxVersionsToCheck []*deploymentspb.TaskQueueMaxVersion

       // Execute all batches
       for _, batch := range batches {
           activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
           var syncRes deploymentspb.SyncDeploymentVersionUserDataResponse

           err := workflow.ExecuteActivity(activityCtx, d.a.SyncDeploymentVersionUserData, &deploymentspb.SyncDeploymentVersionUserDataRequest{
               Version: d.VersionState.GetVersion(),
               Sync:    batch,
           }).Get(ctx, &syncRes)

           if err != nil {
               d.logger.Error("async propagation batch failed", "error", err)
               // Continue with other batches
               continue
           }

           // Only check propagation for task queues where routing config actually changed
           // Task queues with max_version = -1 indicate no routing change, skip those
           for _, tqMaxVersion := range syncRes.TaskQueueMaxVersions {
               if tqMaxVersion.MaxVersion != -1 {
                   maxVersionsToCheck = append(maxVersionsToCheck, tqMaxVersion)
               }
           }
       }

       // Wait for propagation to complete only for task queues where config changed
       if len(maxVersionsToCheck) > 0 {
           activityCtx := workflow.WithActivityOptions(ctx, defaultActivityOptions)
           err := workflow.ExecuteActivity(
               activityCtx,
               d.a.CheckWorkerDeploymentUserDataPropagation,
               &deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{
                   TaskQueueMaxVersions: maxVersionsToCheck,
               }).Get(ctx, nil)

           if err != nil {
               d.logger.Error("async propagation check failed", "error", err)
               return
           }
       }

       // Signal deployment workflow that propagation completed
       d.signalPropagationComplete(ctx, revisionNumber)
   }
   ```

---

## 3. Add Propagation Completion Signal to Deployment Workflow

### Location
`service/worker/workerdeployment/workflow.go`

### Changes Required

**New Signal Handler:**
- Add new signal: `PropagationCompleteSignal`
- Signal payload: `{ revision_number: int64, build_id: string }`
- Track completion state per revision in deployment workflow

### Implementation Steps

1. **Add signal handler in listenToSignals function (line 92):**
   ```go
   func (d *WorkflowRunner) listenToSignals(ctx workflow.Context) {
       forceCANSignalChannel := workflow.GetSignalChannel(ctx, ForceCANSignalName)
       syncVersionSummaryChannel := workflow.GetSignalChannel(ctx, SyncVersionSummarySignal)
       propagationCompleteChannel := workflow.GetSignalChannel(ctx, PropagationCompleteSignal)

       // ... existing signal handlers ...

       d.signalHandler.signalSelector.AddReceive(propagationCompleteChannel, func(c workflow.ReceiveChannel, more bool) {
           d.signalHandler.processingSignals++
           defer func() { d.signalHandler.processingSignals-- }()

           var completion *deploymentspb.PropagationCompletionInfo
           c.Receive(ctx, &completion)
           d.handlePropagationComplete(completion)
           d.setStateChanged()
       })

       // Keep waiting for signals...
   }
   ```

2. **Add tracking state to WorkflowRunner:**
   ```go
   type WorkflowRunner struct {
       // ... existing fields ...

       // Track async propagations in progress per build ID
       // Map: build_id -> set of revision numbers currently propagating
       // Completed propagations are removed from the map
       inProgressPropagations map[string]map[int64]bool
   }
   ```

3. **Add handler function:**
   ```go
   func (d *WorkflowRunner) handlePropagationComplete(completion *deploymentspb.PropagationCompletionInfo) {
       if d.inProgressPropagations == nil {
           d.inProgressPropagations = make(map[string]map[int64]bool)
       }

       buildId := completion.BuildId
       revisionNumber := completion.RevisionNumber

       // Remove this revision from in-progress tracking for this build
       if revisions, ok := d.inProgressPropagations[buildId]; ok {
           delete(revisions, revisionNumber)
           // Clean up empty build id entries
           if len(revisions) == 0 {
               delete(d.inProgressPropagations, buildId)
           }
       }

       d.logger.Info("Propagation completed for revision",
           "revision", revisionNumber,
           "build_id", buildId)
   }
   ```

4. **Add signal sender in version workflow:**
   ```go
   // In version_workflow.go
   func (d *VersionWorkflowRunner) signalPropagationComplete(ctx workflow.Context, revisionNumber int64) {
       err := workflow.SignalExternalWorkflow(
           ctx,
           worker_versioning.GenerateDeploymentWorkflowID(d.VersionState.Version.DeploymentName),
           "",
           PropagationCompleteSignal,
           &deploymentspb.PropagationCompletionInfo{
               RevisionNumber: revisionNumber,
               BuildId:        d.VersionState.Version.BuildId,
           },
       ).Get(ctx, nil)

       if err != nil {
           d.logger.Error("could not signal propagation completion", "error", err)
       }
   }
   ```

---

## 4. Proto Changes Required

### New Messages

```protobuf
// In api/deployment/v1/message.proto

message PropagationCompletionInfo {
  int64 revision_number = 1;
  string build_id = 2;
}

// Updates to existing messages:
message SyncVersionStateUpdateArgs {
  // ... existing fields ...
  temporal.api.deployment.v1.RoutingConfig routing_config = 10;
  // Note: revision_number is inside routing_config, no separate field needed
}

message SyncDeploymentVersionUserDataRequest {
  // Existing: used in sync mode
  DeploymentVersionData data = X;

  // New: used in async mode
  temporal.api.deployment.v1.RoutingConfig update_routing_config = Y;
  temporal.api.deployment.v1.WorkerDeploymentVersionData upsert_version_data = Z;
}

message SyncDeploymentVersionUserDataResponse {
  // Existing field
  // Note: For task queues where routing config didn't change, max_version is set to -1
  // to signal that propagation check can be skipped for that task queue
  repeated TaskQueueMaxVersion task_queue_max_versions = 1;
}
```

---

## 5. Constants and Signal Names

### Add to constants.go or workflow.go

```go
const (
    // Existing signals
    ForceCANSignalName        = "force-can"
    SyncVersionSummarySignal  = "sync-version-summary"

    // New signal
    PropagationCompleteSignal = "propagation-complete"
)
```

---

## 6. Testing Strategy

### Unit Tests

1. **Test sync mode (no routing config):**
   - Verify old `update_version_data` field is used
   - Verify propagation waits before returning

2. **Test async mode (with routing config):**
   - Verify `update_routing_config` + `upsert_versions_data` are used
   - Verify handleSyncState returns immediately
   - Verify propagation tracking goroutine launches
   - Verify signal is sent to deployment workflow

3. **Test deployment workflow signal handling:**
   - Verify signal is received
   - Verify completion tracking works

### Integration Tests

1. Test full flow: SetCurrent → version workflow → task queues → signal back
2. Test rollback scenarios
3. Test concurrent routing changes

---

## 7. Rollout Considerations

### Versioning Strategy

1. **Phase 1:** Add proto fields (backward compatible)
2. **Phase 2:** Deploy code with feature flag / version check
3. **Phase 3:** Enable async mode for new workflows via `hasMinVersion` check

### Backward Compatibility

- Old deployments continue using sync mode (no routing config sent)
- New deployments can opt into async mode via version check
- Proto changes are additive only

### Monitoring

- Add metrics for async propagation latency
- Add metrics for signal delivery success/failure
- Monitor propagation completion times per revision

---

## 8. Implementation Order

1. ✅ **Proto changes** - Add new fields to protos
2. ✅ **Deployment workflow signal handler** - Add propagation complete signal
3. ✅ **Version workflow async propagation** - Add tracking and signaling logic
4. ✅ **SyncWorkerDeploymentVersion update** - Include routing config when hasMinVersion
5. ✅ **Testing** - Unit and integration tests
6. ✅ **Metrics and monitoring** - Add observability

---

## 9. Edge Cases to Handle

1. **Signal delivery failure:**
   - Retry logic in version workflow
   - Timeout handling

2. **Propagation partial failure:**
   - Track which task queues succeeded/failed
   - Retry failed partitions

3. **Multiple concurrent routing updates:**
   - Use revision numbers to track which propagation completed
   - Handle out-of-order signal delivery

4. **Version workflow CaN during propagation:**
   - Ensure goroutine state is preserved across CaN
   - May need to persist propagation state

5. **Deployment workflow CaN before receiving signal:**
   - Signal will be delivered to new run (signals follow workflow)
   - Ensure completion map persists in workflow args

---

## 10. Open Questions

1. **Q:** Should we persist in-progress propagation state in deployment workflow local state?
   **A:** Yes - add `inProgressPropagations` to `WorkerDeploymentLocalState` to survive CaN

2. **Q:** What timeout should we use for propagation completion?
   **A:** Use existing activity timeout (defaultActivityOptions)

3. **Q:** Should we expose propagation status in deployment query?
   **A:** Yes - useful for debugging, add to QueryDescribeWorkerDeploymentResponse

4. **Q:** Handle case where version workflow is deleted before signaling?
   **A:** Deployment workflow should have timeout/fallback logic

---

## Summary

This plan implements async routing config propagation by:
1. Sending full routing config from deployment → version workflow when `hasMinVersion(async)`
2. Version workflow uses new proto fields (`update_routing_config`, `upsert_version_data` with WorkerDeploymentVersionData)
3. Version workflow doesn't block on propagation - launches background tracking
4. Background goroutine only waits for task queues where routing actually changed (max_version = -1 means no change)
5. Background goroutine signals deployment workflow when propagation completes (with build_id and revision_number)
6. Deployment workflow tracks in-progress propagations per build_id, removes completed ones
7. Sync mode (old behavior) preserved for backward compatibility
