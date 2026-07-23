package tests

import (
	"context"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

// sqlMutableStateTableCounts reports surviving row counts per table that SQL uses
// to persist a WorkflowMutableState's sub-collections, so delete assertions can
// detect orphaned rows (e.g. chasm_node_maps). Omits the anchor executions row
// (verified via GetWorkflowExecution) and the current_executions /
// current_chasm_executions pointers, which DeleteCurrentWorkflowExecution clears
// rather than DeleteWorkflowExecution.
func sqlMutableStateTableCounts(
	db sqlplugin.DB,
) func(ctx context.Context, shardID int32, namespaceID, workflowID, runID string) (map[string]int, error) {
	return func(ctx context.Context, shardID int32, namespaceID, workflowID, runID string) (map[string]int, error) {
		nsID := primitives.MustParseUUID(namespaceID)
		rID := primitives.MustParseUUID(runID)
		counts := make(map[string]int)

		activity, err := db.SelectAllFromActivityInfoMaps(ctx, sqlplugin.ActivityInfoMapsAllFilter{
			ShardID: shardID, NamespaceID: nsID, WorkflowID: workflowID, RunID: rID,
		})
		if err != nil {
			return nil, err
		}
		counts["activity_info_maps"] = len(activity)

		timer, err := db.SelectAllFromTimerInfoMaps(ctx, sqlplugin.TimerInfoMapsAllFilter{
			ShardID: shardID, NamespaceID: nsID, WorkflowID: workflowID, RunID: rID,
		})
		if err != nil {
			return nil, err
		}
		counts["timer_info_maps"] = len(timer)

		child, err := db.SelectAllFromChildExecutionInfoMaps(ctx, sqlplugin.ChildExecutionInfoMapsAllFilter{
			ShardID: shardID, NamespaceID: nsID, WorkflowID: workflowID, RunID: rID,
		})
		if err != nil {
			return nil, err
		}
		counts["child_execution_info_maps"] = len(child)

		requestCancel, err := db.SelectAllFromRequestCancelInfoMaps(ctx, sqlplugin.RequestCancelInfoMapsAllFilter{
			ShardID: shardID, NamespaceID: nsID, WorkflowID: workflowID, RunID: rID,
		})
		if err != nil {
			return nil, err
		}
		counts["request_cancel_info_maps"] = len(requestCancel)

		signal, err := db.SelectAllFromSignalInfoMaps(ctx, sqlplugin.SignalInfoMapsAllFilter{
			ShardID: shardID, NamespaceID: nsID, WorkflowID: workflowID, RunID: rID,
		})
		if err != nil {
			return nil, err
		}
		counts["signal_info_maps"] = len(signal)

		signalsRequested, err := db.SelectAllFromSignalsRequestedSets(ctx, sqlplugin.SignalsRequestedSetsAllFilter{
			ShardID: shardID, NamespaceID: nsID, WorkflowID: workflowID, RunID: rID,
		})
		if err != nil {
			return nil, err
		}
		counts["signals_requested_sets"] = len(signalsRequested)

		buffered, err := db.SelectFromBufferedEvents(ctx, sqlplugin.BufferedEventsFilter{
			ShardID: shardID, NamespaceID: nsID, WorkflowID: workflowID, RunID: rID,
		})
		if err != nil {
			return nil, err
		}
		counts["buffered_events"] = len(buffered)

		chasm, err := db.SelectAllFromChasmNodeMaps(ctx, sqlplugin.ChasmNodeMapsAllFilter{
			ShardID: shardID, NamespaceID: nsID, WorkflowID: workflowID, RunID: rID,
		})
		if err != nil {
			return nil, err
		}
		counts["chasm_node_maps"] = len(chasm)

		return counts, nil
	}
}
