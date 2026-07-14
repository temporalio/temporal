package matching

import (
	"context"

	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
)

// chasmUserDataStore decorates a persistence.TaskManager so that task queue
// user-data reads and writes go to the CHASM tquserdata component (hosted on
// history shards) via the history client, instead of the Astra-backed
// task_queue_user_data table. All other task operations delegate to the wrapped
// store.
//
// POC (Option A): this replaces the hot load/store path only. The reverse
// build-id index (GetTaskQueuesByBuildId) and entry scans
// (ListTaskQueueUserDataEntries) still delegate to the wrapped store, and the
// per-build-id task-queue limit is not enforced.
type chasmUserDataStore struct {
	persistence.TaskManager
	historyClient resource.HistoryClient
}

var _ persistence.TaskManager = (*chasmUserDataStore)(nil)

func newChasmUserDataStore(base persistence.TaskManager, historyClient resource.HistoryClient) *chasmUserDataStore {
	return &chasmUserDataStore{TaskManager: base, historyClient: historyClient}
}

func (s *chasmUserDataStore) GetTaskQueueUserData(
	ctx context.Context,
	request *persistence.GetTaskQueueUserDataRequest,
) (*persistence.GetTaskQueueUserDataResponse, error) {
	resp, err := s.historyClient.GetChasmTaskQueueUserData(ctx, &historyservice.GetChasmTaskQueueUserDataRequest{
		NamespaceId: request.NamespaceID,
		TaskQueue:   request.TaskQueue,
	})
	if err != nil {
		return nil, err
	}
	return &persistence.GetTaskQueueUserDataResponse{UserData: resp.GetUserData()}, nil
}

func (s *chasmUserDataStore) UpdateTaskQueueUserData(
	ctx context.Context,
	request *persistence.UpdateTaskQueueUserDataRequest,
) error {
	// The batch may carry updates for multiple task queues; each maps to its own
	// CHASM component, so we fan out one call per task queue.
	for taskQueue, update := range request.Updates {
		_, err := s.historyClient.UpdateChasmTaskQueueUserData(ctx, &historyservice.UpdateChasmTaskQueueUserDataRequest{
			NamespaceId:  request.NamespaceID,
			TaskQueue:    taskQueue,
			UserData:     update.UserData.GetData(),
			KnownVersion: update.UserData.GetVersion(),
		})
		if err != nil {
			return err
		}
		if update.Applied != nil {
			*update.Applied = true
		}
	}
	return nil
}

func (s *chasmUserDataStore) CountTaskQueuesByBuildId(
	context.Context,
	*persistence.CountTaskQueuesByBuildIdRequest,
) (int, error) {
	// POC: the per-build-id task-queue limit is not enforced against CHASM.
	// Reporting zero makes the limit check a no-op and issues no DB read.
	return 0, nil
}
