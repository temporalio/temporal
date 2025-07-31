package sqlplugin

import "context"

type (
	GetTaskQueueUserDataRequest struct {
		NamespaceID   []byte
		TaskQueueName string
	}

	ListTaskQueueUserDataEntriesRequest struct {
		NamespaceID       []byte
		LastTaskQueueName string
		Limit             int
	}

	AddToBuildIdToTaskQueueMapping struct {
		NamespaceID   []byte
		TaskQueueName string
		BuildIds      []string
	}

	RemoveFromBuildIdToTaskQueueMapping struct {
		NamespaceID   []byte
		TaskQueueName string
		BuildIds      []string
	}

	GetTaskQueuesByBuildIdRequest struct {
		NamespaceID []byte
		BuildID     string
	}

	CountTaskQueuesByBuildIdRequest struct {
		NamespaceID []byte
		BuildID     string
	}

	TaskQueueUserDataEntry struct {
		TaskQueueName string
		VersionedBlob
	}

	MatchingTaskQueueUserData interface {
		GetTaskQueueUserData(ctx context.Context, request *GetTaskQueueUserDataRequest) (*VersionedBlob, error)
		UpdateTaskQueueUserData(ctx context.Context, request *UpdateTaskQueueDataRequest) error
		ListTaskQueueUserDataEntries(ctx context.Context, request *ListTaskQueueUserDataEntriesRequest) ([]TaskQueueUserDataEntry, error)

		AddToBuildIdToTaskQueueMapping(ctx context.Context, request AddToBuildIdToTaskQueueMapping) error
		RemoveFromBuildIdToTaskQueueMapping(ctx context.Context, request RemoveFromBuildIdToTaskQueueMapping) error
		GetTaskQueuesByBuildId(ctx context.Context, request *GetTaskQueuesByBuildIdRequest) ([]string, error)
		CountTaskQueuesByBuildId(ctx context.Context, request *CountTaskQueuesByBuildIdRequest) (int, error)
	}
)
