package tquserdata

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
)

type (
	// GetRequest identifies the task-queue family to read.
	GetRequest struct {
		NamespaceID   namespace.ID
		TaskQueueName string
	}

	// SetRequest carries the user data to write for a task-queue family.
	SetRequest struct {
		NamespaceID   namespace.ID
		TaskQueueName string
		Data          *persistencespb.TaskQueueUserData
		KnownVersion  int64
	}
)

// executionKey maps a task-queue family to its CHASM execution. The family
// name is the business ID, scoped by namespace, matching today's key.
func executionKey(nsID namespace.ID, taskQueueName string) chasm.ExecutionKey {
	return chasm.ExecutionKey{
		NamespaceID: nsID.String(),
		BusinessID:  taskQueueName,
	}
}

// Get reads the versioned user data for a task-queue family from its CHASM
// component. Returns a NotFound error if the component does not exist.
//
// These handlers call the CHASM engine, which lives on history shards, so they
// must run inside a history request context (where the engine is installed).
func Get(ctx context.Context, req GetRequest) (*persistencespb.VersionedTaskQueueUserData, error) {
	return chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*UserData](executionKey(req.NamespaceID, req.TaskQueueName)),
		(*UserData).Get,
		nil,
	)
}

// Set writes the user data for a task-queue family, creating the component on
// first write, and returns the new versioned value.
func Set(ctx context.Context, req SetRequest) (*persistencespb.VersionedTaskQueueUserData, error) {
	result, err := chasm.UpdateWithStartExecution(
		ctx,
		executionKey(req.NamespaceID, req.TaskQueueName),
		NewUserData,
		(*UserData).Set,
		SetInput{Data: req.Data, KnownVersion: req.KnownVersion},
	)
	if err != nil {
		return nil, err
	}
	return result.UpdateOutput, nil
}
