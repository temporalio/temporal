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

// Get reads the user data for a task-queue family from its CHASM component.
//
// These handlers call the CHASM engine, which lives on history shards, so they
// must run inside a history request context (where the engine is installed).
func Get(ctx context.Context, req GetRequest) (*persistencespb.TaskQueueUserData, error) {
	return chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*UserData](executionKey(req.NamespaceID, req.TaskQueueName)),
		(*UserData).Get,
		nil,
	)
}

// Set writes the user data for a task-queue family, creating the component on
// first write.
func Set(ctx context.Context, req SetRequest) (*persistencespb.TaskQueueUserData, error) {
	result, err := chasm.UpdateWithStartExecution(
		ctx,
		executionKey(req.NamespaceID, req.TaskQueueName),
		NewUserData,
		(*UserData).Set,
		req.Data,
	)
	if err != nil {
		return nil, err
	}
	return result.UpdateOutput, nil
}
