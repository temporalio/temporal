//go:build test_dep

package persistence_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	mockp "go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestExecutionManagerAddHistoryTasksCallsHistoryTasksWrittenHook(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil)

	hooks := testhooks.NewTestHooks()
	var writes []testhooks.HistoryTaskWrite
	cleanup := testhooks.Set(
		hooks,
		testhooks.HistoryTasksWritten,
		func(write testhooks.HistoryTaskWrite) {
			writes = append(writes, write)
		},
		testhooks.GlobalScope,
	)
	t.Cleanup(cleanup)

	em := p.NewExecutionManager(
		store,
		serialization.NewSerializer(),
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
		hooks,
	)

	tasksByCategory := map[tasks.Category][]tasks.Task{}
	err := em.AddHistoryTasks(context.Background(), &p.AddHistoryTasksRequest{
		ShardID:     12,
		RangeID:     34,
		NamespaceID: "namespace-id",
		WorkflowID:  "workflow-id",
		Tasks:       tasksByCategory,
	})
	require.NoError(t, err)
	require.Equal(t, []testhooks.HistoryTaskWrite{{
		ShardID:     12,
		RangeID:     34,
		NamespaceID: "namespace-id",
		WorkflowID:  "workflow-id",
		Tasks:       tasksByCategory,
	}}, writes)
}

func TestExecutionManagerAddHistoryTasksDoesNotCallHistoryTasksWrittenHookOnError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(errors.New("write failed"))

	hooks := testhooks.NewTestHooks()
	var writes []testhooks.HistoryTaskWrite
	cleanup := testhooks.Set(
		hooks,
		testhooks.HistoryTasksWritten,
		func(write testhooks.HistoryTaskWrite) {
			writes = append(writes, write)
		},
		testhooks.GlobalScope,
	)
	t.Cleanup(cleanup)

	em := p.NewExecutionManager(
		store,
		serialization.NewSerializer(),
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
		hooks,
	)

	err := em.AddHistoryTasks(context.Background(), &p.AddHistoryTasksRequest{
		ShardID:     12,
		RangeID:     34,
		NamespaceID: "namespace-id",
		WorkflowID:  "workflow-id",
		Tasks:       map[tasks.Category][]tasks.Task{},
	})
	require.Error(t, err)
	require.Empty(t, writes)
}
