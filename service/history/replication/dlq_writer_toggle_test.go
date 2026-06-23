package replication
package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/configs"
)

type (
	dlqWriterToggleTestExecutionManager struct {
		requests []*persistence.PutReplicationTaskToDLQRequest
		err      error
	}

	dlqWriterToggleSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func (m *dlqWriterToggleTestExecutionManager) PutReplicationTaskToDLQ(
	_ context.Context,
	request *persistence.PutReplicationTaskToDLQRequest,
) error {
	m.requests = append(m.requests, request)
	return m.err
}

func TestDLQWriterToggleSuite(t *testing.T) {
	suite.Run(t, new(dlqWriterToggleSuite))
}

func (s *dlqWriterToggleSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *dlqWriterToggleSuite) newConfig(dlqV2 bool) *configs.Config {
	col := dynamicconfig.NewCollection(
		dynamicconfig.StaticClient(map[dynamicconfig.Key]any{
			dynamicconfig.EnableHistoryReplicationDLQV2.Key(): dlqV2,
		}),
		log.NewNoopLogger(),
	)
	return configs.NewConfig(col, 1)
}

func (s *dlqWriterToggleSuite) TestNewDLQWriterToggle() {
	execManager := &dlqWriterToggleTestExecutionManager{}
	toggle := newDLQWriterToggle(dlqWriterToggleParams{
		Config:                    s.newConfig(false),
		ExecutionManagerDLQWriter: NewExecutionManagerDLQWriter(execManager),
		DLQWriterAdapter:          nil,
		TestHooks:                 testhooks.NewTestHooks(),
	})
	s.NotNil(toggle)
}

func (s *dlqWriterToggleSuite) TestWriteTaskToDLQ_V1() {
	execManager := &dlqWriterToggleTestExecutionManager{}
	toggle := newDLQWriterToggle(dlqWriterToggleParams{
		Config:                    s.newConfig(false),
		ExecutionManagerDLQWriter: NewExecutionManagerDLQWriter(execManager),
		DLQWriterAdapter:          nil,
		TestHooks:                 testhooks.NewTestHooks(),
	})

	replicationTaskInfo := &persistencespb.ReplicationTaskInfo{TaskId: 21}
	err := toggle.WriteTaskToDLQ(context.Background(), DLQWriteRequest{
		SourceShardID:       13,
		TargetShardID:       26,
		SourceCluster:       "test-source-cluster",
		ReplicationTaskInfo: replicationTaskInfo,
	})
	s.NoError(err)
	s.Len(execManager.requests, 1)
	s.Equal(int32(26), execManager.requests[0].ShardID)
	s.Equal("test-source-cluster", execManager.requests[0].SourceClusterName)
}

func (s *dlqWriterToggleSuite) TestWriteTaskToDLQ_V1_Error() {
	wantErr := errors.New("dlq-toggle-write-error")
	execManager := &dlqWriterToggleTestExecutionManager{err: wantErr}
	toggle := newDLQWriterToggle(dlqWriterToggleParams{
		Config:                    s.newConfig(false),
		ExecutionManagerDLQWriter: NewExecutionManagerDLQWriter(execManager),
		DLQWriterAdapter:          nil,
		TestHooks:                 testhooks.NewTestHooks(),
	})

	err := toggle.WriteTaskToDLQ(context.Background(), DLQWriteRequest{
		ReplicationTaskInfo: &persistencespb.ReplicationTaskInfo{TaskId: 1},
	})
	s.ErrorIs(err, wantErr)
}
