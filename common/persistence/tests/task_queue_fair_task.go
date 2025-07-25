package tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

type (
	TaskQueueFairTaskSuite struct {
		suite.Suite
		*require.Assertions

		namespaceID   string
		taskQueueName string
		taskQueueType enumspb.TaskQueueType

		taskManager p.TaskManager
		logger      log.Logger

		ctx    context.Context
		cancel context.CancelFunc
	}
)

func NewTaskQueueFairTaskSuite(
	t *testing.T,
	taskManager p.TaskStore,
	logger log.Logger,
) *TaskQueueFairTaskSuite {
	return &TaskQueueFairTaskSuite{
		Assertions: require.New(t),
		taskManager: p.NewTaskManager(
			taskManager,
			serialization.NewSerializer(),
		),
		logger: logger,
	}
}

func (s *TaskQueueFairTaskSuite) SetupSuite() {
}

func (s *TaskQueueFairTaskSuite) TearDownSuite() {
}

func (s *TaskQueueFairTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)

	s.namespaceID = uuid.New().String()
	s.taskQueueName = uuid.New().String()
	s.taskQueueType = enumspb.TaskQueueType(rand.Int31n(
		int32(len(enumspb.TaskQueueType_name)) + 1),
	)
}

func (s *TaskQueueFairTaskSuite) TearDownTest() {
	s.cancel()
}

// FIXME: new tests here
