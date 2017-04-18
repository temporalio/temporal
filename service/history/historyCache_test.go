package history

import (
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	historyCacheSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		logger           bark.Logger
		mockExecutionMgr *mocks.ExecutionManager
		mockShard        *shardContextImpl
		cache            *historyCache
	}
)

func TestHistoryCacheSuite(t *testing.T) {
	s := new(historyCacheSuite)
	suite.Run(t, s)
}

func (s *historyCacheSuite) SetupTest() {
	s.logger = bark.NewLoggerFromLogrus(log.New())
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockShard = &shardContextImpl{
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              &mocks.ShardManager{},
		rangeSize:                 defaultRangeSize,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		logger:                    s.logger,
	}
	s.cache = newHistoryCache(historyCacheMaxSize, s.mockShard, s.logger)
}

func (s *historyCacheSuite) TestHistoryCachePinning() {
	domain := "test_domain"
	s.cache = newHistoryCache(2, s.mockShard, s.logger)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wf-cache-test"),
		RunId:      common.StringPtr(uuid.New()),
	}

	context, release, err := s.cache.getOrCreateWorkflowExecution(domain, we)
	s.Nil(err)

	we2 := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wf-cache-test"),
		RunId:      common.StringPtr(uuid.New()),
	}

	// Cache is full because context is pinned, should get an error now
	_, _, err2 := s.cache.getOrCreateWorkflowExecution(domain, we2)
	s.NotNil(err2)

	// Now release the context, this should unpin it.
	release()

	_, release2, err3 := s.cache.getOrCreateWorkflowExecution(domain, we2)
	s.Nil(err3)
	release2()

	// Old context should be evicted.
	newContext, release, err4 := s.cache.getOrCreateWorkflowExecution(domain, we)
	s.Nil(err4)
	s.False(context == newContext)
	release()
}
