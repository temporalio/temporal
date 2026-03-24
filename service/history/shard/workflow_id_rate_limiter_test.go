package shard

import (
	"testing"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type workflowIDRateLimiterSuite struct {
	suite.Suite
	*require.Assertions
	controller *gomock.Controller
	shard      *ContextTest
}

func TestWorkflowIDRateLimiter(t *testing.T) {
	suite.Run(t, new(workflowIDRateLimiterSuite))
}

func (s *workflowIDRateLimiterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.shard = NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{ShardId: 1},
		configs.NewConfig(dynamicconfig.NewNoopCollection(), 1),
	)
}

func (s *workflowIDRateLimiterSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *workflowIDRateLimiterSuite) TestGetWorkflowIDReuseRL_Disabled() {
	// rps=0 means disabled; GetWorkflowIDReuseRL should return nil
	s.shard.config.WorkflowIDStartRPSPerInstance = func(_ string) int { return 0 }
	nsID := namespace.ID("test-ns-id")
	s.Nil(s.shard.GetWorkflowIDReuseRL(nsID, "wf-id"))
}

func (s *workflowIDRateLimiterSuite) TestGetWorkflowIDReuseRL_AllowsUnderLimit() {
	// 10 RPS with burst=10: first 10 calls to Allow() should succeed
	s.shard.config.WorkflowIDStartRPSPerInstance = func(_ string) int { return 10 }
	nsID := namespace.ID("test-ns-id")
	rl := s.shard.GetWorkflowIDReuseRL(nsID, "wf-id")
	s.NotNil(rl)
	for i := 0; i < 10; i++ {
		s.True(rl.Allow())
	}
}

func (s *workflowIDRateLimiterSuite) TestGetWorkflowIDReuseRL_BlocksOverLimit() {
	// 1 RPS with burst=1: second Allow() should return false
	s.shard.config.WorkflowIDStartRPSPerInstance = func(_ string) int { return 1 }
	nsID := namespace.ID("test-ns-id")
	rl := s.shard.GetWorkflowIDReuseRL(nsID, "wf-id")
	s.NotNil(rl)
	s.True(rl.Allow())
	s.False(rl.Allow())
}

func (s *workflowIDRateLimiterSuite) TestGetWorkflowIDReuseRL_IndependentWorkflowIDs() {
	// Different workflow IDs should have independent rate limiters
	s.shard.config.WorkflowIDStartRPSPerInstance = func(_ string) int { return 1 }
	nsID := namespace.ID("test-ns-id")
	rl1 := s.shard.GetWorkflowIDReuseRL(nsID, "wf-id-1")
	rl2 := s.shard.GetWorkflowIDReuseRL(nsID, "wf-id-2")
	s.True(rl1.Allow())
	// rl1 is exhausted, but rl2 should still allow
	s.True(rl2.Allow())
}

func (s *workflowIDRateLimiterSuite) TestGetWorkflowIDReuseRL_IndependentNamespaces() {
	// Same workflow ID in different namespaces should have independent rate limiters
	s.shard.config.WorkflowIDStartRPSPerInstance = func(_ string) int { return 1 }
	ns1 := namespace.ID("ns-1")
	ns2 := namespace.ID("ns-2")
	rl1 := s.shard.GetWorkflowIDReuseRL(ns1, "wf-id")
	rl2 := s.shard.GetWorkflowIDReuseRL(ns2, "wf-id")
	s.True(rl1.Allow())
	// rl1 is exhausted, but rl2 should still allow
	s.True(rl2.Allow())
}

func (s *workflowIDRateLimiterSuite) TestGetWorkflowIDReuseRL_BurstRatio() {
	// rps=2, ratio=3 → burst=6; first 6 Allow() calls should succeed
	s.shard.config.WorkflowIDStartRPSPerInstance = func(_ string) int { return 2 }
	s.shard.config.WorkflowIDStartBurstRatio = func(_ string) float64 { return 3.0 }
	nsID := namespace.ID("test-ns-id")
	rl := s.shard.GetWorkflowIDReuseRL(nsID, "wf-id")
	s.NotNil(rl)
	for i := 0; i < 6; i++ {
		s.True(rl.Allow(), "expected Allow() on call %d", i+1)
	}
	s.False(rl.Allow(), "expected Allow() to be false after burst exhausted")
}

func (s *workflowIDRateLimiterSuite) TestGetWorkflowIDReuseRL_BurstUpdatesOnConfigChange() {
	// Start with rps=2, ratio=1 (burst=2), then change ratio to 3 (burst=6).
	s.shard.config.WorkflowIDStartRPSPerInstance = func(_ string) int { return 2 }
	s.shard.config.WorkflowIDStartBurstRatio = func(_ string) float64 { return 1.0 }
	nsID := namespace.ID("test-ns-id")
	s.shard.GetWorkflowIDReuseRL(nsID, "wf-id") // populate cache

	s.shard.config.WorkflowIDStartBurstRatio = func(_ string) float64 { return 3.0 }
	rl := s.shard.GetWorkflowIDReuseRL(nsID, "wf-id") // should update burst to 6
	s.Equal(6, rl.(*quotas.RateLimiterImpl).Burst())
}
