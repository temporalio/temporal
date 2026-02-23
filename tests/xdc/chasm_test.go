package xdc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	chasmTestTimeout = 30 * time.Second * debug.TimeoutMultiplier
)

type ChasmSuite struct {
	xdcBaseSuite

	chasmContext context.Context
}

func TestChasmSuite(t *testing.T) {
	t.Parallel()

	s := &ChasmSuite{}
	s.enableTransitionHistory = true
	suite.Run(t, s)
}

func (s *ChasmSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableChasm.Key():                 true,
		dynamicconfig.NamespaceMinRetentionGlobal.Key(): 1 * time.Second,
	}
	s.setupSuite()
}

func (s *ChasmSuite) SetupTest() {
	s.setupTest()

	chasmEngine, err := s.clusters[0].Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(chasmEngine)

	chasmVisibilityMgr := s.clusters[0].Host().ChasmVisibilityManager()
	s.Require().NotNil(chasmVisibilityMgr)

	s.chasmContext = chasm.NewEngineContext(context.Background(), chasmEngine)
	s.chasmContext = chasm.NewVisibilityManagerContext(s.chasmContext, chasmVisibilityMgr)
}

func (s *ChasmSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *ChasmSuite) TestRetentionTimer() {
	nsName := s.createGlobalNamespace()

	nsResp, err := s.clusters[0].FrontendClient().DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{
		Namespace: nsName,
	})
	s.NoError(err)
	nsID := nsResp.NamespaceInfo.GetId()

	tv := testvars.New(s.T())
	storeID := tv.Any().String()

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	_, err = tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      namespace.ID(nsID),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)

	chasmRegistry := s.clusters[0].Host().GetCHASMRegistry()
	archetypeID, ok := chasmRegistry.ComponentIDFor(&tests.PayloadStore{})
	s.True(ok)
	archetype, ok := chasmRegistry.ComponentFqnByID(archetypeID)
	s.True(ok)

	describeExecutionRequest := &adminservice.DescribeMutableStateRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
		},
		Archetype: archetype,
	}
	_, err = s.clusters[0].AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
	s.NoError(err)

	s.Eventually(func() bool {
		// Wait for it to be replicated to the standby cluster
		_, err = s.clusters[1].AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	// Reduce namespace retention to trigger deletion
	retention := 3 * time.Second
	_, err = s.clusters[0].FrontendClient().UpdateNamespace(testcore.NewContext(), &workflowservice.UpdateNamespaceRequest{
		Namespace: nsName,
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
		},
	})
	s.NoError(err)

	// Wait for ns update to be replicated
	s.Eventually(func() bool {
		// Wait for it to be replicated to the standby cluster
		resp, err := s.clusters[1].FrontendClient().DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{
			Namespace: nsName,
		})
		s.NoError(err)
		return resp.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration() == retention
	}, 10*time.Second, 100*time.Millisecond)

	// Wait for ns registry refresh
	time.Sleep(2 * testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo

	// Close the execution and validate it's deleted on both clusters.
	_, err = tests.ClosePayloadStoreHandler(
		ctx,
		tests.ClosePayloadStoreRequest{
			NamespaceID: namespace.ID(nsID),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	for _, cluster := range []*testcore.TestCluster{s.clusters[0], s.clusters[1]} {
		s.Eventually(func() bool {
			// Wait for replication, retention period, and retention timer task processing.
			_, err = cluster.AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
			return errors.As(err, new(*serviceerror.NotFound))
		}, 10*time.Second, 100*time.Millisecond)
	}
}
