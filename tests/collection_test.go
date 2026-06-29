package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/collection"
	collectionpb "go.temporal.io/server/chasm/lib/collection/gen/collectionpb/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

const collectionTestTimeout = 10 * time.Second * debug.TimeoutMultiplier

type CollectionTestSuite struct {
	testcore.FunctionalTestBase

	chasmContext context.Context
	handler      collectionpb.CollectionServiceServer
}

func TestCollectionTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(CollectionTestSuite))
}

func (s *CollectionTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():                            true,
			dynamicconfig.DeleteNamespaceUseChasmDeleteExecution.Key(): true,
		}),
	)

	chasmEngine, err := s.FunctionalTestBase.GetTestCluster().Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(chasmEngine)

	s.chasmContext = chasm.NewEngineContext(context.Background(), chasmEngine)
	s.handler = collection.NewHandler(s.Logger)
}

func (s *CollectionTestSuite) TestCollectionLifecycle() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, collectionTestTimeout)
	defer cancel()

	collectionID := tv.Any().String()

	startResp, err := s.handler.StartCollectionExecution(ctx, &collectionpb.StartCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
		RequestId:    tv.Any().String(),
	})
	s.NoError(err)
	s.NotEmpty(startResp.GetRunId())

	descResp, err := s.handler.DescribeCollectionExecution(ctx, &collectionpb.DescribeCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
	})
	s.NoError(err)
	s.False(descResp.GetState().GetClosed())

	_, err = s.handler.CloseCollectionExecution(ctx, &collectionpb.CloseCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
	})
	s.NoError(err)

	descResp, err = s.handler.DescribeCollectionExecution(ctx, &collectionpb.DescribeCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
	})
	s.NoError(err)
	s.True(descResp.GetState().GetClosed())
}

func (s *CollectionTestSuite) TestCollectionStart_AlreadyRunning() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(s.chasmContext, collectionTestTimeout)
	defer cancel()

	collectionID := tv.Any().String()
	req := &collectionpb.StartCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
		RequestId:    tv.Any().String(),
	}

	_, err := s.handler.StartCollectionExecution(ctx, req)
	s.NoError(err)

	_, err = s.handler.StartCollectionExecution(ctx, &collectionpb.StartCollectionExecutionRequest{
		NamespaceId:  s.NamespaceID().String(),
		CollectionId: collectionID,
		RequestId:    tv.Any().String(),
	})
	s.Error(err)
}
