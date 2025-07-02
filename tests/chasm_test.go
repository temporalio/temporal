package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type ChasmTestSuite struct {
	testcore.FunctionalTestBase

	chasmEngine chasm.Engine
}

func TestChasmTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ChasmTestSuite))
}

func (s *ChasmTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key(): true,
		}),
	)

	var err error
	s.chasmEngine, err = s.FunctionalTestBase.GetTestCluster().Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(s.chasmEngine)
}

func (s *ChasmTestSuite) TestNewPayloadStore() {
	tv := testvars.New(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := tests.NewPayloadStoreHandler(
		chasm.NewEngineContext(ctx, s.chasmEngine),
		tests.NewPayloadStoreRequest{
			NamespaceID: s.NamespaceID(),
			StoreID:     tv.Any().String(),
		},
	)
	s.NoError(err)
}

// TODO: More tests here...
