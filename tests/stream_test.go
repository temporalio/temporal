package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type streamSuite struct {
	testcore.FunctionalTestBase
	tv *testvars.TestVars
}

func TestStreamSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(streamSuite))
}

func (s *streamSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuite()
	s.OverrideDynamicConfig(
		dynamicconfig.EnableChasm,
		true,
	)
	s.OverrideDynamicConfig(
		activity.Enabled,
		true,
	)
}

func (s *streamSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.tv = testvars.New(s.T())
}

func (s *streamSuite) TestHappyPath() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err := s.FrontendClient().AddToStream(ctx, &workflowservice.AddToStreamRequest{
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	_, err = s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
}
