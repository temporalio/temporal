package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
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
		StreamId:  "my-stream-id",
		Messages: []*commonpb.Payload{
			{
				Data: []byte("hello"),
			},
			{
				Data: []byte("world"),
			},
		},
	})
	s.NoError(err)

	resp, err := s.FrontendClient().PollStream(ctx, &workflowservice.PollStreamRequest{
		Namespace: s.Namespace().String(),
		StreamId:  "my-stream-id",
	})
	s.NoError(err)
	messages := resp.GetMessages()
	s.Equal(2, len(messages))
	s.Equal([]byte("hello"), messages[0].GetData())
	s.Equal([]byte("world"), messages[1].GetData())

}
