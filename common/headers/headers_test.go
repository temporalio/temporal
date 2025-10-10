package headers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
)

type (
	HeadersSuite struct {
		suite.Suite
	}
)

func TestHeadersSuite(t *testing.T) {
	suite.Run(t, &HeadersSuite{})
}

func (s *HeadersSuite) TestPropagate_CreateNewOutgoingContext() {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:           "22.08.78",
		SupportedServerVersionsHeaderName: ">21.04.16",
		ClientNameHeaderName:              "28.08.14",
		SupportedFeaturesHeaderName:       "my-feature",
	}))

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(s.T(), ok)

	require.Equal(s.T(), "22.08.78", md.Get(ClientVersionHeaderName)[0])
	require.Equal(s.T(), ">21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	require.Equal(s.T(), "28.08.14", md.Get(ClientNameHeaderName)[0])
	require.Equal(s.T(), "my-feature", md.Get(SupportedFeaturesHeaderName)[0])
}

func (s *HeadersSuite) TestPropagate_CreateNewOutgoingContext_SomeMissing() {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName: "22.08.78",
		ClientNameHeaderName:    "28.08.14",
	}))

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(s.T(), ok)

	require.Equal(s.T(), "22.08.78", md.Get(ClientVersionHeaderName)[0])
	require.Equal(s.T(), 0, len(md.Get(SupportedServerVersionsHeaderName)))
	require.Equal(s.T(), "28.08.14", md.Get(ClientNameHeaderName)[0])
	require.Equal(s.T(), 0, len(md.Get(SupportedFeaturesHeaderName)))
}

func (s *HeadersSuite) TestPropagate_UpdateExistingEmptyOutgoingContext() {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:           "22.08.78",
		SupportedServerVersionsHeaderName: "<21.04.16",
		ClientNameHeaderName:              "28.08.14",
		SupportedFeaturesHeaderName:       "my-feature",
	}))

	ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(s.T(), ok)

	require.Equal(s.T(), "22.08.78", md.Get(ClientVersionHeaderName)[0])
	require.Equal(s.T(), "<21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	require.Equal(s.T(), "28.08.14", md.Get(ClientNameHeaderName)[0])
	require.Equal(s.T(), "my-feature", md.Get(SupportedFeaturesHeaderName)[0])
}

func (s *HeadersSuite) TestPropagate_UpdateExistingNonEmptyOutgoingContext() {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:           "07.08.78",   // Must be ignored
		SupportedServerVersionsHeaderName: "<07.04.16",  // Must be ignored
		SupportedFeaturesHeaderName:       "my-feature", // Passed through
	}))

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:           "22.08.78",
		SupportedServerVersionsHeaderName: "<21.04.16",
		ClientNameHeaderName:              "28.08.14",
	}))

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(s.T(), ok)

	require.Equal(s.T(), "22.08.78", md.Get(ClientVersionHeaderName)[0])
	require.Equal(s.T(), "<21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	require.Equal(s.T(), "28.08.14", md.Get(ClientNameHeaderName)[0])
	require.Equal(s.T(), "my-feature", md.Get(SupportedFeaturesHeaderName)[0])
}

func (s *HeadersSuite) TestPropagate_EmptyIncomingContext() {
	ctx := context.Background()

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:           "22.08.78",
		SupportedServerVersionsHeaderName: "<21.04.16",
		ClientNameHeaderName:              "28.08.14",
	}))

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(s.T(), ok)

	require.Equal(s.T(), "22.08.78", md.Get(ClientVersionHeaderName)[0])
	require.Equal(s.T(), "<21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	require.Equal(s.T(), "28.08.14", md.Get(ClientNameHeaderName)[0])
}
