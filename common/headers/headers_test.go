// Copyright (c) 2020 Temporal Technologies, Inc.

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
		*require.Assertions
		suite.Suite
	}
)

func TestHeadersSuite(t *testing.T) {
	suite.Run(t, &HeadersSuite{})
}

func (s *HeadersSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *HeadersSuite) TestPropagateHeaders_CreateNewOutgoingContext() {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:        "22.08.78",
		ClientFeatureVersionHeaderName: "21.04.16",
		ClientImplHeaderName:           "28.08.14",
	}))

	ctx = PropagateVersions(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	s.True(ok)

	s.Equal("22.08.78", md.Get(ClientVersionHeaderName)[0])
	s.Equal("21.04.16", md.Get(ClientFeatureVersionHeaderName)[0])
	s.Equal("28.08.14", md.Get(ClientImplHeaderName)[0])
}

func (s *HeadersSuite) TestPropagateHeaders_UpdateExistingEmptyOutgoingContext() {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:        "22.08.78",
		ClientFeatureVersionHeaderName: "21.04.16",
		ClientImplHeaderName:           "28.08.14",
	}))

	ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})

	ctx = PropagateVersions(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	s.True(ok)

	s.Equal("22.08.78", md.Get(ClientVersionHeaderName)[0])
	s.Equal("21.04.16", md.Get(ClientFeatureVersionHeaderName)[0])
	s.Equal("28.08.14", md.Get(ClientImplHeaderName)[0])
}

func (s *HeadersSuite) TestPropagateHeaders_UpdateExistingNonEmptyOutgoingContext() {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:        "07.08.78", // Must be ignored
		ClientFeatureVersionHeaderName: "07.04.16", // Must be ignored
	}))

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:        "22.08.78",
		ClientFeatureVersionHeaderName: "21.04.16",
		ClientImplHeaderName:           "28.08.14",
	}))

	ctx = PropagateVersions(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	s.True(ok)

	s.Equal("22.08.78", md.Get(ClientVersionHeaderName)[0])
	s.Equal("21.04.16", md.Get(ClientFeatureVersionHeaderName)[0])
	s.Equal("28.08.14", md.Get(ClientImplHeaderName)[0])
}

func (s *HeadersSuite) TestPropagateHeaders_EmptyIncomingContext() {
	ctx := context.Background()

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName:        "22.08.78",
		ClientFeatureVersionHeaderName: "21.04.16",
		ClientImplHeaderName:           "28.08.14",
	}))

	ctx = PropagateVersions(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	s.True(ok)

	s.Equal("22.08.78", md.Get(ClientVersionHeaderName)[0])
	s.Equal("21.04.16", md.Get(ClientFeatureVersionHeaderName)[0])
	s.Equal("28.08.14", md.Get(ClientImplHeaderName)[0])
}
