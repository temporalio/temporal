// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	s.True(ok)

	s.Equal("22.08.78", md.Get(ClientVersionHeaderName)[0])
	s.Equal(">21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	s.Equal("28.08.14", md.Get(ClientNameHeaderName)[0])
	s.Equal("my-feature", md.Get(SupportedFeaturesHeaderName)[0])
}

func (s *HeadersSuite) TestPropagate_CreateNewOutgoingContext_SomeMissing() {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
		ClientVersionHeaderName: "22.08.78",
		ClientNameHeaderName:    "28.08.14",
	}))

	ctx = Propagate(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	s.True(ok)

	s.Equal("22.08.78", md.Get(ClientVersionHeaderName)[0])
	s.Equal(0, len(md.Get(SupportedServerVersionsHeaderName)))
	s.Equal("28.08.14", md.Get(ClientNameHeaderName)[0])
	s.Equal(0, len(md.Get(SupportedFeaturesHeaderName)))
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
	s.True(ok)

	s.Equal("22.08.78", md.Get(ClientVersionHeaderName)[0])
	s.Equal("<21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	s.Equal("28.08.14", md.Get(ClientNameHeaderName)[0])
	s.Equal("my-feature", md.Get(SupportedFeaturesHeaderName)[0])
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
	s.True(ok)

	s.Equal("22.08.78", md.Get(ClientVersionHeaderName)[0])
	s.Equal("<21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	s.Equal("28.08.14", md.Get(ClientNameHeaderName)[0])
	s.Equal("my-feature", md.Get(SupportedFeaturesHeaderName)[0])
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
	s.True(ok)

	s.Equal("22.08.78", md.Get(ClientVersionHeaderName)[0])
	s.Equal("<21.04.16", md.Get(SupportedServerVersionsHeaderName)[0])
	s.Equal("28.08.14", md.Get(ClientNameHeaderName)[0])
}
