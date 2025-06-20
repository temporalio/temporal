package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
)

type (
	contextSuite struct {
		*require.Assertions
		suite.Suite
	}
)

func TestContextSuite(t *testing.T) {
	suite.Run(t, &contextSuite{})
}

func (s *contextSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *contextSuite) TestCopyContextValues_ValueCopied() {
	key := struct{}{}
	value := "value"

	metadataKey := "header-key"
	metadataValue := "header-value"

	ctx := context.Background()
	ctx = context.WithValue(ctx, key, value)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(metadataKey, metadataValue))

	newDeadline := time.Now().Add(time.Hour)
	newContext, cancel := context.WithDeadline(context.Background(), newDeadline)
	defer cancel()

	newContext = CopyContextValues(newContext, ctx)

	s.Equal(value, newContext.Value(key))
	md, ok := metadata.FromIncomingContext(newContext)
	s.True(ok)
	s.Equal(metadataValue, md[metadataKey][0])
}

func (s *contextSuite) TestCopyContextValue_DeadlineSeparated() {
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	newDeadline := time.Now().Add(time.Hour)
	newContext, newCancel := context.WithDeadline(context.Background(), newDeadline)
	defer newCancel()

	newContext = CopyContextValues(newContext, ctx)

	cancel()
	s.NotNil(ctx.Err())
	s.Nil(newContext.Err())
}

func (s *contextSuite) TestCopyContextValue_ValueNotOverWritten() {
	key := struct{}{}
	value := "value"
	ctx := context.WithValue(context.Background(), key, value)

	newValue := "newValue"
	newContext := context.WithValue(context.Background(), key, newValue)

	newContext = CopyContextValues(newContext, ctx)

	s.Equal(newValue, newContext.Value(key))
}
