package headers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
)

type (
	callerInfoSuite struct {
		suite.Suite
	}
)

func TestCallerInfoSuite(t *testing.T) {
	suite.Run(t, &callerInfoSuite{})
}

func (s *callerInfoSuite) TestSetCallerName() {
	ctx := context.Background()
	info := GetCallerInfo(ctx)
	require.Empty(s.T(), info.CallerName)

	ctx = SetCallerName(ctx, CallerNameSystem)
	info = GetCallerInfo(ctx)
	require.Equal(s.T(), CallerNameSystem, info.CallerName)

	ctx = SetCallerName(ctx, "")
	info = GetCallerInfo(ctx)
	require.Equal(s.T(), CallerNameSystem, info.CallerName)

	newCallerName := "new caller name"
	ctx = SetCallerName(ctx, newCallerName)
	info = GetCallerInfo(ctx)
	require.Equal(s.T(), newCallerName, info.CallerName)
}

func (s *callerInfoSuite) TestSetCallerType() {
	ctx := context.Background()
	info := GetCallerInfo(ctx)
	require.Empty(s.T(), info.CallerType)

	ctx = SetCallerType(ctx, CallerTypeBackgroundHigh)
	info = GetCallerInfo(ctx)
	require.Equal(s.T(), CallerTypeBackgroundHigh, info.CallerType)

	ctx = SetCallerName(ctx, "")
	info = GetCallerInfo(ctx)
	require.Equal(s.T(), CallerTypeBackgroundHigh, info.CallerType)

	ctx = SetCallerType(ctx, CallerTypeAPI)
	info = GetCallerInfo(ctx)
	require.Equal(s.T(), CallerTypeAPI, info.CallerType)

	ctx = SetCallerType(ctx, CallerTypePreemptable)
	info = GetCallerInfo(ctx)
	require.Equal(s.T(), CallerTypePreemptable, info.CallerType)
}

func (s *callerInfoSuite) TestSetCallOrigin() {
	ctx := context.Background()
	info := GetCallerInfo(ctx)
	require.Empty(s.T(), info.CallOrigin)

	initiation := "method name"
	ctx = SetOrigin(ctx, initiation)
	info = GetCallerInfo(ctx)
	require.Equal(s.T(), initiation, info.CallOrigin)

	ctx = SetOrigin(ctx, "")
	info = GetCallerInfo(ctx)
	require.Equal(s.T(), initiation, info.CallOrigin)

	newCallOrigin := "another method name"
	ctx = SetOrigin(ctx, newCallOrigin)
	info = GetCallerInfo(ctx)
	require.Equal(s.T(), newCallOrigin, info.CallOrigin)
}

func (s *callerInfoSuite) TestSetCallerInfo_PreserveOtherValues() {
	existingKey := "key"
	existingValue := "value"
	callerName := "callerName"
	callerType := CallerTypeAPI
	callOrigin := "methodName"

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(existingKey, existingValue),
	)

	ctx = SetCallerInfo(ctx, NewCallerInfo(callerName, callerType, callOrigin))

	md, ok := metadata.FromIncomingContext(ctx)
	require.True(s.T(), ok)
	require.Equal(s.T(), existingValue, md.Get(existingKey)[0])
	require.Equal(s.T(), callerName, md.Get(CallerNameHeaderName)[0])
	require.Equal(s.T(), callerType, md.Get(CallerTypeHeaderName)[0])
	require.Equal(s.T(), callOrigin, md.Get(CallOriginHeaderName)[0])
	require.Len(s.T(), md, 4)
}

func (s *callerInfoSuite) TestSetCallerInfo_NoExistingCallerInfo() {
	callerName := CallerNameSystem
	callerType := CallerTypeAPI
	callOrigin := "methodName"

	ctx := SetCallerInfo(context.Background(), CallerInfo{
		CallerName: callerName,
		CallerType: callerType,
		CallOrigin: callOrigin,
	})

	md, ok := metadata.FromIncomingContext(ctx)
	require.True(s.T(), ok)
	require.Equal(s.T(), callerName, md.Get(CallerNameHeaderName)[0])
	require.Equal(s.T(), callerType, md.Get(CallerTypeHeaderName)[0])
	require.Equal(s.T(), callOrigin, md.Get(CallOriginHeaderName)[0])
	require.Len(s.T(), md, 3)
}

func (s *callerInfoSuite) TestSetCallerInfo_WithExistingCallerInfo() {
	callerName := CallerNameSystem
	callerType := CallerTypeBackgroundHigh
	callOrigin := "methodName"

	ctx := SetCallerName(context.Background(), callerName)
	ctx = SetCallerType(ctx, CallerTypeAPI)
	ctx = SetOrigin(ctx, callOrigin)

	ctx = SetCallerInfo(ctx, CallerInfo{
		CallerName: "",
		CallerType: callerType,
		CallOrigin: "",
	})

	md, ok := metadata.FromIncomingContext(ctx)
	require.True(s.T(), ok)
	require.Equal(s.T(), callerName, md.Get(CallerNameHeaderName)[0])
	require.Equal(s.T(), callerType, md.Get(CallerTypeHeaderName)[0])
	require.Equal(s.T(), callOrigin, md.Get(CallOriginHeaderName)[0])
	require.Len(s.T(), md, 3)
}

func (s *callerInfoSuite) TestSetCallerInfo_WithPartialCallerInfo() {
	callerName := CallerNameSystem
	callerType := CallerTypeAPI

	ctx := SetCallerType(context.Background(), callerType)

	ctx = SetCallerInfo(ctx, CallerInfo{
		CallerName: callerName,
	})

	md, ok := metadata.FromIncomingContext(ctx)
	require.True(s.T(), ok)
	require.Equal(s.T(), callerName, md.Get(CallerNameHeaderName)[0])
	require.Equal(s.T(), callerType, md.Get(CallerTypeHeaderName)[0])
	require.Empty(s.T(), md.Get(CallOriginHeaderName))
	require.Len(s.T(), md, 2)
}
