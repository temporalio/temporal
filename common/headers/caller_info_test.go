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
		*require.Assertions
		suite.Suite
	}
)

func TestCallerInfoSuite(t *testing.T) {
	suite.Run(t, &callerInfoSuite{})
}

func (s *callerInfoSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *callerInfoSuite) TestSetCallerName() {
	ctx := context.Background()
	info := GetCallerInfo(ctx)
	s.Empty(info.CallerName)

	ctx = SetCallerName(ctx, CallerNameSystem)
	info = GetCallerInfo(ctx)
	s.Equal(CallerNameSystem, info.CallerName)

	ctx = SetCallerName(ctx, "")
	info = GetCallerInfo(ctx)
	s.Equal(CallerNameSystem, info.CallerName)

	newCallerName := "new caller name"
	ctx = SetCallerName(ctx, newCallerName)
	info = GetCallerInfo(ctx)
	s.Equal(newCallerName, info.CallerName)
}

func (s *callerInfoSuite) TestSetCallerType() {
	ctx := context.Background()
	info := GetCallerInfo(ctx)
	s.Empty(info.CallerType)

	ctx = SetCallerType(ctx, CallerTypeBackgroundHigh)
	info = GetCallerInfo(ctx)
	s.Equal(CallerTypeBackgroundHigh, info.CallerType)

	ctx = SetCallerName(ctx, "")
	info = GetCallerInfo(ctx)
	s.Equal(CallerTypeBackgroundHigh, info.CallerType)

	ctx = SetCallerType(ctx, CallerTypeAPI)
	info = GetCallerInfo(ctx)
	s.Equal(CallerTypeAPI, info.CallerType)

	ctx = SetCallerType(ctx, CallerTypePreemptable)
	info = GetCallerInfo(ctx)
	s.Equal(CallerTypePreemptable, info.CallerType)
}

func (s *callerInfoSuite) TestSetCallOrigin() {
	ctx := context.Background()
	info := GetCallerInfo(ctx)
	s.Empty(info.CallOrigin)

	initiation := "method name"
	ctx = SetOrigin(ctx, initiation)
	info = GetCallerInfo(ctx)
	s.Equal(initiation, info.CallOrigin)

	ctx = SetOrigin(ctx, "")
	info = GetCallerInfo(ctx)
	s.Equal(initiation, info.CallOrigin)

	newCallOrigin := "another method name"
	ctx = SetOrigin(ctx, newCallOrigin)
	info = GetCallerInfo(ctx)
	s.Equal(newCallOrigin, info.CallOrigin)
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
	s.True(ok)
	s.Equal(existingValue, md.Get(existingKey)[0])
	s.Equal(callerName, md.Get(CallerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Equal(callOrigin, md.Get(CallOriginHeaderName)[0])
	s.Len(md, 4)
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
	s.True(ok)
	s.Equal(callerName, md.Get(CallerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Equal(callOrigin, md.Get(CallOriginHeaderName)[0])
	s.Len(md, 3)
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
	s.True(ok)
	s.Equal(callerName, md.Get(CallerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Equal(callOrigin, md.Get(CallOriginHeaderName)[0])
	s.Len(md, 3)
}

func (s *callerInfoSuite) TestSetCallerInfo_WithPartialCallerInfo() {
	callerName := CallerNameSystem
	callerType := CallerTypeAPI

	ctx := SetCallerType(context.Background(), callerType)

	ctx = SetCallerInfo(ctx, CallerInfo{
		CallerName: callerName,
	})

	md, ok := metadata.FromIncomingContext(ctx)
	s.True(ok)
	s.Equal(callerName, md.Get(CallerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Empty(md.Get(CallOriginHeaderName))
	s.Len(md, 2)
}
