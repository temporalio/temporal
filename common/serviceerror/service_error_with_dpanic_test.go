package serviceerror

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
)

type serviceErrorWithDPanicSuite struct {
	suite.Suite
}

func TestServiceErrorWithDPanicSuite(t *testing.T) {
	suite.Run(t, new(serviceErrorWithDPanicSuite))
}



func (s *serviceErrorWithDPanicSuite) TestNewDPanicInProd() {
	cfg := log.Config{
		Level: "info",
	}

	logger := log.NewZapLogger(log.BuildZapLogger(cfg))
	require.NotNil(s.T(), logger)

	err := NewInternalErrorWithDPanic(logger, "Must not panic!")
	require.NotNil(s.T(), err)
	_, ok := err.(*serviceerror.Internal)
	require.True(s.T(), ok)
}

func (s *serviceErrorWithDPanicSuite) TestNewDPanicInDev() {
	cfg := log.Config{
		Level:       "info",
		Development: true,
	}

	logger := log.NewZapLogger(log.BuildZapLogger(cfg))
	require.NotNil(s.T(), logger)

	require.Panics(s.T(), nil, func() {
		err := NewInternalErrorWithDPanic(logger, "Must panic!")
		require.Nil(s.T(), err)
	})
}
