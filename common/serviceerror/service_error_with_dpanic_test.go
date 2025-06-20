package serviceerror

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
)

type serviceErrorWithDPanicSuite struct {
	*require.Assertions
	suite.Suite
}

func TestServiceErrorWithDPanicSuite(t *testing.T) {
	suite.Run(t, new(serviceErrorWithDPanicSuite))
}

func (s *serviceErrorWithDPanicSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *serviceErrorWithDPanicSuite) TestNewDPanicInProd() {
	cfg := log.Config{
		Level: "info",
	}

	logger := log.NewZapLogger(log.BuildZapLogger(cfg))
	s.NotNil(logger)

	err := NewInternalErrorWithDPanic(logger, "Must not panic!")
	s.NotNil(err)
	_, ok := err.(*serviceerror.Internal)
	s.True(ok)
}

func (s *serviceErrorWithDPanicSuite) TestNewDPanicInDev() {
	cfg := log.Config{
		Level:       "info",
		Development: true,
	}

	logger := log.NewZapLogger(log.BuildZapLogger(cfg))
	s.NotNil(logger)

	s.Panics(nil, func() {
		err := NewInternalErrorWithDPanic(logger, "Must panic!")
		s.Nil(err)
	})
}
