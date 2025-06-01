package log

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/mock/gomock"
)

type SdkLoggerSuite struct {
	*require.Assertions
	suite.Suite

	controller       *gomock.Controller
	sdkLogger        log.Logger
	underlyingLogger *MockLogger
}

func TestSdkLoggerSuite(t *testing.T) {
	suite.Run(t, &SdkLoggerSuite{})
}

func (s *SdkLoggerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.underlyingLogger = NewMockLogger(s.controller)
	s.sdkLogger = NewSdkLogger(s.underlyingLogger)
}

func (s *SdkLoggerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *SdkLoggerSuite) TestEvenKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg", tag.NewAnyTag("key1", "val1"), tag.NewAnyTag("key2", "val2"))
	s.sdkLogger.Info("msg", "key1", "val1", "key2", "val2")
}

func (s *SdkLoggerSuite) TestOddKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg", tag.NewAnyTag("key1", "val1"), tag.NewAnyTag("key2", "no value"))
	s.sdkLogger.Info("msg", "key1", "val1", "key2")
}

func (s *SdkLoggerSuite) TestKeyValPairsWithTag() {
	s.underlyingLogger.EXPECT().Info("msg", tag.NewAnyTag("key1", "val1"), tag.NewStringTag("key3", "val3"), tag.NewAnyTag("key2", "val2"))
	s.sdkLogger.Info("msg", "key1", "val1", tag.NewStringTag("key3", "val3"), "key2", "val2")

	s.underlyingLogger.EXPECT().Info("msg", tag.NewAnyTag("key1", "val1"), tag.NewInt("key3", 3), tag.NewAnyTag("key2", "val2"))
	s.sdkLogger.Info("msg", "key1", "val1", tag.NewInt("key3", 3), "key2", "val2")

}

func (s *SdkLoggerSuite) TestEmptyKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg")
	s.sdkLogger.Info("msg")
}

func (s *SdkLoggerSuite) TestSingleKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg", tag.NewAnyTag("key1", "no value"))
	s.sdkLogger.Info("msg", "key1")
}
