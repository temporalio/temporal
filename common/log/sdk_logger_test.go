package log

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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
	s.underlyingLogger.EXPECT().Info("msg", tag.Any("key1", "val1"), tag.Any("key2", "val2"))
	s.sdkLogger.Info("msg", "key1", "val1", "key2", "val2")
}

func (s *SdkLoggerSuite) TestOddKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg", tag.Any("key1", "val1"), tag.Any("key2", "no value"))
	s.sdkLogger.Info("msg", "key1", "val1", "key2")
}

func (s *SdkLoggerSuite) TestKeyValPairsWithTag() {
	s.underlyingLogger.EXPECT().Info("msg", tag.Any("key1", "val1"), tag.String("key3", "val3"), tag.Any("key2", "val2"))
	s.sdkLogger.Info("msg", "key1", "val1", tag.String("key3", "val3"), "key2", "val2")

	s.underlyingLogger.EXPECT().Info("msg", tag.Any("key1", "val1"), tag.Int("key3", 3), tag.Any("key2", "val2"))
	s.sdkLogger.Info("msg", "key1", "val1", tag.Int("key3", 3), "key2", "val2")

}

func (s *SdkLoggerSuite) TestEmptyKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg")
	s.sdkLogger.Info("msg")
}

func (s *SdkLoggerSuite) TestSingleKeyValPairs() {
	s.underlyingLogger.EXPECT().Info("msg", tag.Any("key1", "no value"))
	s.sdkLogger.Info("msg", "key1")
}

// loggingCallAt extracts the "logging-call-at" field (source attribution) from a recorded entry.
func loggingCallAt(entry observer.LoggedEntry) string {
	for _, f := range entry.Context {
		if f.Key == tag.LoggingCallAtKey {
			return f.String
		}
	}
	return "<none>"
}

// TestSdkLoggerWithPreservesCallSite guards against re-introducing the skip-accounting bug where
// SdkLogger.With() re-applied the SDK skip on every call, walking logging-call-at one stack frame
// further from the real call site with each (compounding) With(). With the fix, every line — direct
// or after any number of With() calls — must attribute to this test file (the true caller).
func TestSdkLoggerWithPreservesCallSite(t *testing.T) {
	core, recorded := observer.New(zap.DebugLevel)
	sdk := NewSdkLogger(NewZapLogger(zap.New(core)))

	sdk.Info("direct")
	w1 := sdk.With("k1", "v1").(*SdkLogger)
	w1.Info("after one With")
	w2 := w1.With("k2", "v2").(*SdkLogger)
	w2.Info("after two Withs")

	entries := recorded.All()
	require.Len(t, entries, 3)
	for _, e := range entries {
		callAt := loggingCallAt(e)
		require.Truef(t, strings.Contains(callAt, "sdk_logger_test.go"),
			"logging-call-at for %q should point to this test file (the real caller), got %q", e.Message, callAt)
	}

	// Tags accumulated through With() must also survive. Without this, a change that
	// restored call-site attribution by dropping tags would still pass — which is the
	// adjacent failure mode that Skip() exhibited by omitting its tags field.
	require.NotContains(t, entries[0].ContextMap(), "k1", "direct log should carry no With() tags")
	require.Equal(t, "v1", entries[1].ContextMap()["k1"])
	require.Equal(t, "v1", entries[2].ContextMap()["k1"], "tag from the first With() must survive the second")
	require.Equal(t, "v2", entries[2].ContextMap()["k2"])
}
