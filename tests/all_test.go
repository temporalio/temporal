package tests

import (
	"flag"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/dynamicconfig"
	"testing"
	"time"
)

// TestAcquireShard_OwnershipLostErrorSuite tests what happens when acquire shard returns an ownership lost error.
func TestAcquireShard_OwnershipLostErrorSuite(t *testing.T) {
	s := new(OwnershipLostErrorSuite)
	suite.Run(t, s)
}

// TestAcquireShard_DeadlineExceededErrorSuite tests what happens when acquire shard returns a deadline exceeded error
func TestAcquireShard_DeadlineExceededErrorSuite(t *testing.T) {
	s := new(DeadlineExceededErrorSuite)
	suite.Run(t, s)
}

// TestAcquireShard_EventualSuccess verifies that we eventually succeed in acquiring the shard when we get a deadline
// exceeded error followed by a successful acquire shard call.
// To make this test deterministic, we set the seed to in the config file to a fixed value.
func TestAcquireShard_EventualSuccess(t *testing.T) {
	s := new(EventualSuccessSuite)
	suite.Run(t, s)
}

func TestAddTasksSuite(t *testing.T) {
	suite.Run(t, new(AddTasksSuite))
}

func TestAdvancedVisibilitySuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(AdvancedVisibilitySuite))
}

func TestArchivalSuite(t *testing.T) {
	flag.Parse()
	s := new(ArchivalSuite)
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		dynamicconfig.RetentionTimerJitterDuration:  time.Second,
		dynamicconfig.ArchivalProcessorArchiveDelay: time.Duration(0),
	}
	suite.Run(t, s)
}

func TestClientFunctionalSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(ClientFunctionalSuite))
}

func TestDLQSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(DLQSuite))
}

func TestFunctionalSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(FunctionalSuite))
}

func TestFunctionalTestBaseSuite(t *testing.T) {
	suite.Run(t, new(FunctionalTestBaseSuite))
}

func TestRawHistorySuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(RawHistorySuite))
}

func TestNamespaceSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, &namespaceTestSuite{})
}

func TestPurgeDLQTasksSuite(t *testing.T) {
	suite.Run(t, new(PurgeDLQTasksSuite))
}

func TestScheduleFunctionalSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(ScheduleFunctionalSuite))
}

func TestSizeLimitFunctionalSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(SizeLimitFunctionalSuite))
}

func TestTLSFunctionalSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(TLSFunctionalSuite))
}

func TestVersioningFunctionalSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(VersioningIntegSuite))
}
