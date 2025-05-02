package circuitbreaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/dynamicconfig"
)

type TSCBWithDynamicSettingsTestSuite struct {
	suite.Suite
}

func TestTSCBWithDynamicSettings(t *testing.T) {
	suite.Run(t, &TSCBWithDynamicSettingsTestSuite{})
}

func TestBasic(t *testing.T) {
	s := assert.New(t)

	name := "test-tscb"
	tscb := NewTwoStepCircuitBreakerWithDynamicSettings(Settings{Name: name})
	tscb.UpdateSettings(dynamicconfig.CircuitBreakerSettings{})
	s.Equal(name, tscb.Name())

	doneFn, err := tscb.Allow()
	s.NoError(err)
	doneFn(true)
}

func TestDynamicSettings(t *testing.T) {
	s := assert.New(t)

	tscb := NewTwoStepCircuitBreakerWithDynamicSettings(Settings{})
	tscb.UpdateSettings(dynamicconfig.CircuitBreakerSettings{})
	cb1 := tscb.cb.Load()

	// should not change
	tscb.UpdateSettings(dynamicconfig.CircuitBreakerSettings{})
	cb2 := tscb.cb.Load()
	s.Equal(cb2, cb1)

	// should change
	tscb.UpdateSettings(dynamicconfig.CircuitBreakerSettings{
		MaxRequests: 2,
		Interval:    3600 * time.Second,
		Timeout:     30 * time.Second,
	})
	cb3 := tscb.cb.Load()
	s.NotEqual(cb3, cb2)
}
