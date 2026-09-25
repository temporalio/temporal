package testcore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	persistencefaults "go.temporal.io/server/common/persistence/faultinjection"
)

func TestInjectPersistenceFault_LazilyCreatesOneRegistry(t *testing.T) {
	t.Parallel()

	errOne := errors.New("one")
	errTwo := errors.New("two")
	first := InjectPersistenceFault(t, func(persistencefaults.Target) error {
		return errOne
	}, WithMethod("MethodOne"))
	second := InjectPersistenceFault(t, func(persistencefaults.Target) error {
		return errTwo
	}, WithMethod("MethodTwo"))

	var options testOptions
	require.Nil(t, options.persistenceFaultRegistry)

	first(&options)
	registry := options.persistenceFaultRegistry
	require.NotNil(t, registry)
	require.Len(t, options.clusterOptions, 1)

	second(&options)
	require.Same(t, registry, options.persistenceFaultRegistry)
	require.Len(t, options.clusterOptions, 1)
	require.True(t, options.dedicatedCluster)

	var params testClusterParams
	options.clusterOptions[0](&params)
	require.NotNil(t, params.FaultInjectionConfig)
	require.ErrorIs(t, params.FaultInjectionConfig.Injector(config.FaultInjectionTarget{Method: "MethodOne"}), errOne)
	require.ErrorIs(t, params.FaultInjectionConfig.Injector(config.FaultInjectionTarget{Method: "MethodTwo"}), errTwo)
}
