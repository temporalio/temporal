package testcore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
)

func TestInjectPersistenceFault_SetsOneInjector(t *testing.T) {
	t.Parallel()

	errOne := errors.New("one")
	errTwo := errors.New("two")
	first := InjectPersistenceFault(t, func(config.FaultInjectionTarget) error {
		return errOne
	}, WithMethod("MethodOne"))
	second := InjectPersistenceFault(t, func(config.FaultInjectionTarget) error {
		return errTwo
	}, WithMethod("MethodTwo"))

	var options testOptions
	first(&options)
	second(&options)
	require.Len(t, options.clusterOptions, 1)
	require.True(t, options.dedicatedCluster)

	var params testClusterParams
	options.clusterOptions[0](&params)
	require.NotNil(t, params.FaultInjectionConfig)
	injector := params.FaultInjectionConfig.Injector
	require.ErrorIs(t, injector(config.FaultInjectionTarget{Method: "MethodOne"}), errOne)
	require.ErrorIs(t, injector(config.FaultInjectionTarget{Method: "MethodTwo"}), errTwo)
	require.NoError(t, injector(config.FaultInjectionTarget{Method: "MethodThree"}))
}
