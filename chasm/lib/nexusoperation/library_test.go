package nexusoperation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.uber.org/fx"
)

func TestLibraryOptionalDestinationBlocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		provided bool
	}{
		{name: "without provider", provided: false},
		{name: "with provider", provided: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var library *Library
			opts := []fx.Option{
				fx.NopLogger,
				fx.Supply(
					&handler{},
					&operationBackoffTaskHandler{},
					&operationInvocationTaskHandler{},
					&operationScheduleToCloseTimeoutTaskHandler{},
					&operationScheduleToStartTimeoutTaskHandler{},
					&operationStartToCloseTimeoutTaskHandler{},
					&cancellationInvocationTaskHandler{},
					&cancellationBackoffTaskHandler{},
					dynamicconfig.NewNoopCollection(),
				),
				fx.Provide(newLibrary),
				fx.Populate(&library),
			}
			if tc.provided {
				opts = append(opts, fx.Supply(DestinationBlockedFn(func(namespaceID, destination string) bool {
					require.Equal(t, "ns-id", namespaceID)
					require.Equal(t, "test-endpoint", destination)
					return true
				})))
			}
			app := fx.New(opts...)
			require.NoError(t, app.Err())
			require.NotNil(t, library)
			if tc.provided {
				require.NotNil(t, library.destinationBlocked)
				require.True(t, library.destinationBlocked("ns-id", "test-endpoint"))
			} else {
				require.Nil(t, library.destinationBlocked)
			}
		})
	}
}
