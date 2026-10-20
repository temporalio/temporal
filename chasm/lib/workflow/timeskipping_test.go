package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestValidateAndPopulateWorkflowTimeSkippingConfig(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		namespace string
		config    *commonpb.TimeSkippingConfig
		wantError bool
	}{
		{name: "nil config in disabled namespace", namespace: "ns2"},
		{name: "disabled config in disabled namespace", namespace: "ns2", config: &commonpb.TimeSkippingConfig{}, wantError: true},
		{name: "enabled config in disabled namespace", namespace: "ns2", config: &commonpb.TimeSkippingConfig{Enabled: true}, wantError: true},
		{name: "disabled config in enabled namespace", namespace: "ns1", config: &commonpb.TimeSkippingConfig{}},
		{name: "enabled config in enabled namespace", namespace: "ns1", config: &commonpb.TimeSkippingConfig{Enabled: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateAndPopulateWorkflowTimeSkippingConfig(
				tc.config,
				tc.namespace,
				func(namespace string) bool { return namespace == "ns1" },
				dynamicconfig.GetIntPropertyFnFilteredByNamespace(200),
				dynamicconfig.GetDurationPropertyFn(90*time.Minute),
			)
			if tc.wantError {
				require.ErrorIs(t, err, ErrWorkflowTimeSkippingNotEnabled)
				var permissionDenied *serviceerror.PermissionDenied
				require.ErrorAs(t, err, &permissionDenied)
			} else {
				require.NoError(t, err)
				if tc.config != nil {
					require.Equal(t, int32(200), tc.config.GetMaxSessionSkipCount())
				}
			}
		})
	}
}

func TestValidateAndPopulateTimeSkippingConfig(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		config       *commonpb.TimeSkippingConfig
		wantError    string
		wantMaxSkips int32
	}{
		{name: "nil config"},
		{name: "disabled config", config: &commonpb.TimeSkippingConfig{}, wantMaxSkips: 200},
		{name: "enabled config", config: &commonpb.TimeSkippingConfig{Enabled: true}, wantMaxSkips: 200},
		{name: "explicit max skip count", config: &commonpb.TimeSkippingConfig{Enabled: true, MaxSessionSkipCount: 999}, wantMaxSkips: 999},
		{name: "fast forward while disabled", config: &commonpb.TimeSkippingConfig{FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(10 * time.Second), Id: "ff-id"}}, wantError: "cannot set fast_forward when enabled is false"},
		{name: "negative duration", config: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(-10 * time.Second), Id: "ff-id"}}, wantError: "fast_forward duration must be positive"},
		{name: "zero duration", config: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Id: "ff-id"}}, wantError: "fast_forward duration must be positive"},
		{name: "missing id", config: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(10 * time.Second)}}, wantError: ErrTimeSkippingFastForwardIDNotSet.Error()},
		{name: "blank id", config: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(10 * time.Second), Id: "  "}}, wantError: ErrTimeSkippingFastForwardIDNotSet.Error()},
		{name: "valid fast forward", config: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(10 * time.Second), Id: "ff-id"}}, wantMaxSkips: 200},
		{name: "below duration cap", config: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(90*time.Minute - time.Second), Id: "ff-id"}}, wantMaxSkips: 200},
		{name: "at duration cap", config: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(90 * time.Minute), Id: "ff-id"}}, wantError: "fast_forward duration exceeds max value (1.5 hours)"},
		{name: "above duration cap", config: &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(90*time.Minute + time.Second), Id: "ff-id"}}, wantError: "fast_forward duration exceeds max value (1.5 hours)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateAndPopulateTimeSkippingConfig(
				tc.config,
				"ns1",
				dynamicconfig.GetIntPropertyFnFilteredByNamespace(200),
				dynamicconfig.GetDurationPropertyFn(90*time.Minute),
			)
			if tc.wantError != "" {
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
				require.ErrorContains(t, err, "time-skipping config invalid:")
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
				if tc.config != nil {
					require.Equal(t, tc.wantMaxSkips, tc.config.GetMaxSessionSkipCount())
				}
			}
		})
	}
}

func TestValidateAndPopulateTimeSkippingConfigNamespaceDefault(t *testing.T) {
	t.Parallel()

	client := dynamicconfig.StaticClient{
		dynamicconfig.TimeSkippingMaxSkipPerSession.Key(): []dynamicconfig.ConstrainedValue{
			{Value: 42},
			{Constraints: dynamicconfig.Constraints{Namespace: "ns1"}, Value: 7},
		},
	}
	maxSkipPerSession := dynamicconfig.TimeSkippingMaxSkipPerSession.Get(dynamicconfig.NewCollection(client, log.NewNoopLogger()))
	for _, tc := range []struct {
		name      string
		namespace string
		want      int32
	}{
		{name: "namespace override", namespace: "ns1", want: 7},
		{name: "fallback", namespace: "ns2", want: 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config := &commonpb.TimeSkippingConfig{Enabled: true}
			require.NoError(t, ValidateAndPopulateTimeSkippingConfig(config, tc.namespace, maxSkipPerSession, dynamicconfig.GetDurationPropertyFn(time.Hour)))
			require.Equal(t, tc.want, config.GetMaxSessionSkipCount())
		})
	}
}
