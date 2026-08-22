package callbacks

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func TestKindOf(t *testing.T) {
	for _, tc := range []struct {
		callback *commonpb.Callback
		want     Kind
		wantName string
	}{
		{
			callback: newNexusCallback(),
			want:     KindNexus,
			wantName: "nexus",
		},
		{
			callback: newWorkerCallback(),
			want:     KindWorker,
			wantName: "worker",
		},
		{
			// Internal-variant callbacks should be removed; treated as Unknown.
			callback: &commonpb.Callback{
				Variant: &commonpb.Callback_Internal_{
					Internal: &commonpb.Callback_Internal{},
				},
			},
			want:     KindUnknown,
			wantName: "unknown",
		},
		{
			callback: &commonpb.Callback{},
			want:     KindUnknown,
			wantName: "unknown",
		},
		{
			callback: nil,
			want:     KindUnknown,
			wantName: "unknown",
		},
	} {
		t.Run(tc.wantName, func(t *testing.T) {
			require.Equal(t, tc.want, KindOf(tc.callback))
			require.Equal(t, tc.wantName, tc.want.String())
		})
	}
}

func TestConvertEnabledKinds(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  any
		want []Kind
	}{
		// Empty lists are supported. It means no callbacks are allowed on the execution.
		{name: "Empty", val: []string{}, want: []Kind{}},
		{name: "Nil", val: nil, want: []Kind{}},

		{name: "NexusOnly", val: []string{"nexus"}, want: []Kind{KindNexus}},
		{name: "WorkerOnly", val: []string{"worker"}, want: []Kind{KindWorker}},
		{
			name: "Both",
			val:  []string{"nexus", "worker"},
			want: []Kind{KindNexus, KindWorker},
		},
		{
			name: "OrderPreserved",
			val:  []string{"worker", "nexus"},
			want: []Kind{KindWorker, KindNexus},
		},
		{
			name: "DuplicatesDropped",
			val:  []string{"nexus", "nexus"},
			want: []Kind{KindNexus},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertEnabledKinds(tc.val)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Return an error for any unrecognized name, rejecting the whole value. So dynamic config
	// parsing will log and fall back to the setting's default value.
	for _, tc := range []struct {
		name   string
		val    any
		errMsg string
	}{
		{name: "NotAList", val: 42, errMsg: "source data must be an array or slice"},
		{
			name:   "OnlyUnknownNames",
			val:    []string{"carrier-pigeon"},
			errMsg: `[carrier-pigeon] does not match a known callback kind`,
		},
		{
			name:   "UnknownNameBesideKnownName",
			val:    []string{"nexsus", "worker"},
			errMsg: `[nexsus] does not match a known callback kind`,
		},
		{
			// Internal callbacks are server-generated and cannot be enabled by an operator.
			name:   "Internal",
			val:    []string{"nexus", "internal"},
			errMsg: `[internal] does not match a known callback kind`,
		},
		{
			name:   "NotNormalizedOrTrimmed",
			val:    []any{" Nexus", "WORKER", "   nexus", "worker "},
			errMsg: "[ Nexus WORKER    nexus worker ] does not match a known callback kind",
		},
		{
			name:   "AllUnknownNamesReported",
			val:    []string{"nexsus", "wroker"},
			errMsg: `[nexsus wroker] does not match a known callback kind`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertEnabledKinds(tc.val)
			require.ErrorContains(t, err, tc.errMsg)
			require.Nil(t, got)
		})
	}
}
