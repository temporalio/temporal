package callback

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
)

func TestValidateInternalCallbackRef(t *testing.T) {
	const (
		sourceNamespaceID = namespace.ID("source-ns")
		exemptArchetype   = "test.crossNamespaceExempt"
	)
	marshal := func(ref *persistencespb.ChasmComponentRef) []byte {
		b, err := ref.Marshal()
		require.NoError(t, err)
		return b
	}

	cases := []struct {
		name      string
		ref       []byte
		crossNS   []string
		wantErrIs error
	}{
		{
			name: "same-namespace",
			ref: marshal(&persistencespb.ChasmComponentRef{
				NamespaceId: sourceNamespaceID.String(), BusinessId: "sched", ArchetypeId: chasm.SchedulerArchetypeID,
			}),
		},
		{
			name: "cross-namespace-rejected-by-default",
			ref: marshal(&persistencespb.ChasmComponentRef{
				NamespaceId: "victim-ns", BusinessId: "sched", ArchetypeId: chasm.SchedulerArchetypeID,
			}),
			wantErrIs: ErrInternalCallbackNamespaceMismatch,
		},
		{
			name: "cross-namespace-archetype-not-in-list",
			ref: marshal(&persistencespb.ChasmComponentRef{
				NamespaceId: "victim-ns", BusinessId: "sched", ArchetypeId: chasm.SchedulerArchetypeID,
			}),
			crossNS:   []string{exemptArchetype},
			wantErrIs: ErrInternalCallbackNamespaceMismatch,
		},
		{
			name: "cross-namespace-exempt-archetype",
			ref: marshal(&persistencespb.ChasmComponentRef{
				NamespaceId: "other-ns", BusinessId: "id", ArchetypeId: chasm.GenerateTypeID(exemptArchetype),
			}),
			crossNS: []string{exemptArchetype},
		},
		{
			name: "missing-archetype-cross-namespace",
			ref: marshal(&persistencespb.ChasmComponentRef{
				NamespaceId: "victim-ns", BusinessId: "sched",
			}),
			crossNS:   []string{exemptArchetype},
			wantErrIs: ErrInternalCallbackNamespaceMismatch,
		},
		{
			name:      "missing-namespace",
			ref:       marshal(&persistencespb.ChasmComponentRef{BusinessId: "sched"}),
			wantErrIs: ErrInvalidInternalCallbackRef,
		},
		{
			name:      "missing-business-id",
			ref:       marshal(&persistencespb.ChasmComponentRef{NamespaceId: sourceNamespaceID.String()}),
			wantErrIs: ErrInvalidInternalCallbackRef,
		},
		{
			name:      "empty",
			ref:       nil,
			wantErrIs: ErrInvalidInternalCallbackRef,
		},
		{
			name:      "garbage",
			ref:       []byte{0xff, 0xff, 0xff},
			wantErrIs: ErrInvalidInternalCallbackRef,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateInternalCallbackRef(tc.ref, sourceNamespaceID, tc.crossNS)
			if tc.wantErrIs == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.wantErrIs)
		})
	}
}
