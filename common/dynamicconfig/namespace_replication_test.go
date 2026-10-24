package dynamicconfig_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

func TestNamespaceReplicationTransportMode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value any
		valid bool
		want  string
	}{
		{name: "legacy", value: "legacy", valid: true, want: "legacy"},
		{name: "shadow", value: "shadow", valid: true, want: "shadow"},
		{name: "unknown", value: "unknown", want: "legacy"},
		{name: "authoritative not enabled", value: "chasm", want: "legacy"},
		{name: "non-string", value: 1, want: "legacy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			setting := dynamicconfig.NamespaceReplicationTransportMode
			err := setting.Validate(tc.value)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			collection := dynamicconfig.NewCollection(dynamicconfig.StaticClient{setting.Key(): tc.value}, log.NewNoopLogger())
			require.Equal(t, tc.want, setting.Get(collection)())
		})
	}
}
