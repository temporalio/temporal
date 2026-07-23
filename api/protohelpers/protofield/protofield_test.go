package protofield_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/protohelpers/protofield"
)

// bogusField builds a Field handle for a proto field name that does not exist,
// used to exercise Verify's detection of stale handles.
func bogusField(name string) protofield.Field[*commonpb.Memo] {
	return protofield.NewScalar(
		name, "Bogus", protofield.KindScalar,
		func(*commonpb.Memo) bool { return false },
		func(*commonpb.Memo) string { return "" },
		func(*commonpb.Memo, string) {},
	)
}

func realMemoFields() []protofield.Field[*commonpb.Memo] {
	return []protofield.Field[*commonpb.Memo]{
		protofield.NewMap(
			"fields", "Fields",
			func(m *commonpb.Memo) bool { return len(m.Fields) > 0 },
			func(m *commonpb.Memo) map[string]*commonpb.Payload { return m.Fields },
			func(m *commonpb.Memo, v map[string]*commonpb.Payload) { m.Fields = v },
		),
	}
}

func TestVerify(t *testing.T) {
	memo := &commonpb.Memo{}

	t.Run("exhaustive", func(t *testing.T) {
		require.NoError(t, protofield.Verify(memo, realMemoFields()))
	})

	t.Run("missing handle", func(t *testing.T) {
		err := protofield.Verify(memo, nil)
		require.ErrorContains(t, err, "missing handles for: fields")
	})

	t.Run("unknown field", func(t *testing.T) {
		fields := append(realMemoFields(), bogusField("not_a_real_field"))
		err := protofield.Verify(memo, fields)
		require.ErrorContains(t, err, "handles for unknown fields: not_a_real_field")
	})
}
