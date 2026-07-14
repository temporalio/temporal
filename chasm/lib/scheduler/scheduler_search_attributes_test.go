package scheduler

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func TestUserCustomSearchAttributes(t *testing.T) {
	userPayload := &commonpb.Payload{Data: []byte("user-value")}
	tests := []struct {
		name   string
		fields map[string]*commonpb.Payload
		want   map[string]*commonpb.Payload
	}{
		{
			name: "nil",
		},
		{
			name:   "empty",
			fields: map[string]*commonpb.Payload{},
			want:   map[string]*commonpb.Payload{},
		},
		{
			name: "reserved only",
			fields: map[string]*commonpb.Payload{
				sadefs.TemporalSchedulePaused:    {},
				sadefs.TemporalNamespaceDivision: {},
			},
			want: map[string]*commonpb.Payload{},
		},
		{
			name: "mixed",
			fields: map[string]*commonpb.Payload{
				"CustomKeywordField":             userPayload,
				sadefs.TemporalSchedulePaused:    {},
				sadefs.TemporalNamespaceDivision: {},
			},
			want: map[string]*commonpb.Payload{
				"CustomKeywordField": userPayload,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := maps.Clone(tt.fields)

			result := userCustomSearchAttributes(tt.fields)

			require.Equal(t, tt.want, result)
			require.Equal(t, original, tt.fields)
		})
	}
}
