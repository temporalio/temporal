package query

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConverterError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name   string
		format string
		args   []any
		want   string
	}{
		{
			name:   "no args",
			format: "test error",
			want:   "test error",
		},
		{
			name:   "multiple args",
			format: "test error: %d %s",
			args:   []any{123, "foo"},
			want:   "test error: 123 foo",
		},
		{
			name:   "wrap error",
			format: "test error: %v",
			args:   []any{errors.New("inside error")},
			want:   "test error: inside error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewConverterError(tc.format, tc.args...)
			require.Error(t, err)
			require.Equal(t, tc.want, err.Error())

			var converterErr *ConverterError
			require.ErrorAs(t, err, &converterErr)
			invalidArgErr := converterErr.ToInvalidArgument()
			require.Error(t, invalidArgErr)
			require.Equal(t, fmt.Sprintf("invalid query: %s", tc.want), invalidArgErr.Error())
		})
	}
}
