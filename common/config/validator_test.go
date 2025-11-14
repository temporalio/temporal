package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateVisibilityConfig(t *testing.T) {
	testCases := []struct {
		name string
		in   Visibility
		err  bool
	}{
		{
			name: "success",
			in: Visibility{
				PersistenceCustomSearchAttributes: map[string]int{
					"Bool":    5,
					"Keyword": 2,
				},
			},
			err: false,
		},
		{
			name: "invalid search attribute type",
			in: Visibility{
				PersistenceCustomSearchAttributes: map[string]int{
					"Bool": 5,
					"Foo":  2,
				},
			},
			err: true,
		},
		{
			name: "invalid unspecified",
			in: Visibility{
				PersistenceCustomSearchAttributes: map[string]int{
					"Bool":        5,
					"Unspecified": 2,
				},
			},
			err: true,
		},
		{
			name: "invalid negative number",
			in: Visibility{
				PersistenceCustomSearchAttributes: map[string]int{
					"Bool":    5,
					"Keyword": -2,
				},
			},
			err: true,
		},
		{
			name: "invalid large number",
			in: Visibility{
				PersistenceCustomSearchAttributes: map[string]int{
					"Bool":    5,
					"Keyword": 100,
				},
			},
			err: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validate := newValidator()
			err := validate.Validate(tc.in)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
