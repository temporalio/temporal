package tdbg

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/tasks"
)

func TestGetCategory(t *testing.T) {
	cat101 := tasks.NewCategory(101, tasks.CategoryTypeImmediate, "CategoryName")

	registry := tasks.NewDefaultTaskCategoryRegistry()
	registry.AddCategory(cat101)

	testCases := []struct {
		name        string
		input       string
		expectedCat tasks.Category
		expectedErr string
	}{
		{
			name:        "same case",
			input:       "CategoryName",
			expectedCat: cat101,
		},
		{
			name:        "different case",
			input:       "cAtEgOrYnAmE",
			expectedCat: cat101,
		},
		{
			name:        "not found",
			input:       "random",
			expectedErr: "unknown task category \"random\"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := require.New(t)
			cat, err := getCategory(registry, tc.input)
			if tc.expectedErr == "" {
				s.NoError(err)
				s.Equal(tc.expectedCat, cat)
			} else {
				s.ErrorContains(err, tc.expectedErr)
			}
		})
	}
}
