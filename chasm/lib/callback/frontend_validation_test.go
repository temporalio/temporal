package callback

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequiredStringFields(t *testing.T) {
	// Positive tests
	positiveTests := requiredFields{
		{"Field1", "exists"},
		{"Field2", " "}, // Whitespace, but still non-empty.
	}
	require.NoError(t, positiveTests.Validate())
	for _, positiveTest := range positiveTests {
		require.NoError(t, positiveTest.Validate())
	}

	// Negative tests
	negativeTests := requiredFields{
		{"Field1", ""},
		{"Field2", ""},
	}
	require.ErrorContains(t, negativeTests.Validate(), "Field1 is required")
	for _, negativeTest := range negativeTests {
		wantErr := fmt.Sprintf("%s is required", negativeTest.FieldName)
		require.ErrorContains(t, negativeTest.Validate(), wantErr)
	}

	// Special case: Confirm the validation error is stable, in that it
	// is always the first invalid field in the slice.
	mixedTests := requiredFields{
		{"Mixed Field1", "ok"},
		{"Mixed Field2", ""},
		{"Mixed Field3", ""},
		{"Mixed Field4", "ok"},
		{"Mixed Field5", ""},
	}
	require.ErrorContains(t, mixedTests.Validate(), "Mixed Field2 is required")
}
