package regress

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateCompiledSuiteReturnsStructuredCompileError(t *testing.T) {
	_, err := validateCompiledSuite(Suite{})

	require.ErrorIs(t, err, ErrInvalidCompletedSuite)
	compileErr := new(CompileError)
	require.ErrorAs(t, err, &compileErr)
	require.Equal(t, ErrorInvalidCompletedSuite, compileErr.Category)
}

func TestValidateCompiledSuiteReturnsValidSuite(t *testing.T) {
	expected := Suite{Paths: []CompletedPath{{}}, PathCount: 1}

	actual, err := validateCompiledSuite(expected)

	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
