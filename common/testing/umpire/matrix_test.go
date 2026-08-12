package umpire

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGeneratePairwiseIsDeterministicAndCoversEveryPair(t *testing.T) {
	dimensions := []MatrixDimension{
		{Name: "os", Values: []string{"linux", "darwin", "windows"}},
		{Name: "database", Values: []string{"mysql", "postgres", "sqlite"}},
		{Name: "mode", Values: []string{"sync", "async"}},
	}

	first, err := GeneratePairwise(dimensions, nil)
	require.NoError(t, err)
	second, err := GeneratePairwise(dimensions, nil)
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.Less(t, len(first), 18)
	requireMatrixPairsCovered(t, dimensions, first, nil)
	for _, testCase := range first {
		require.Equal(t, []string{"os", "database", "mode"}, matrixCaseNames(testCase))
	}
}

func TestGeneratePairwiseHonorsConstraints(t *testing.T) {
	dimensions := []MatrixDimension{
		{Name: "os", Values: []string{"linux", "darwin"}},
		{Name: "database", Values: []string{"mysql", "postgres"}},
		{Name: "mode", Values: []string{"sync", "async"}},
	}
	valid := func(testCase MatrixCase) bool {
		return testCase.Value("os") != "darwin" || testCase.Value("database") != "mysql"
	}

	cases, err := GeneratePairwise(dimensions, valid)
	require.NoError(t, err)

	for _, testCase := range cases {
		require.True(t, valid(testCase))
	}
	requireMatrixPairsCovered(t, dimensions, cases, valid)
}

func TestGeneratePairwiseRejectsInvalidAndUnsatisfiableInputs(t *testing.T) {
	tests := []struct {
		name       string
		dimensions []MatrixDimension
		valid      MatrixConstraint
		want       error
	}{
		{name: "empty", want: ErrMatrixDimension},
		{name: "empty name", dimensions: []MatrixDimension{{Values: []string{"a"}}}, want: ErrMatrixDimension},
		{name: "empty values", dimensions: []MatrixDimension{{Name: "one"}}, want: ErrMatrixDimension},
		{name: "duplicate dimension", dimensions: []MatrixDimension{{Name: "one", Values: []string{"a"}}, {Name: "one", Values: []string{"b"}}}, want: ErrMatrixDimension},
		{name: "duplicate value", dimensions: []MatrixDimension{{Name: "one", Values: []string{"a", "a"}}}, want: ErrMatrixDimension},
		{name: "unsatisfiable", dimensions: []MatrixDimension{{Name: "one", Values: []string{"a", "b"}}}, valid: func(MatrixCase) bool { return false }, want: ErrMatrixUnsatisfiable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cases, err := GeneratePairwise(test.dimensions, test.valid)

			require.Nil(t, cases)
			require.ErrorIs(t, err, test.want)
		})
	}
}

func TestGeneratePairwiseReturnsAllValuesForOneDimension(t *testing.T) {
	cases, err := GeneratePairwise([]MatrixDimension{{Name: "mode", Values: []string{"sync", "async"}}}, nil)

	require.NoError(t, err)
	require.Equal(t, []MatrixCase{
		{Values: []MatrixValue{{Dimension: "mode", Value: "sync"}}},
		{Values: []MatrixValue{{Dimension: "mode", Value: "async"}}},
	}, cases)
}

func requireMatrixPairsCovered(t *testing.T, dimensions []MatrixDimension, cases []MatrixCase, valid MatrixConstraint) {
	t.Helper()
	validCases, err := enumerateMatrixCandidates(dimensions, valid, defaultMatrixCandidateLimit)
	require.NoError(t, err)
	want := matrixPairs(validCases)
	got := matrixPairs(cases)
	for pair := range want {
		_, covered := got[pair]
		require.Truef(t, covered, "pair is not covered: %+v", pair)
	}
}

func matrixCaseNames(testCase MatrixCase) []string {
	names := make([]string, len(testCase.Values))
	for index, value := range testCase.Values {
		names[index] = value.Dimension
	}
	return names
}

func TestMatrixErrorsAreDistinct(t *testing.T) {
	require.NotErrorIs(t, ErrMatrixDimension, ErrMatrixUnsatisfiable)
}
