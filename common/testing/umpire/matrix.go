package umpire

import (
	"errors"
	"fmt"
)

const defaultMatrixCandidateLimit = 100_000

var (
	ErrMatrixDimension     = errors.New("invalid matrix dimension")
	ErrMatrixUnsatisfiable = errors.New("matrix constraints are unsatisfiable")
	ErrMatrixLimit         = errors.New("matrix candidate limit exceeded")
)

// MatrixDimension is one named, ordered set of matrix values.
type MatrixDimension struct {
	Name   string
	Values []string
}

// MatrixValue assigns one value to one dimension.
type MatrixValue struct {
	Dimension string
	Value     string
}

// MatrixCase is one complete assignment in declaration order.
type MatrixCase struct {
	Values []MatrixValue
}

// Value returns the assigned value for a named dimension, or an empty string when absent.
func (c MatrixCase) Value(dimension string) string {
	for _, value := range c.Values {
		if value.Dimension == dimension {
			return value.Value
		}
	}
	return ""
}

// MatrixConstraint accepts or rejects a complete matrix assignment.
type MatrixConstraint func(MatrixCase) bool

// GeneratePairwise returns a deterministic bounded greedy covering set.
func GeneratePairwise(dimensions []MatrixDimension, valid MatrixConstraint) ([]MatrixCase, error) {
	if err := validateMatrixDimensions(dimensions); err != nil {
		return nil, err
	}
	candidates, err := enumerateMatrixCandidates(dimensions, valid, defaultMatrixCandidateLimit)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, ErrMatrixUnsatisfiable
	}
	if len(dimensions) == 1 {
		return candidates, nil
	}

	uncovered := matrixPairs(candidates)
	selected := make([]MatrixCase, 0, len(candidates))
	used := make([]bool, len(candidates))
	for len(uncovered) > 0 {
		bestIndex := -1
		bestScore := 0
		for index, candidate := range candidates {
			if used[index] {
				continue
			}
			score := 0
			for pair := range matrixPairs([]MatrixCase{candidate}) {
				if _, missing := uncovered[pair]; missing {
					score++
				}
			}
			if score > bestScore {
				bestIndex = index
				bestScore = score
			}
		}
		if bestIndex < 0 {
			return nil, fmt.Errorf("%w: valid pairs cannot be covered", ErrMatrixUnsatisfiable)
		}
		used[bestIndex] = true
		selected = append(selected, cloneMatrixCase(candidates[bestIndex]))
		for pair := range matrixPairs([]MatrixCase{candidates[bestIndex]}) {
			delete(uncovered, pair)
		}
	}
	return selected, nil
}

func validateMatrixDimensions(dimensions []MatrixDimension) error {
	if len(dimensions) == 0 {
		return fmt.Errorf("%w: no dimensions", ErrMatrixDimension)
	}
	names := map[string]struct{}{}
	for _, dimension := range dimensions {
		if dimension.Name == "" {
			return fmt.Errorf("%w: name is empty", ErrMatrixDimension)
		}
		if _, exists := names[dimension.Name]; exists {
			return fmt.Errorf("%w: duplicate name %q", ErrMatrixDimension, dimension.Name)
		}
		names[dimension.Name] = struct{}{}
		if len(dimension.Values) == 0 {
			return fmt.Errorf("%w: %q has no values", ErrMatrixDimension, dimension.Name)
		}
		values := map[string]struct{}{}
		for _, value := range dimension.Values {
			if value == "" {
				return fmt.Errorf("%w: %q has an empty value", ErrMatrixDimension, dimension.Name)
			}
			if _, exists := values[value]; exists {
				return fmt.Errorf("%w: %q repeats value %q", ErrMatrixDimension, dimension.Name, value)
			}
			values[value] = struct{}{}
		}
	}
	return nil
}

func enumerateMatrixCandidates(dimensions []MatrixDimension, valid MatrixConstraint, limit int) ([]MatrixCase, error) {
	var result []MatrixCase
	current := MatrixCase{Values: make([]MatrixValue, len(dimensions))}
	var visit func(int) error
	visit = func(index int) error {
		if index == len(dimensions) {
			candidate := cloneMatrixCase(current)
			if valid != nil && !valid(candidate) {
				return nil
			}
			if len(result) >= limit {
				return fmt.Errorf("%w: more than %d valid assignments", ErrMatrixLimit, limit)
			}
			result = append(result, candidate)
			return nil
		}
		dimension := dimensions[index]
		for _, value := range dimension.Values {
			current.Values[index] = MatrixValue{Dimension: dimension.Name, Value: value}
			if err := visit(index + 1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(0); err != nil {
		return nil, err
	}
	return result, nil
}

func cloneMatrixCase(source MatrixCase) MatrixCase {
	return MatrixCase{Values: append([]MatrixValue(nil), source.Values...)}
}

type matrixPair struct {
	leftDimension  string
	leftValue      string
	rightDimension string
	rightValue     string
}

func matrixPairs(cases []MatrixCase) map[matrixPair]struct{} {
	result := map[matrixPair]struct{}{}
	for _, testCase := range cases {
		for left := 0; left < len(testCase.Values); left++ {
			for right := left + 1; right < len(testCase.Values); right++ {
				result[matrixPair{
					leftDimension:  testCase.Values[left].Dimension,
					leftValue:      testCase.Values[left].Value,
					rightDimension: testCase.Values[right].Dimension,
					rightValue:     testCase.Values[right].Value,
				}] = struct{}{}
			}
		}
	}
	return result
}
