package mutationtest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"

	"go.temporal.io/server/tools/mutationtest/operators"
)

type mutantRecord struct {
	id       int
	operator string
	file     string
	line     int
	column   int
	digest   string
	source   []byte
}

type generatorStats struct {
	generated  int
	duplicates int
}

type mutationGenerator struct {
	ctx      context.Context
	coverage coverageIndex
	emit     func(mutantRecord) error
	seen     map[[32]byte]struct{}
	stats    generatorStats
}

func generateMutantsWithOperators(
	ctx context.Context,
	targets []targetFile,
	coverage coverageIndex,
	selectedOperators []operators.Operator,
	emit func(mutantRecord) error,
) (generatorStats, error) {
	generator := mutationGenerator{
		ctx:      ctx,
		coverage: coverage,
		emit:     emit,
		seen:     make(map[[32]byte]struct{}),
	}
	for _, target := range targets {
		if err := generator.generateTarget(target, selectedOperators); err != nil {
			return generator.stats, err
		}
	}
	return generator.stats, nil
}

func (generator *mutationGenerator) generateTarget(target targetFile, selectedOperators []operators.Operator) error {
	baseline, err := formatNode(target.fileSet, target.syntax)
	if err != nil {
		return fmt.Errorf("format mutation target %s: %w", target.relativePath, err)
	}
	for _, operator := range selectedOperators {
		if err := generator.generateOperator(target, baseline, operator); err != nil {
			return err
		}
	}
	return nil
}

func (generator *mutationGenerator) generateOperator(target targetFile, baseline []byte, operator operators.Operator) error {
	var walkErr error
	ast.Inspect(target.syntax, func(node ast.Node) bool {
		if walkErr != nil || node == nil {
			return false
		}
		if err := generator.ctx.Err(); err != nil {
			walkErr = err
			return false
		}
		if !generator.coverage.covers(target, node.Pos()) {
			return true
		}
		walkErr = generator.generateNodeMutations(target, baseline, operator, node)
		return walkErr == nil
	})
	return walkErr
}

func (generator *mutationGenerator) generateNodeMutations(
	target targetFile,
	baseline []byte,
	operator operators.Operator,
	node ast.Node,
) error {
	location := target.fileSet.Position(node.Pos())
	for _, mutation := range operator.Mutate(target.types, target.typesInfo, node) {
		source, err := renderMutation(target.fileSet, target.syntax, baseline, mutation.Change, mutation.Reset)
		if err != nil {
			return fmt.Errorf("generate %s mutation for %s:%d:%d: %w", operator.Name(), target.relativePath, location.Line, location.Column, err)
		}
		if bytes.Equal(source, baseline) {
			generator.stats.duplicates++
			continue
		}
		digest := sha256.Sum256(append([]byte(target.relativePath+"\x00"), source...))
		if _, duplicate := generator.seen[digest]; duplicate {
			generator.stats.duplicates++
			continue
		}
		generator.seen[digest] = struct{}{}
		generator.stats.generated++
		record := mutantRecord{
			id:       generator.stats.generated,
			operator: operator.Name(),
			file:     target.relativePath,
			line:     location.Line,
			column:   location.Column,
			digest:   fmt.Sprintf("%x", digest),
			source:   source,
		}
		if err := generator.emit(record); err != nil {
			return err
		}
	}
	return nil
}

func renderMutation(
	fileSet *token.FileSet,
	file *ast.File,
	baseline []byte,
	change func(),
	reset func(),
) ([]byte, error) {
	change()
	mutated, mutationErr := formatNode(fileSet, file)
	reset()
	restored, resetErr := formatNode(fileSet, file)
	if mutationErr != nil {
		return nil, mutationErr
	}
	if resetErr != nil {
		return nil, fmt.Errorf("format reset syntax: %w", resetErr)
	}
	if !bytes.Equal(baseline, restored) {
		return nil, errors.New("mutation reset did not restore the syntax tree")
	}
	return mutated, nil
}

func formatNode(fileSet *token.FileSet, file *ast.File) ([]byte, error) {
	var buffer bytes.Buffer
	if err := format.Node(&buffer, fileSet, file); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
