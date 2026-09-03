package mutationtest

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/tools/go/packages"
)

type targetFile struct {
	packagePath  string
	relativePath string
	coveragePath string
	syntax       *ast.File
	fileSet      *token.FileSet
	types        *types.Package
	typesInfo    *types.Info
}

func loadTargets(ctx context.Context, repoRoot string, relativePaths []string, buildTags string) ([]targetFile, error) {
	selected, queries, err := targetQueries(repoRoot, relativePaths)
	if err != nil {
		return nil, err
	}
	loaded, err := loadTargetPackages(ctx, repoRoot, queries, buildTags)
	if err != nil {
		return nil, err
	}
	targetsByPath, err := mapLoadedTargets(loaded, selected)
	if err != nil {
		return nil, err
	}
	return orderLoadedTargets(relativePaths, targetsByPath)
}

func targetQueries(repoRoot string, relativePaths []string) (map[string]string, []string, error) {
	selected := make(map[string]string, len(relativePaths))
	querySet := make(map[string]struct{})
	for _, relativePath := range relativePaths {
		absolutePath, err := filepath.Abs(filepath.Join(repoRoot, filepath.FromSlash(relativePath)))
		if err != nil {
			return nil, nil, err
		}
		selected[filepath.Clean(absolutePath)] = filepath.ToSlash(relativePath)
		directory := filepath.ToSlash(filepath.Dir(relativePath))
		if directory == "." {
			querySet["."] = struct{}{}
		} else {
			querySet["./"+directory] = struct{}{}
		}
	}
	queries := make([]string, 0, len(querySet))
	for query := range querySet {
		queries = append(queries, query)
	}
	slices.Sort(queries)
	return selected, queries, nil
}

func loadTargetPackages(ctx context.Context, repoRoot string, queries []string, buildTags string) ([]*packages.Package, error) {
	buildFlags := make([]string, 0, 1)
	if strings.TrimSpace(buildTags) != "" {
		buildFlags = append(buildFlags, "-tags="+buildTags)
	}
	loaded, err := packages.Load(&packages.Config{
		Context:    ctx,
		Dir:        repoRoot,
		Env:        append(os.Environ(), "GOFLAGS="),
		BuildFlags: buildFlags,
		Mode: packages.NeedName |
			packages.NeedFiles |
			packages.NeedCompiledGoFiles |
			packages.NeedSyntax |
			packages.NeedTypes |
			packages.NeedTypesInfo |
			packages.NeedImports |
			packages.NeedDeps,
	}, queries...)
	if err != nil {
		return nil, fmt.Errorf("load mutation targets: %w", err)
	}
	var loadErrors []error
	for _, pkg := range loaded {
		for _, loadErr := range pkg.Errors {
			loadErrors = append(loadErrors, errors.New(loadErr.Error()))
		}
	}
	if err := errors.Join(loadErrors...); err != nil {
		return nil, fmt.Errorf("load mutation targets: %w", err)
	}
	return loaded, nil
}

func mapLoadedTargets(loaded []*packages.Package, selected map[string]string) (map[string]targetFile, error) {
	targetsByPath := make(map[string]targetFile, len(selected))
	for _, pkg := range loaded {
		for index, filename := range pkg.CompiledGoFiles {
			relativePath, ok, err := selectedRelativePath(filename, selected)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			if index >= len(pkg.Syntax) {
				return nil, fmt.Errorf("package loader omitted syntax for %s", relativePath)
			}
			if _, duplicate := targetsByPath[relativePath]; duplicate {
				return nil, fmt.Errorf("multiple packages own mutation target %s", relativePath)
			}
			targetsByPath[relativePath] = targetFile{
				packagePath:  pkg.PkgPath,
				relativePath: relativePath,
				coveragePath: pkg.PkgPath + "/" + filepath.ToSlash(filepath.Base(filename)),
				syntax:       pkg.Syntax[index],
				fileSet:      pkg.Fset,
				types:        pkg.Types,
				typesInfo:    pkg.TypesInfo,
			}
		}
	}
	return targetsByPath, nil
}

func selectedRelativePath(filename string, selected map[string]string) (string, bool, error) {
	absolutePath, err := filepath.Abs(filename)
	if err != nil {
		return "", false, err
	}
	relativePath, ok := selected[filepath.Clean(absolutePath)]
	return relativePath, ok, nil
}

func orderLoadedTargets(relativePaths []string, targetsByPath map[string]targetFile) ([]targetFile, error) {
	targets := make([]targetFile, 0, len(relativePaths))
	for _, relativePath := range relativePaths {
		target, ok := targetsByPath[relativePath]
		if !ok {
			return nil, fmt.Errorf("mutation target was not loaded: %s", relativePath)
		}
		targets = append(targets, target)
	}
	return targets, nil
}
