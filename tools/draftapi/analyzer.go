package draftapi

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer requires an explicit opt-in for direct references to draft protobuf APIs and
// limits draft annotations to locations supported by response filtering.
var Analyzer = &analysis.Analyzer{
	Name:      "draftapi",
	Doc:       "report references to draft Temporal protobuf APIs and unsupported draft annotations",
	Run:       run,
	FactTypes: []analysis.Fact{new(apiFact), new(optionsFact)},
}

type apiFact struct {
	Features []string
	Symbol   string
}

func (*apiFact) AFact() {}

func (f *apiFact) String() string { return strings.Join(f.Features, ", ") }

func run(pass *analysis.Pass) (any, error) {
	checkAPIMetadata(pass)
	if !strings.HasPrefix(pass.Pkg.Path(), "go.temporal.io/server/") || exemptPackage(pass.Pkg.Path()) {
		return nil, nil
	}
	for _, file := range pass.Files {
		if strings.HasSuffix(pass.Fset.Position(file.Pos()).Filename, "_test.go") || ast.IsGenerated(file) {
			continue
		}
		suppressions := suppressedRanges(pass.Fset, file)
		ast.Inspect(file, func(node ast.Node) bool {
			id, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			obj := pass.TypesInfo.Uses[id]
			if obj == nil || obj.Pkg() == nil || obj.Pkg() == pass.Pkg {
				return true
			}
			var fact apiFact
			if !pass.ImportObjectFact(obj, &fact) || suppressed(suppressions, id.Pos(), fact.Features) {
				return true
			}
			pass.Reportf(id.Pos(), "use of draft API %s (%s); acknowledge with // temporal:allow-draft-api %s",
				fact.Symbol, strings.Join(fact.Features, ", "), strings.Join(fact.Features, " "))
			return true
		})
	}
	return nil, nil
}

func checkAPIMetadata(pass *analysis.Pass) {
	var metadata optionsFact
	var options []option
	for _, pkg := range pass.Pkg.Imports() {
		var imported optionsFact
		if pass.ImportPackageFact(pkg, &imported) {
			options = append(options, imported.Options...)
			metadata.Errors = append(metadata.Errors, imported.Errors...)
		}
	}
	if strings.HasPrefix(pass.Pkg.Path(), "go.temporal.io/api/") {
		exportAPIs(pass, options, &metadata)
	}
	metadata.Errors = reportValidationErrors(pass, metadata.Errors)
	if len(metadata.Options) > 0 || len(metadata.Errors) > 0 {
		pass.ExportPackageFact(&metadata)
	}
}

func reportValidationErrors(pass *analysis.Pass, errors []string) []string {
	slices.Sort(errors)
	errors = slices.Compact(errors)
	for _, err := range errors {
		pass.Reportf(pass.Files[0].Pos(), "draft API validation failed: %s", err)
	}
	return errors
}

func objectName(obj types.Object) string {
	if fn, ok := obj.(*types.Func); ok {
		if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
			return fmt.Sprintf("%s.%s", sig.Recv().Type(), obj.Name())
		}
	}
	return obj.Pkg().Path() + "." + obj.Name()
}

func exemptPackage(path string) bool {
	path = strings.TrimPrefix(path, "go.temporal.io/server/")
	return path == "tests" || strings.HasPrefix(path, "tests/") ||
		path == "common/testing" || strings.HasPrefix(path, "common/testing/")
}

type sourceRange struct {
	start    token.Pos
	end      token.Pos
	features []string
}

func suppressed(ranges []sourceRange, pos token.Pos, features []string) bool {
	return len(features) > 0 && !slices.ContainsFunc(features, func(feature string) bool {
		return !slices.ContainsFunc(ranges, func(r sourceRange) bool {
			return r.start <= pos && pos < r.end && slices.Contains(r.features, feature)
		})
	})
}

func suppressedRanges(fset *token.FileSet, file *ast.File) []sourceRange {
	firstCode := firstCodeByLine(fset, file)
	var ranges []sourceRange
	comments := ast.NewCommentMap(fset, file, file.Comments)
	for node, groups := range comments {
		for _, group := range groups {
			for _, comment := range group.List {
				features := allowedFeatures(comment.Text)
				if len(features) == 0 {
					continue
				}
				// A trailing directive, after code on its line, applies only to that line. A
				// leading directive applies to the associated declaration or statement,
				// including its body.
				line := fset.Position(comment.Pos()).Line
				if first, ok := firstCode[line]; ok && first < comment.Pos() {
					ranges = append(ranges, lineRange(fset.File(comment.Pos()), line, features))
				} else if fset.Position(group.End()).Line+1 == fset.Position(node.Pos()).Line {
					ranges = append(ranges, sourceRange{node.Pos(), node.End(), features})
				}
			}
		}
	}
	return ranges
}

// firstCodeByLine maps each line to the earliest position where a syntax node starts or ends.
func firstCodeByLine(fset *token.FileSet, file *ast.File) map[int]token.Pos {
	firstCode := make(map[int]token.Pos)
	ast.Inspect(file, func(node ast.Node) bool {
		switch node.(type) {
		case nil, *ast.CommentGroup, *ast.Comment:
			return false
		}
		for _, pos := range []token.Pos{node.Pos(), node.End() - 1} {
			if first, ok := firstCode[fset.Position(pos).Line]; !ok || pos < first {
				firstCode[fset.Position(pos).Line] = pos
			}
		}
		return true
	})
	return firstCode
}

// lineRange covers one whole source line.
func lineRange(tf *token.File, line int, features []string) sourceRange {
	end := tf.Pos(tf.Size())
	if line < tf.LineCount() {
		end = tf.LineStart(line + 1)
	}
	return sourceRange{tf.LineStart(line), end, features}
}

func allowedFeatures(comment string) []string {
	comment = strings.TrimSpace(strings.TrimPrefix(comment, "//"))
	directive, _, _ := strings.Cut(comment, "//")
	fields := strings.Fields(directive)
	if len(fields) < 2 || fields[0] != "temporal:allow-draft-api" {
		return nil
	}
	return fields[1:]
}

func generatedProto(pass *analysis.Pass, file *ast.File) bool {
	return ast.IsGenerated(file) && strings.HasSuffix(filepath.Base(pass.Fset.Position(file.Pos()).Filename), ".pb.go")
}
