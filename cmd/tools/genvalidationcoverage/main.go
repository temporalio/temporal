package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

var internalProtoFields = map[string]struct{}{
	"state":         {},
	"unknownFields": {},
	"sizeCache":     {},
}

type field struct {
	GoName    string
	ProtoName string
	Type      string
}

type templateData struct {
	PackageName string
	ImportPath  string
	TypeName    string
	Prefix      string
	SelfAlias   string // import alias for the package containing TypeName
	Nested      bool   // true for nested proto types (use NestedFieldValidator)
	Fields      []field
	Imports     map[string]string
}

func main() {
	var (
		messageFlag      = flag.String("message", "", "Go protobuf message type, as import/path.Type. Repeat with comma-separated values for multiple messages")
		messagesFileFlag = flag.String("messages-file", "", "file containing Go protobuf message types, one import/path.Type per line")
		outFlag          = flag.String("out", "", "output file")
	)
	flag.Parse()
	if err := run(*messageFlag, *messagesFileFlag, *outFlag); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(messageFlag, messagesFileFlag, outFlag string) error {
	if outFlag == "" {
		return errors.New("-out is required")
	}

	packageName, err := packageNameForOut(outFlag)
	if err != nil {
		return err
	}

	messages, err := messageList(messageFlag, messagesFileFlag)
	if err != nil {
		return err
	}
	if len(messages) == 0 {
		return errors.New("-message or -messages-file is required")
	}

	var allData []templateData
	for _, message := range messages {
		nested := strings.HasPrefix(message, "nested:")
		if nested {
			message = strings.TrimPrefix(message, "nested:")
		}

		importPath, typeName, err := splitMessage(message)
		if err != nil {
			return err
		}

		pkgDir, err := goListDir(importPath)
		if err != nil {
			return err
		}

		fields, imports, err := findStructFields(pkgDir, typeName)
		if err != nil {
			return err
		}

		allData = append(allData, templateData{
			PackageName: packageName,
			ImportPath:  importPath,
			TypeName:    typeName,
			Prefix:      unexported(typeName),
			SelfAlias:   selfAliasForImport(importPath, nested),
			Nested:      nested,
			Fields:      fields,
			Imports:     imports,
		})
	}

	src, err := render(allData)
	if err != nil {
		return err
	}

	return os.WriteFile(outFlag, src, 0o644)
}

func messageList(messageFlag string, messagesFile string) ([]string, error) {
	var messages []string
	addMessage := func(message string) {
		message = strings.TrimSpace(message)
		if message != "" {
			messages = append(messages, message)
		}
	}

	for message := range strings.SplitSeq(messageFlag, ",") {
		addMessage(message)
	}

	if messagesFile != "" {
		contents, err := os.ReadFile(messagesFile)
		if err != nil {
			return nil, err
		}
		for line := range strings.SplitSeq(string(contents), "\n") {
			line, _, _ = strings.Cut(line, "#")
			addMessage(line)
		}
	}

	return messages, nil
}

func splitMessage(message string) (importPath, typeName string, err error) {
	idx := strings.LastIndex(message, ".")
	if idx < 0 || idx == len(message)-1 {
		return "", "", fmt.Errorf("message must be import/path.Type: %q", message)
	}
	return message[:idx], message[idx+1:], nil
}

func goListDir(importPath string) (string, error) {
	cmd := exec.Command("go", "list", "-f", "{{.Dir}}", importPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("go list %s: %w\n%s", importPath, err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func findStructFields(pkgDir string, typeName string) ([]field, map[string]string, error) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, pkgDir, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, nil, err
	}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			if fields, imports, found, err := findStructFieldsInFile(fset, file, typeName); found || err != nil {
				return fields, imports, err
			}
		}
	}
	return nil, nil, fmt.Errorf("type %s not found in %s", typeName, pkgDir)
}

func findStructFieldsInFile(fset *token.FileSet, file *ast.File, typeName string) ([]field, map[string]string, bool, error) {
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.TYPE {
			continue
		}
		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != typeName {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				return nil, nil, true, fmt.Errorf("%s is not a struct", typeName)
			}
			fields, imports, err := protoStructFields(fset, file, structType)
			return fields, imports, true, err
		}
	}
	return nil, nil, false, nil
}

func protoStructFields(fset *token.FileSet, file *ast.File, structType *ast.StructType) ([]field, map[string]string, error) {
	importsByAlias := importsByAlias(file)
	neededImports := make(map[string]string)
	var fields []field
	for _, structField := range structType.Fields.List {
		for _, name := range structField.Names {
			if _, ok := internalProtoFields[name.Name]; ok {
				continue
			}
			typeName, err := typeString(fset, structField.Type, importsByAlias, neededImports)
			if err != nil {
				return nil, nil, err
			}
			fields = append(fields, field{GoName: name.Name, ProtoName: protoFieldName(name.Name, structField.Tag), Type: typeName})
		}
	}
	return fields, neededImports, nil
}

func protoFieldName(goName string, tag *ast.BasicLit) string {
	if tag == nil {
		return goName
	}
	match := regexp.MustCompile(`name=([^,"]+)`).FindStringSubmatch(tag.Value)
	if len(match) != 2 {
		return goName
	}
	return match[1]
}

func packageNameForOut(out string) (string, error) {
	dir := filepath.Dir(out)
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, nil, parser.PackageClauseOnly)
	if err != nil {
		return "", err
	}
	if len(pkgs) == 0 {
		return "", fmt.Errorf("no Go package found in %s", dir)
	}
	names := make([]string, 0, len(pkgs))
	for name := range pkgs {
		names = append(names, name)
	}
	slices.Sort(names)
	return names[0], nil
}

func selfAliasForImport(importPath string, nested bool) string {
	if !nested {
		// Top-level RPC messages are always from workflowservice; keep the existing alias.
		return "workflowservice"
	}
	return outputImportAlias(importPath, "v1")
}

func render(allData []templateData) ([]byte, error) {
	if len(allData) == 0 {
		return nil, errors.New("no messages specified")
	}
	var b bytes.Buffer
	fmt.Fprint(&b, "// Code generated by genvalidationcoverage. DO NOT EDIT.\n\n")
	fmt.Fprintf(&b, "package %s\n\n", allData[0].PackageName)
	fmt.Fprint(&b, "import (\n")
	imports := make(map[string]string)
	for _, data := range allData {
		imports[data.SelfAlias] = data.ImportPath
		maps.Copy(imports, data.Imports)
	}
	for _, alias := range sortedImportAliases(imports) {
		fmt.Fprintf(&b, "\t%s %q\n", alias, imports[alias])
	}
	fmt.Fprint(&b, "\t\"go.temporal.io/server/common/validation\"\n")
	fmt.Fprint(&b, ")\n\n")
	for _, data := range allData {
		if data.Nested {
			renderNestedValidator(&b, data)
		} else {
			renderValidator(&b, data)
		}
	}

	src, err := format.Source(b.Bytes())
	if err != nil {
		return nil, errors.Join(err, fmt.Errorf("source:\n%s", b.String()))
	}
	return src, nil
}

func renderValidator(b *bytes.Buffer, data templateData) {
	fmt.Fprintf(b, "type %sFieldValidators struct {\n", data.Prefix)
	for _, f := range data.Fields {
		fmt.Fprintf(b, "\t%s validation.FieldValidator[%s.%s, %s]\n", f.GoName, data.SelfAlias, data.TypeName, f.Type)
	}
	fmt.Fprint(b, "}\n")
	fmt.Fprintf(b, "\nfunc (v %sFieldValidators) ValidateAndNormalize(req *%s.%s) error {\n", data.Prefix, data.SelfAlias, data.TypeName)
	for _, f := range data.Fields {
		fmt.Fprintf(b, "\tif err := v.%s(req, %q, req.Get%s()); err != nil {\n", f.GoName, f.ProtoName, f.GoName)
		fmt.Fprint(b, "\t\treturn err\n")
		fmt.Fprint(b, "\t}\n")
	}
	fmt.Fprint(b, "\treturn nil\n")
	fmt.Fprint(b, "}\n")
	fmt.Fprintf(b, "\nfunc (v %sFieldValidators) RegisterValidator(registry *validation.ValidatorRegistry) error {\n", data.Prefix)
	fmt.Fprintf(b, "\treturn validation.RegisterValidator[%s.%s](registry, v)\n", data.SelfAlias, data.TypeName)
	fmt.Fprint(b, "}\n")
}

func renderNestedValidator(b *bytes.Buffer, data templateData) {
	fmt.Fprintf(b, "type %sFieldValidators struct {\n", data.Prefix)
	for _, f := range data.Fields {
		fmt.Fprintf(b, "\t%s validation.NestedFieldValidator[%s.%s, %s]\n", f.GoName, data.SelfAlias, data.TypeName, f.Type)
	}
	fmt.Fprint(b, "}\n")
	fmt.Fprintf(b, "\nfunc (v %sFieldValidators) ValidateAndNormalize(ns string, fieldPrefix string, req *%s.%s) error {\n", data.Prefix, data.SelfAlias, data.TypeName)
	for _, f := range data.Fields {
		fmt.Fprintf(b, "\tif err := v.%s(ns, req, fieldPrefix+%q, req.Get%s()); err != nil {\n", f.GoName, "."+f.ProtoName, f.GoName)
		fmt.Fprint(b, "\t\treturn err\n")
		fmt.Fprint(b, "\t}\n")
	}
	fmt.Fprint(b, "\treturn nil\n")
	fmt.Fprint(b, "}\n")
}

func importsByAlias(file *ast.File) map[string]string {
	imports := make(map[string]string)
	for _, spec := range file.Imports {
		path := strings.Trim(spec.Path.Value, `"`)
		alias := ""
		if spec.Name != nil {
			alias = spec.Name.Name
		} else {
			parts := strings.Split(path, "/")
			alias = parts[len(parts)-1]
		}
		imports[alias] = path
	}
	return imports
}

func typeString(
	fset *token.FileSet,
	expr ast.Expr,
	importsByAlias map[string]string,
	neededImports map[string]string,
) (string, error) {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name, nil
	case *ast.StarExpr:
		name, err := typeString(fset, t.X, importsByAlias, neededImports)
		return "*" + name, err
	case *ast.ArrayType:
		name, err := typeString(fset, t.Elt, importsByAlias, neededImports)
		return "[]" + name, err
	case *ast.MapType:
		key, err := typeString(fset, t.Key, importsByAlias, neededImports)
		if err != nil {
			return "", err
		}
		value, err := typeString(fset, t.Value, importsByAlias, neededImports)
		if err != nil {
			return "", err
		}
		return "map[" + key + "]" + value, nil
	case *ast.SelectorExpr:
		ident, ok := t.X.(*ast.Ident)
		if !ok {
			break
		}
		importPath, ok := importsByAlias[ident.Name]
		if !ok {
			break
		}
		alias := outputImportAlias(importPath, ident.Name)
		neededImports[alias] = importPath
		return alias + "." + t.Sel.Name, nil
	}

	var b bytes.Buffer
	if err := printer.Fprint(&b, fset, expr); err != nil {
		return "", err
	}
	return "", fmt.Errorf("unsupported field type expression %q", b.String())
}

var generatedAliasPattern = regexp.MustCompile(`^v[0-9]+$`)

func outputImportAlias(importPath string, sourceAlias string) string {
	if !generatedAliasPattern.MatchString(sourceAlias) {
		return sourceAlias
	}
	if strings.HasPrefix(importPath, "go.temporal.io/api/") && strings.HasSuffix(importPath, "/v1") {
		parts := strings.Split(importPath, "/")
		return parts[len(parts)-2] + "pb"
	}
	parts := strings.Split(importPath, "/")
	return parts[len(parts)-1]
}

func sortedImportAliases(imports map[string]string) []string {
	aliases := make([]string, 0, len(imports))
	for alias := range imports {
		aliases = append(aliases, alias)
	}
	slices.Sort(aliases)
	return aliases
}

func unexported(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}
