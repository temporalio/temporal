package draftapi

import (
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	_ "go.temporal.io/api/errordetails/v1"
	_ "go.temporal.io/api/operatorservice/v1"
	protometapb "go.temporal.io/api/protometa/v1"
	_ "go.temporal.io/api/workflowservice/v1"
	"golang.org/x/mod/modfile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// TestAPIContract runs the analyzer on server code that references every message type
// and field getter of the go.temporal.io/api the server links. The draft annotations in
// the linked descriptors decide which references must be reported and with which
// features: a type has its own draft_message feature and those of the messages it is
// nested in, and a getter adds its field's own feature and those of the field's type.
// Every other reference must not be reported.
func TestAPIContract(t *testing.T) {
	source, expected := apiContractSource(t)

	serverRoot, err := filepath.Abs("../..")
	require.NoError(t, err)
	dir := t.TempDir()
	writeContractModule(t, serverRoot, dir, source)
	tool := filepath.Join(t.TempDir(), "draftapi")
	build := exec.CommandContext(t.Context(), "go", "build", "-tags", "test_dep", "-o", tool, "./cmd/tools/draftapi")
	build.Dir = serverRoot
	output, err := build.CombinedOutput()
	require.NoError(t, err, "%s", output)

	cmd := exec.CommandContext(t.Context(), "go", "vet", "-tags", "test_dep", "-vettool="+tool, "./contract")
	cmd.Dir = dir
	output, err = cmd.CombinedOutput()
	diagnostic := regexp.MustCompile(`contract\.go:(\d+):\d+: use of draft API \S+ \(([^)]*)\)`)
	actual := map[int][]string{}
	for _, match := range diagnostic.FindAllStringSubmatch(string(output), -1) {
		line, convErr := strconv.Atoi(match[1])
		require.NoError(t, convErr)
		actual[line] = featureSet(append(actual[line], strings.Split(match[2], ", ")...))
	}
	if err != nil && len(actual) == 0 {
		t.Fatalf("go vet failed without draft diagnostics: %v\n%s", err, output)
	}
	lines := strings.Split(source, "\n")
	for _, line := range slices.Sorted(maps.Keys(maps.Collect(func(yield func(int, bool) bool) {
		for line := range expected {
			yield(line, true)
		}
		for line := range actual {
			yield(line, true)
		}
	}))) {
		if !slices.Equal(expected[line], actual[line]) {
			t.Errorf("contract.go:%d %s: want draft features %v, got %v", line, strings.TrimSpace(lines[line-1]), expected[line], actual[line])
		}
	}
}

// apiContractSource generates the referencing package and the features expected per line.
func apiContractSource(t *testing.T) (string, map[int][]string) {
	t.Helper()
	aliases := map[string]string{}
	alias := func(pkgPath string) string {
		if aliases[pkgPath] == "" {
			aliases[pkgPath] = fmt.Sprintf("p%d", len(aliases))
		}
		return aliases[pkgPath]
	}
	type entry struct {
		code     string
		features []string
	}
	var body []entry
	var visit func(protoreflect.MessageDescriptors)
	visit = func(messages protoreflect.MessageDescriptors) {
		for i := 0; i < messages.Len(); i++ {
			message := messages.Get(i)
			visit(message.Messages())
			if message.IsMapEntry() {
				continue
			}
			goType := goMessageType(t, message)
			body = append(body,
				entry{code: "func _() {"},
				entry{code: fmt.Sprintf("\tvar m *%s.%s", alias(goType.PkgPath()), goType.Name()), features: typeFeatures(message)},
				entry{code: "\t_ = m"},
			)
			for j := 0; j < message.Fields().Len(); j++ {
				field := message.Fields().Get(j)
				getter := "Get" + goCamelCase(string(field.Name()))
				if _, ok := reflect.PointerTo(goType).MethodByName(getter); !ok {
					t.Fatalf("%s has no getter %s for %s", goType, getter, field.FullName())
				}
				features := typeFeatures(message)
				if feature, _ := proto.GetExtension(field.Options(), protometapb.E_Draft).(string); feature != "" {
					features = append(features, feature)
				}
				features = append(features, fieldTypeFeatures(field)...)
				body = append(body, entry{code: fmt.Sprintf("\t_ = m.%s()", getter), features: featureSet(features)})
			}
			body = append(body, entry{code: "}"})
		}
	}
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if strings.HasPrefix(string(file.Package()), "temporal.api.") {
			visit(file.Messages())
		}
		return true
	})
	require.NotEmpty(t, body)

	var b strings.Builder
	b.WriteString("package contract\n\nimport (\n")
	for _, pkgPath := range slices.Sorted(maps.Keys(aliases)) {
		fmt.Fprintf(&b, "\t%s %q\n", aliases[pkgPath], pkgPath)
	}
	b.WriteString(")\n\n")
	line := strings.Count(b.String(), "\n") + 1
	expected := map[int][]string{}
	for _, e := range body {
		b.WriteString(e.code + "\n")
		if len(e.features) > 0 {
			expected[line] = featureSet(e.features)
		}
		line++
	}
	return b.String(), expected
}

// typeFeatures returns the draft_message features of message and of the messages it is
// nested in.
func typeFeatures(descriptor protoreflect.Descriptor) []string {
	var features []string
	for ; descriptor != nil; descriptor = descriptor.Parent() {
		if message, ok := descriptor.(protoreflect.MessageDescriptor); ok {
			if feature, _ := proto.GetExtension(message.Options(), protometapb.E_DraftMessage).(string); feature != "" {
				features = append(features, feature)
			}
		}
	}
	return features
}

// fieldTypeFeatures returns the features of the message or enum a field holds, or of a
// map field's value.
func fieldTypeFeatures(field protoreflect.FieldDescriptor) []string {
	if field.IsMap() {
		field = field.MapValue()
	}
	if field.Message() != nil {
		return typeFeatures(field.Message())
	}
	if field.Enum() != nil {
		return typeFeatures(field.Enum())
	}
	return nil
}

func featureSet(features []string) []string {
	set := slices.Sorted(slices.Values(features))
	return slices.Compact(set)
}

func goMessageType(t *testing.T, message protoreflect.MessageDescriptor) reflect.Type {
	t.Helper()
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(message.FullName())
	require.NoError(t, err)
	return reflect.TypeOf(messageType.Zero().Interface()).Elem()
}

// goCamelCase is protoc-gen-go's conversion of a field name to its Go name.
func goCamelCase(s string) string {
	var b []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '.' && i+1 < len(s) && isASCIILower(s[i+1]):
		case c == '.':
			b = append(b, '_')
		case c == '_' && (i == 0 || s[i-1] == '.'):
			b = append(b, 'X')
		case c == '_' && i+1 < len(s) && isASCIILower(s[i+1]):
		case isASCIIDigit(c):
			b = append(b, c)
		default:
			if isASCIILower(c) {
				c -= 'a' - 'A'
			}
			b = append(b, c)
			for ; i+1 < len(s) && isASCIILower(s[i+1]); i++ {
				b = append(b, s[i+1])
			}
		}
	}
	return string(b)
}

func isASCIILower(c byte) bool { return 'a' <= c && c <= 'z' }
func isASCIIDigit(c byte) bool { return '0' <= c && c <= '9' }

// writeContractModule writes a copy of the server module holding only the contract
// package, with the server's dependencies and relative replaces made absolute.
func writeContractModule(t *testing.T, serverRoot, dir, source string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(serverRoot, "go.mod"))
	require.NoError(t, err)
	mod, err := modfile.Parse("go.mod", data, nil)
	require.NoError(t, err)
	var relative []modfile.Replace
	for _, replacement := range mod.Replace {
		if replacement.New.Version == "" && !filepath.IsAbs(replacement.New.Path) {
			relative = append(relative, *replacement)
		}
	}
	for _, replacement := range relative {
		require.NoError(t, mod.DropReplace(replacement.Old.Path, replacement.Old.Version))
		require.NoError(t, mod.AddReplace(replacement.Old.Path, replacement.Old.Version, filepath.Join(serverRoot, replacement.New.Path), ""))
	}
	data, err = mod.Format()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), data, 0600))
	data, err = os.ReadFile(filepath.Join(serverRoot, "go.sum"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.sum"), data, 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "contract"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "contract", "contract.go"), []byte(source), 0600))
}
