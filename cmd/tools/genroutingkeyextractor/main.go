package main

import (
	_ "embed"
	"flag"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"go.temporal.io/api/protometa/v1"
	_ "go.temporal.io/api/workflowservice/v1" // trigger proto file registration
	"go.temporal.io/server/cmd/tools/codegen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

//go:embed template.tmpl
var templateStr string

const (
	workflowServiceFile = "temporal/api/workflowservice/v1/service.proto"
	resourceIDHeader    = "temporal-resource-id"
	pollerGroupIDField  = "poller_group_id"
	pollerGroupStrategy = "namespace.RoutingStrategyPollerGroup"
)

var fieldPathRegex = regexp.MustCompile(`\{([^}]+)\}`)

type methodEntry struct {
	RequestType string
	Accessor    string
	Strategy    string
}

type templateData struct {
	Methods []methodEntry
}

func main() {
	outFlag := flag.String("out", ".", "output directory")
	flag.Parse()

	data := buildTemplateData()
	codegen.GenerateTemplateToFile(templateStr, data, *outFlag, "routing_key_extractor")
}

func buildTemplateData() templateData {
	fd, err := protoregistry.GlobalFiles.FindFileByPath(workflowServiceFile)
	if err != nil {
		codegen.Fatalf("finding %s: %v", workflowServiceFile, err)
	}

	if fd.Services().Len() == 0 {
		codegen.Fatalf("no services found in %s", workflowServiceFile)
	}
	svc := fd.Services().Get(0)

	var entries []methodEntry
	for i := range svc.Methods().Len() {
		method := svc.Methods().Get(i)
		if entry, ok := methodEntryFromDescriptor(svc, method); ok {
			entries = append(entries, entry)
		}
	}

	// Sort by request type for deterministic output.
	slices.SortFunc(entries, func(a, b methodEntry) int {
		return strings.Compare(a.RequestType, b.RequestType)
	})

	return templateData{Methods: entries}
}

// methodEntryFromDescriptor extracts the routing key accessor for a method.
// Returns (entry, ok=true) if the method has a concrete field path annotation,
// or (zero, ok=false) if the method should be skipped (no annotation).
func methodEntryFromDescriptor(
	svc protoreflect.ServiceDescriptor,
	method protoreflect.MethodDescriptor,
) (methodEntry, bool) {
	opts := method.Options()
	if opts == nil || !proto.HasExtension(opts, protometa.E_RequestHeader) {
		return methodEntry{}, false
	}

	annotations, _ := proto.GetExtension(opts, protometa.E_RequestHeader).([]*protometa.RequestHeaderAnnotation)
	for _, ann := range annotations {
		if ann.GetHeader() != resourceIDHeader {
			continue
		}

		matches := fieldPathRegex.FindAllStringSubmatch(ann.GetValue(), -1)
		if len(matches) != 1 {
			codegen.Fatalf("%s.%s: expected exactly one field interpolation in %s value %q, got %d",
				svc.Name(), method.Name(), resourceIDHeader, ann.GetValue(), len(matches))
		}
		fieldPath := matches[0][1]

		accessor, err := fieldPathToAccessor(fieldPath, method.Input())
		if err != nil {
			codegen.Fatalf("generating accessor for %s.%s field path %q: %v",
				svc.Name(), method.Name(), fieldPath, err)
		}

		// The resource_id field has the format "prefix:<businessID>".
		// Wrap the accessor to extract just the business ID part.
		if fieldPath == "resource_id" {
			accessor = "routingIDFromResourceID(" + accessor + ")"
		}

		// Fields whose terminal segment is `poller_group_id` route via the
		// poller-group strategy; everything else uses the default strategy.
		strategy := ""
		if terminalField(fieldPath) == pollerGroupIDField {
			strategy = pollerGroupStrategy
		}

		return methodEntry{
			RequestType: string(method.Input().Name()),
			Accessor:    accessor,
			Strategy:    strategy,
		}, true
	}

	return methodEntry{}, false
}

func terminalField(fieldPath string) string {
	parts := strings.Split(fieldPath, ".")
	return parts[len(parts)-1]
}

// fieldPathToAccessor converts a dot-separated proto field path to a Go getter chain.
// E.g. "workflow_execution.workflow_id" → "r.GetWorkflowExecution().GetWorkflowId()"
func fieldPathToAccessor(fieldPath string, msgDesc protoreflect.MessageDescriptor) (string, error) {
	parts := strings.Split(fieldPath, ".")
	accessor := "r"
	currentMsg := msgDesc

	for _, part := range parts {
		field := currentMsg.Fields().ByName(protoreflect.Name(part))
		if field == nil {
			return "", fmt.Errorf("field %q not found in message %s", part, currentMsg.FullName())
		}

		goName := protoFieldToGoName(part)
		accessor += ".Get" + goName + "()"

		if field.Kind() == protoreflect.MessageKind && field.Message() != nil {
			currentMsg = field.Message()
		}
	}

	return accessor, nil
}

// protoFieldToGoName converts a snake_case proto field name to PascalCase Go name.
func protoFieldToGoName(name string) string {
	parts := strings.Split(name, "_")
	for i := range parts {
		if len(parts[i]) > 0 {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}
