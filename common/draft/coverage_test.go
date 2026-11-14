package draft

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	_ "go.temporal.io/api/errordetails/v1"
	_ "go.temporal.io/api/operatorservice/v1"
	protometapb "go.temporal.io/api/protometa/v1"
	_ "go.temporal.io/api/workflowservice/v1"
	_ "go.temporal.io/server/common/draft/testfixture"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Inspect the descriptors actually linked by the server, independently of the generator planner.
func TestCoverage(t *testing.T) {
	messages := map[protoreflect.FullName]protoreflect.MessageDescriptor{}
	affected := map[protoreflect.FullName]bool{}
	errorDetailsVisited := false
	var collect func(protoreflect.MessageDescriptors)
	collect = func(descriptors protoreflect.MessageDescriptors) {
		for i := 0; i < descriptors.Len(); i++ {
			message := descriptors.Get(i)
			if message.IsMapEntry() {
				continue
			}
			messages[message.FullName()] = message
			if draftType(message) {
				affected[message.FullName()] = true
			}
			collect(message.Messages())
			fields := message.Fields()
			for j := 0; j < fields.Len(); j++ {
				feature, _ := proto.GetExtension(fields.Get(j).Options(), protometapb.E_Draft).(string)
				if feature != "" || draftFieldType(fields.Get(j)) {
					affected[message.FullName()] = true
				}
			}
		}
	}
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if !strings.HasPrefix(string(file.Package()), "temporal.api.") {
			return true
		}
		if file.Package() == "temporal.api.errordetails.v1" {
			errorDetailsVisited = true
		}
		collect(file.Messages())
		return true
	})
	require.True(t, errorDetailsVisited, "error detail descriptors must be linked and checked")
	require.Contains(t, messages, protoreflect.FullName("temporal.api.drafttest.v1.Message"))
	require.True(t, affected["temporal.api.drafttest.v1.EmptyDraft"], "empty draft messages still require omission")
	for changed := true; changed; {
		changed = false
		for name, message := range messages {
			if affected[name] {
				continue
			}
			fields := message.Fields()
			for i := 0; i < fields.Len(); i++ {
				field := fields.Get(i)
				if field.IsMap() {
					field = field.MapValue()
				}
				if child := field.Message(); child != nil && affected[child.FullName()] {
					affected[name] = true
					changed = true
					break
				}
			}
		}
	}
	omitter := reflect.TypeFor[draftOmitter]()
	for name, message := range messages {
		if strings.HasPrefix(string(name), "temporal.api.errordetails.v1.") {
			require.False(t, affected[name], "error detail %s travels through an excluded status channel", name)
		}
		messageType, err := protoregistry.GlobalTypes.FindMessageByName(name)
		require.NoError(t, err, "linked Go type missing for %s", message.FullName())
		implements := reflect.TypeOf(messageType.Zero().Interface()).Implements(omitter)
		require.Equal(t, affected[name], implements, "%s: generated omission method missing or stale", name)
	}
}

func draftType(d protoreflect.Descriptor) bool {
	for ; d != nil; d = d.Parent() {
		if message, ok := d.(protoreflect.MessageDescriptor); ok {
			feature, _ := proto.GetExtension(message.Options(), protometapb.E_DraftMessage).(string)
			if feature != "" {
				return true
			}
		}
	}
	return false
}

func draftFieldType(field protoreflect.FieldDescriptor) bool {
	if field.IsMap() {
		field = field.MapValue()
	}
	if message := field.Message(); message != nil {
		return draftType(message)
	}
	if enum := field.Enum(); enum != nil {
		return draftType(enum)
	}
	return false
}
