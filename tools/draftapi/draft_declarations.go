package draftapi

import (
	"fmt"
	"slices"
	"strings"
	"unicode"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// checkDeclarations limits draft annotations to locations supported by response filtering.
func (c *catalog) checkDeclarations(files map[string]*descriptorpb.FileDescriptorProto) error {
	for _, file := range files {
		// Option facts flow only through direct imports, so a public import would hide
		// annotations from the files that use it. Reject it as the generators do.
		for _, index := range file.PublicDependency {
			if int(index) < len(file.Dependency) && slices.ContainsFunc(c.options, func(opt option) bool { return opt.File == file.Dependency[index] }) {
				return fmt.Errorf("%s: public import of %s is not supported; import it directly", file.GetName(), file.Dependency[index])
			}
		}
		if err := c.checkFieldAnnotations(file); err != nil {
			return err
		}
	}
	return nil
}

func (c *catalog) checkFieldAnnotations(file *descriptorpb.FileDescriptorProto) error {
	var visit func(proto.Message, string) error
	visit = func(message proto.Message, path string) error {
		m := message.ProtoReflect()
		if err := c.checkDraftOptions(message, path); err != nil {
			return err
		}
		var err error
		m.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if field.Message() == nil {
				return true
			}
			path := path + "." + string(field.Name())
			if field.IsList() {
				list := value.List()
				for index := 0; index < list.Len(); index++ {
					if err = visit(list.Get(index).Message().Interface(), fmt.Sprintf("%s[%d]", path, index)); err != nil {
						return false
					}
				}
			} else {
				err = visit(value.Message().Interface(), path)
			}
			return err == nil
		})
		return err
	}
	return visit(file, file.GetName())
}

func (c *catalog) checkDraftOptions(message proto.Message, path string) error {
	if field, ok := message.(*descriptorpb.FieldDescriptorProto); ok && field.GetExtendee() != "" && len(c.draftValues(field.Options)) > 0 {
		return fmt.Errorf("%s: draft annotations are unsupported on extensions", path)
	}
	name := message.ProtoReflect().Descriptor().FullName()
	if !strings.HasSuffix(string(name), "Options") {
		return nil
	}
	values := c.draftValues(message)
	if len(values) == 0 {
		return nil
	}
	if name != "google.protobuf.FieldOptions" && name != "google.protobuf.MessageOptions" {
		return fmt.Errorf("%s: draft annotations are supported only on fields and messages", path)
	}
	for _, feature := range values {
		if feature == "" || strings.ContainsFunc(feature, unicode.IsSpace) {
			return fmt.Errorf("%s: draft feature must be a nonempty whitespace-free token", path)
		}
	}
	return nil
}
