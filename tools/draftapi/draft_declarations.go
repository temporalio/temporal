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

// checkDeclarations validates draft feature names and containment in generated descriptors.
func (c *catalog) checkDeclarations(files map[string]*descriptorpb.FileDescriptorProto) error {
	for _, file := range files {
		// Option facts flow only through direct imports, so a public import would hide
		// annotations from the files that use it. Reject it as the generators do.
		for _, index := range file.PublicDependency {
			if int(index) < len(file.Dependency) && slices.ContainsFunc(c.options, func(opt option) bool { return opt.File == file.Dependency[index] }) {
				return fmt.Errorf("%s: public import of %s is not supported; import it directly", file.GetName(), file.Dependency[index])
			}
		}
		if err := c.checkAnnotations(file, file.GetName(), false); err != nil {
			return err
		}
	}
	return nil
}

func (c *catalog) checkAnnotations(message proto.Message, path string, inherited bool) error {
	if err := c.checkDraftOptions(message, path); err != nil {
		return err
	}
	m := message.ProtoReflect()
	if options := m.Descriptor().Fields().ByName("options"); options != nil && m.Has(options) &&
		c.feature(m.Get(options).Message().Interface()) != "" {
		if inherited {
			return fmt.Errorf("%s: draft members cannot declare their own feature", path)
		}
		inherited = true
	}

	var err error
	m.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Message() == nil {
			return true
		}
		childPath := path + "." + string(field.Name())
		if field.IsList() {
			err = c.checkAnnotationList(message, value.List(), childPath, inherited)
		} else {
			err = c.checkAnnotations(value.Message().Interface(), childPath, inherited)
		}
		return err == nil
	})
	return err
}

func (c *catalog) checkAnnotationList(parent proto.Message, list protoreflect.List, path string, inherited bool) error {
	for index := 0; index < list.Len(); index++ {
		child := list.Get(index).Message().Interface()
		childInherited := inherited
		if message, ok := parent.(*descriptorpb.DescriptorProto); ok && !inherited {
			if field, ok := child.(*descriptorpb.FieldDescriptorProto); ok && field.OneofIndex != nil {
				oneof := field.GetOneofIndex()
				if oneof >= 0 && int(oneof) < len(message.OneofDecl) {
					childInherited = c.feature(message.OneofDecl[oneof].Options) != ""
				}
			}
		}
		if err := c.checkAnnotations(child, fmt.Sprintf("%s[%d]", path, index), childInherited); err != nil {
			return err
		}
	}
	return nil
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
	if len(values) > 1 {
		return fmt.Errorf("%s: draft declarations must name one feature", path)
	}
	for _, feature := range values {
		if feature == "" || strings.ContainsFunc(feature, unicode.IsSpace) {
			return fmt.Errorf("%s: draft feature must be a nonempty whitespace-free token", path)
		}
	}
	return nil
}
