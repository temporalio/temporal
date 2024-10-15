// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package util

import (
	"fmt"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func splitPath(path string) []string {
	words := strings.Split(path, "_")
	if len(words) == 0 {
		words = strings.Split(path, ".")
	}
	for i, word := range words {
		// Convert first rune to upper and the rest to lower case
		words[i] = cases.Title(language.Und).String(strings.ToLower(word))
	}
	return words
}

// ApplyFieldMask applies the FieldMask to update a protobuf message using reflection.
func ApplyFieldMask(target proto.Message, msg proto.Message, mask *fieldmaskpb.FieldMask) {
	targetReflect := target.ProtoReflect()
	msgReflect := msg.ProtoReflect()

	for _, path := range mask.Paths {
		pathParts := splitPath(path)
		applyFieldMask(targetReflect, msgReflect, pathParts)
	}
}

// Recursive helper to apply field updates for nested fields.
// note - this is a limited version, it doesn't support maps/lists
func applyFieldMask(target, msg protoreflect.Message, pathParts []string) {
	if len(pathParts) == 0 {
		return
	}

	fieldName := pathParts[0]
	fieldDescriptor := target.Descriptor().Fields().ByName(protoreflect.Name(fieldName))

	// If the field does not exist in the descriptor, return early
	if fieldDescriptor == nil {
		fmt.Printf("Field %s does not exist in the message\n", fieldName)
		return
	}

	if len(pathParts) == 1 {
		// Base case: update the field value
		updateValue := msg.Get(fieldDescriptor)
		target.Set(fieldDescriptor, updateValue)
	} else {
		// Recursive case: drill down into nested fields
		if fieldDescriptor.Kind() == protoreflect.MessageKind {
			targetMessage := target.Mutable(fieldDescriptor).Message()
			updateMessage := msg.Get(fieldDescriptor).Message()
			applyFieldMask(targetMessage, updateMessage, pathParts[1:])
		}
	}
}
