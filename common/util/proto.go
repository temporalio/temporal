// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func ConvertPathToCamel(input string) []string {
	pathParts := strings.Split(input, ".")
	for i, path := range pathParts {
		// Split by "_" and convert each word to Title Case (CamelCase)
		words := strings.Split(path, "_")
		snakeCase := len(words) > 1
		for j, word := range words {
			if snakeCase && j > 0 {
				words[j] = cases.Title(language.Und).String(strings.ToLower(word))
			} else {
				// lowercase the first letter
				words[j] = strings.ToLower(word[:1]) + word[1:]
			}
		}
		// Join the words into a CamelCase substring
		pathParts[i] = strings.Join(words, "")
	}
	// Join all CamelCase substrings back with "."
	return pathParts
}

/*

!!!!!!!!!
protoreflect for some reason is not working well with niL pointers to messages.
Or at least I was not able to make it work.
msg.Get(fd).IsValid() is not returning false for nil values.
I will keep this code around for some time, but I will not use it.
I will remove it in the future.

// ApplyFieldMask applies the FieldMask to update a protobuf message using reflection.
func ApplyFieldMask(source proto.Message, target proto.Message, mask *fieldmaskpb.FieldMask) {
	if mask == nil {
		return
	}
	targetReflect := target.ProtoReflect()
	sourceReflect := source.ProtoReflect()

	for _, path := range mask.Paths {
		pathParts := convertPathToCamel(path)
		applyFieldMask(sourceReflect, targetReflect, pathParts)
	}
}

func setDefaultIfNil(msg protoreflect.Message, fd protoreflect.FieldDescriptor) {
	if !msg.Get(fd).IsValid() {
		msg.Set(fd, protoreflect.ValueOf(fd.Default()))
	}
}

// Recursive helper to apply field updates for nested fields.
// note - this is a limited version, it doesn't support maps/lists
func applyFieldMask(source, target protoreflect.Message, pathParts []string) {
	if len(pathParts) == 0 {
		return
	}

	fieldName := pathParts[0]
	fd := source.Descriptor().Fields().ByJSONName(fieldName)

	// If the field does not exist in the descriptor, return early
	if fd == nil {
		fmt.Printf("Field %s does not exist in the message\n", fieldName)
		return
	}

	if fd.Kind() == protoreflect.MessageKind {
		setDefaultIfNil(target, fd)
		setDefaultIfNil(source, fd)
	}

	if len(pathParts) == 1 {

		updateValue := source.Get(fd)
		target.Set(fd, updateValue)
	} else {
		// Recursive case: drill down into nested fields
		if fd.Kind() == protoreflect.MessageKind {
			sourceMessage := source.Mutable(fd).Message()
			targetMessage := target.Get(fd).Message()
			applyFieldMask(sourceMessage, targetMessage, pathParts[1:])
		}
	}
}

*/
