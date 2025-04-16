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
	"google.golang.org/protobuf/types/known/fieldmaskpb"
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

func ParseFieldMask(mask *fieldmaskpb.FieldMask) map[string]struct{} {
	updateFields := make(map[string]struct{})

	for _, path := range mask.Paths {
		pathParts := ConvertPathToCamel(path)
		jsonPath := strings.Join(pathParts, ".")
		updateFields[jsonPath] = struct{}{}
	}

	return updateFields
}
