package util

import (
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func ConvertPathToCamel(input string) []string {
	var pathParts []string
	for path := range strings.SplitSeq(input, ".") {
		// Split by "_" and convert each word to Title Case (CamelCase)
		var b strings.Builder
		j := 0
		for word := range strings.SplitSeq(path, "_") {
			if j > 0 {
				b.WriteString(cases.Title(language.Und).String(strings.ToLower(word)))
			} else {
				// lowercase the first letter
				if len(word) > 0 {
					b.WriteString(strings.ToLower(word[:1]))
					if len(word) > 1 {
						b.WriteString(word[1:])
					}
				}
			}
			j++
		}
		// Join the words into a CamelCase substring
		pathParts = append(pathParts, b.String())
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
