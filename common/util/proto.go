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
