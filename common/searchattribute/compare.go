package searchattribute

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

// AreKeywordListPayloadsEqual compares two payloads assumed to be of type KeywordList.
// Returns true if both are nil or decode to equal multisets (order-insensitive).
func AreKeywordListPayloadsEqual(a, b *commonpb.Payload) bool {
	if a == nil && b == nil {
		return true
	}
	// If exactly one is nil, they're not equal
	if (a == nil) != (b == nil) {
		return false
	}

	decodedA, err := DecodeValue(a, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
	if err != nil {
		return false
	}
	decodedB, err := DecodeValue(b, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
	if err != nil {
		return false
	}

	keywordListA, ok := decodedA.([]string)
	if !ok {
		return false
	}
	keywordListB, ok := decodedB.([]string)
	if !ok {
		return false
	}

	// Convert to sets to compare unique values only
	setA := make(map[string]bool, len(keywordListA))
	for _, v := range keywordListA {
		setA[v] = true
	}
	setB := make(map[string]bool, len(keywordListB))
	for _, v := range keywordListB {
		setB[v] = true
	}

	if len(setA) != len(setB) {
		return false
	}

	for v := range setA {
		if !setB[v] {
			return false
		}
	}
	return true
}
