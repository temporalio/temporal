package searchattribute

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

// AreKeywordListPayloadsEqual compares two payloads assumed to be of type KeywordList.
// Returns true if both are nil or decode to equal multisets (order-insensitive, counts match).
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

	if len(keywordListA) != len(keywordListB) {
		return false
	}

	counts := make(map[string]int, len(keywordListA))
	for _, v := range keywordListA {
		counts[v]++
	}
	for _, v := range keywordListB {
		c := counts[v]
		if c == 0 {
			return false
		}
		if c == 1 {
			delete(counts, v)
		} else {
			counts[v] = c - 1
		}
	}
	return len(counts) == 0
}
