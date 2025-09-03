package searchattribute

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestAreKeywordListPayloadsEqual_TableDriven(t *testing.T) {
	r := require.New(t)
	createPayload := func(values []string) *commonpb.Payload {
		if values == nil {
			return nil
		}
		encoded, err := EncodeValue(values, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
		r.NoError(err)
		return encoded
	}

	long := strings.Repeat("very_long_string_", 100)

	cases := []struct {
		name     string
		a, b     *commonpb.Payload
		expected bool
	}{
		{"both_nil", nil, nil, true},
		{"a_nil_b_not_nil", nil, createPayload([]string{"test"}), false},
		{"a_not_nil_b_nil", createPayload([]string{"test"}), nil, false},
		{"both_empty", createPayload([]string{}), createPayload([]string{}), true},
		{"same_order", createPayload([]string{"a", "b", "c"}), createPayload([]string{"a", "b", "c"}), true},
		{"diff_order", createPayload([]string{"a", "b", "c"}), createPayload([]string{"c", "a", "b"}), true},
		{"different_elems", createPayload([]string{"a", "b", "c"}), createPayload([]string{"a", "b", "d"}), false},
		{"different_lengths", createPayload([]string{"a", "b"}), createPayload([]string{"a", "b", "c"}), false},
		{"duplicates_a", createPayload([]string{"a", "a", "b"}), createPayload([]string{"a", "b"}), false},
		{"duplicates_b", createPayload([]string{"a", "b"}), createPayload([]string{"a", "a", "b"}), false},
		{"single_same", createPayload([]string{"test"}), createPayload([]string{"test"}), true},
		{"single_different", createPayload([]string{"test1"}), createPayload([]string{"test2"}), false},
		{"case_sensitive", createPayload([]string{"Test", "Value"}), createPayload([]string{"test", "value"}), false},
		{"special_same", createPayload([]string{"test@123", "value#456", "key$789"}), createPayload([]string{"test@123", "value#456", "key$789"}), true},
		{"special_diff", createPayload([]string{"test@123", "value#456"}), createPayload([]string{"test@123", "value%456"}), false},
		{"empty_strings", createPayload([]string{"", "test", ""}), createPayload([]string{"test", "", ""}), true},
		{"unicode_same", createPayload([]string{"测试", "тест", "test"}), createPayload([]string{"test", "测试", "тест"}), true},
		{"unicode_diff", createPayload([]string{"测试", "тест"}), createPayload([]string{"测试", "тест", "test"}), false},
		{"whitespace_diff", createPayload([]string{" test ", "value"}), createPayload([]string{"test", "value"}), false},
		{"numbers_same", createPayload([]string{"123", "456", "789"}), createPayload([]string{"789", "123", "456"}), true},
		{"numbers_diff", createPayload([]string{"123", "456"}), createPayload([]string{"123", "456", "789"}), false},
		{"very_long_strings", createPayload([]string{long, "normal"}), createPayload([]string{"normal", long}), true},
	}

	for _, tc := range cases {
		// capture
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := AreKeywordListPayloadsEqual(tc.a, tc.b)
			r.Equal(tc.expected, got)
		})
	}

	// Large list scenario (kept separate for readability)
	t.Run("very_large_lists_equal", func(t *testing.T) {
		largeListA := make([]string, 1000)
		largeListB := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			largeListA[i] = fmt.Sprintf("item_%d", i)
			largeListB[999-i] = fmt.Sprintf("item_%d", i)
		}
		a := createPayload(largeListA)
		b := createPayload(largeListB)
		r.True(AreKeywordListPayloadsEqual(a, b))
	})

	// Non-keyword payloads should not be considered equal to keyword-list payloads
	t.Run("non_keyword_payloads", func(t *testing.T) {
		nonKeyword := &commonpb.Payload{Data: []byte("not a keyword list")}
		valid := createPayload([]string{"test"})
		r.False(AreKeywordListPayloadsEqual(nonKeyword, valid))
		r.False(AreKeywordListPayloadsEqual(valid, nonKeyword))
		r.False(AreKeywordListPayloadsEqual(nonKeyword, nonKeyword))
	})
}

func TestAreKeywordListPayloadsEqual_Combinatorial(t *testing.T) {
	r := require.New(t)
	createPayload := func(values []string) *commonpb.Payload {
		if values == nil {
			return nil
		}
		encoded, err := EncodeValue(values, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
		r.NoError(err)
		return encoded
	}

	testSets := [][]string{
		nil,
		{},
		{"a"},
		{"a", "b"},
		{"a", "b", "c"},
		{"a", "a", "b"},
		{"", "test", ""},
		{"test@123", "value#456"},
		{"测试", "тест", "test"},
		{"123", "456", "789"},
	}

	for i, setA := range testSets {
		for j, setB := range testSets {
			payloadA := createPayload(setA)
			payloadB := createPayload(setB)

			expected := false
			if setA == nil && setB == nil {
				expected = true
			} else if setA != nil && setB != nil {
				mapA := make(map[string]int)
				mapB := make(map[string]int)
				for _, v := range setA {
					mapA[v]++
				}
				for _, v := range setB {
					mapB[v]++
				}
				expected = reflect.DeepEqual(mapA, mapB)
			}

			name := fmt.Sprintf("combo_%d_%d", i, j)
			t.Run(name, func(t *testing.T) {
				got := AreKeywordListPayloadsEqual(payloadA, payloadB)
				r.Equal(expected, got)
			})
		}
	}
}
