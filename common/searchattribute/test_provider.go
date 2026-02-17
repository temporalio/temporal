package searchattribute

import (
	"fmt"
	"maps"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type (
	TestProvider struct{}

	TestMapper struct {
		Namespace            string
		WithCustomScheduleID bool
	}
)

var _ Provider = (*TestProvider)(nil)
var _ Mapper = (*TestMapper)(nil)

var (
	// testCustomSearchAttributes is the unified custom search attributes map for tests (DB index only).
	testCustomSearchAttributes = sadefs.GetDBIndexSearchAttributes(nil).CustomSearchAttributes

	// ScheduleId is mapped to Keyword10 for tests
	TestScheduleIDFieldName = "Keyword10"

	TestAliases = map[string]string{
		"Int01":         "CustomIntField",
		"Text01":        "CustomTextField",
		"Keyword01":     "CustomKeywordField",
		"Datetime01":    "CustomDatetimeField",
		"Double01":      "CustomDoubleField",
		"Bool01":        "CustomBoolField",
		"KeywordList01": "CustomKeywordListField",
	}

	// testAliasToField maps alias name to storage field name (reverse of TestAliases).
	testAliasToField = func() map[string]string {
		out := make(map[string]string, len(TestAliases))
		for field, alias := range TestAliases {
			out[alias] = field
		}
		return out
	}()
)

func TestNameTypeMap() NameTypeMap {
	csa := maps.Clone(testCustomSearchAttributes)
	return NewNameTypeMap(csa)
}

// TestNameTypeMapWithScheduleID returns a type map that includes ScheduleID as a custom attribute.
// Use only for tests that explicitly need "ScheduleID defined as custom search attribute" (e.g. schedule visibility with custom ScheduleId column).
func TestNameTypeMapWithScheduleID() NameTypeMap {
	csa := maps.Clone(testCustomSearchAttributes)
	csa[sadefs.ScheduleID] = enumspb.INDEXED_VALUE_TYPE_KEYWORD
	return NewNameTypeMap(csa)
}

func NewTestProvider() *TestProvider {
	return &TestProvider{}
}

func (s *TestProvider) GetSearchAttributes(_ string, _ bool) (NameTypeMap, error) {
	return TestNameTypeMap(), nil
}

func (t *TestMapper) GetAlias(fieldName string, namespace string) (string, error) {
	if fieldName == "wrong_field" {
		// This error must be always ignored.
		return "", serviceerror.NewInvalidArgument("unmapped field")
	}
	if namespace == "error-namespace" {
		return "", serviceerror.NewInvalidArgument(
			fmt.Sprintf("Namespace %s has no mapping defined for field name %s", namespace, fieldName),
		)
	}
	if namespace == "test-namespace" || namespace == t.Namespace {
		if fieldName == "pass-through" {
			return fieldName, nil
		}
		if t.WithCustomScheduleID && fieldName == TestScheduleIDFieldName {
			return sadefs.ScheduleID, nil
		}
		return "AliasFor" + fieldName, nil
	}

	// This error must be always ignored.
	return "", serviceerror.NewInvalidArgument("unknown namespace")
}

func (t *TestMapper) GetFieldName(alias string, namespace string) (string, error) {
	if alias == "wrong_alias" {
		// This error must be always ignored.
		return "", serviceerror.NewInvalidArgument("unmapped alias")
	}
	if namespace == "error-namespace" {
		return "", serviceerror.NewInvalidArgument(
			fmt.Sprintf("Namespace %s has no mapping defined for search attribute %s", namespace, alias),
		)
	} else if namespace == "test-namespace" || namespace == t.Namespace {
		if alias == "pass-through" {
			return alias, nil
		}
		if t.WithCustomScheduleID && alias == sadefs.ScheduleID {
			return TestScheduleIDFieldName, nil
		}
		var resolved string
		if strings.HasPrefix(alias, "AliasFor") {
			resolved = strings.TrimPrefix(alias, "AliasFor")
		} else if strings.HasPrefix(alias, "AliasWithHyphenFor-") {
			resolved = strings.TrimPrefix(alias, "AliasWithHyphenFor-")
		} else {
			resolved = alias
		}
		if fieldName, ok := testAliasToField[resolved]; ok {
			return fieldName, nil
		}
		return resolved, nil
	}
	return "", serviceerror.NewInvalidArgument("unknown namespace")
}

func NewNoopMapper() Mapper {
	return &noopMapper{}
}

func NewTestMapperProvider(customMapper Mapper) MapperProvider {
	return NewMapperProvider(customMapper, nil, NewTestProvider(), false)
}

func NewNameTypeMapStub(attributes map[string]enumspb.IndexedValueType) NameTypeMap {
	return NewNameTypeMap(attributes)
}
