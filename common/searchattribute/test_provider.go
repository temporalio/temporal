package searchattribute

import (
	"fmt"
	"maps"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type (
	TestProvider struct {
		es bool
	}

	testMapperProvider struct {
		mapper Mapper
	}

	TestMapper struct {
		Namespace            string
		WithCustomScheduleID bool
	}
)

var _ Provider = (*TestProvider)(nil)
var _ Mapper = (*TestMapper)(nil)
var _ MapperProvider = (*testMapperProvider)(nil)

var (
	esCustomSearchAttributes = map[string]enumspb.IndexedValueType{
		"CustomIntField":      enumspb.INDEXED_VALUE_TYPE_INT,
		"CustomTextField":     enumspb.INDEXED_VALUE_TYPE_TEXT,
		"CustomKeywordField":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"CustomDatetimeField": enumspb.INDEXED_VALUE_TYPE_DATETIME,
		"CustomDoubleField":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"CustomBoolField":     enumspb.INDEXED_VALUE_TYPE_BOOL,
	}

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

func TestEsNameTypeMap() NameTypeMap {
	csa := maps.Clone(esCustomSearchAttributes)
	return NewNameTypeMap(csa)
}

func TestSearchAttributesToRegister() map[string]enumspb.IndexedValueType {
	return maps.Clone(esCustomSearchAttributes)
}

func TestEsNameTypeMapWithScheduleID() NameTypeMap {
	res := TestEsNameTypeMap()
	res.customSearchAttributes[sadefs.ScheduleID] = enumspb.INDEXED_VALUE_TYPE_KEYWORD
	return res
}

func NewTestProvider() *TestProvider {
	return &TestProvider{}
}

func NewTestEsProvider() *TestProvider {
	return &TestProvider{
		es: true,
	}
}

func (s *TestProvider) GetSearchAttributes(_ string, _ bool) (NameTypeMap, error) {
	if s.es {
		return TestEsNameTypeMap(), nil
	}
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
	switch namespace {
	case "error-namespace":
		return "", serviceerror.NewInvalidArgument(
			fmt.Sprintf("Namespace %s has no mapping defined for search attribute %s", namespace, alias),
		)
	case "test-namespace", t.Namespace:
		if alias == "pass-through" {
			return alias, nil
		}
		if t.WithCustomScheduleID && alias == sadefs.ScheduleID {
			return TestScheduleIDFieldName, nil
		}
		resolved := alias
		if after, ok := strings.CutPrefix(alias, "AliasFor"); ok {
			resolved = after
		} else if after, ok := strings.CutPrefix(alias, "AliasWithHyphenFor-"); ok {
			resolved = after
		}
		if fieldName, ok := testAliasToField[resolved]; ok {
			return fieldName, nil
		}
		return resolved, nil
	}
	return "", serviceerror.NewInvalidArgument("unknown namespace")
}

func NewTestMapperProvider(customMapper Mapper) MapperProvider {
	if customMapper == nil {
		customMapper = &NoopMapper{}
	}
	return &testMapperProvider{mapper: customMapper}
}

func (p *testMapperProvider) GetMapper(namespace.Name) (Mapper, error) {
	return p.mapper, nil
}

func NewNameTypeMapStub(attributes map[string]enumspb.IndexedValueType) NameTypeMap {
	return NewNameTypeMap(attributes)
}
