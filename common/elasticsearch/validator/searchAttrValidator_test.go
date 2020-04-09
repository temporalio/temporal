package validator

import (
	"testing"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common"

	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type searchAttributesValidatorSuite struct {
	suite.Suite
}

func TestSearchAttributesValidatorSuite(t *testing.T) {
	s := new(searchAttributesValidatorSuite)
	suite.Run(t, s)
}

func (s *searchAttributesValidatorSuite) TestValidateSearchAttributes() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	validator := NewSearchAttributesValidator(log.NewNoop(),
		dynamicconfig.GetMapPropertyFn(definition.GetDefaultIndexedKeys()),
		dynamicconfig.GetIntPropertyFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFilteredByNamespace(sizeOfTotalLimit))

	namespace := "namespace"
	var attr *commonpb.SearchAttributes

	err := validator.ValidateSearchAttributes(attr, namespace)
	s.Nil(err)

	fields := map[string][]byte{
		"CustomIntField": []byte("1"),
	}
	attr = &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Nil(err)

	fields = map[string][]byte{
		"CustomIntField":     []byte("1"),
		"CustomKeywordField": []byte("keyword"),
		"CustomBoolField":    []byte("true"),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("number of keys 3 exceed limit", err.Error())

	fields = map[string][]byte{
		"InvalidKey": []byte("1"),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("InvalidKey is not valid search attribute", err.Error())

	fields = map[string][]byte{
		"StartTime": []byte("1"),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("StartTime is read-only Temporal reservered attribute", err.Error())

	fields = map[string][]byte{
		"CustomKeywordField": []byte("123456"),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("size limit exceed for key CustomKeywordField", err.Error())

	fields = map[string][]byte{
		"CustomKeywordField": []byte("123"),
		"CustomStringField":  []byte("12"),
	}
	attr.IndexedFields = fields
	err = validator.ValidateSearchAttributes(attr, namespace)
	s.Equal("total size 40 exceed limit", err.Error())
}
