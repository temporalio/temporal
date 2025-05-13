package searchattribute

import (
	"testing"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.uber.org/mock/gomock"
)

type searchAttributesValidatorSuite struct {
	suite.Suite

	mockVisibilityManager *manager.MockVisibilityManager
}

func TestSearchAttributesValidatorSuite(t *testing.T) {
	ctrl := gomock.NewController(t)
	s := &searchAttributesValidatorSuite{
		mockVisibilityManager: manager.NewMockVisibilityManager(ctrl),
	}
	s.mockVisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()
	s.mockVisibilityManager.EXPECT().
		ValidateCustomSearchAttributes(gomock.Any()).
		DoAndReturn(
			func(searchAttributes map[string]any) (map[string]any, error) {
				return searchAttributes, nil
			},
		).
		AnyTimes()
	suite.Run(t, s)
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidate() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	saValidator := NewValidator(
		NewTestProvider(),
		NewTestMapperProvider(nil),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfTotalLimit),
		s.mockVisibilityManager,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		log.NewMockLogger(gomock.NewController(s.T())),
	)

	namespace := "namespace"
	var attr *commonpb.SearchAttributes

	err := saValidator.Validate(attr, namespace)
	s.NoError(err)

	intPayload, err := payload.Encode(1)
	s.NoError(err)
	fields := map[string]*commonpb.Payload{
		"CustomIntField": intPayload,
	}
	attr = &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
	err = saValidator.Validate(attr, namespace)
	s.NoError(err)

	fields = map[string]*commonpb.Payload{
		"CustomIntField":     intPayload,
		"CustomKeywordField": payload.EncodeString("keyword"),
		"CustomBoolField":    payload.EncodeString("true"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace)
	s.Error(err)
	s.Equal("number of search attributes 3 exceeds limit 2", err.Error())

	fields = map[string]*commonpb.Payload{
		"InvalidKey": payload.EncodeString("1"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace)
	s.Error(err)
	s.Equal("search attribute InvalidKey is not defined", err.Error())

	fields = map[string]*commonpb.Payload{
		"CustomTextField": payload.EncodeString("1"),
		"CustomBoolField": payload.EncodeString("123"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace)
	s.Error(err)
	s.Equal("invalid value for search attribute CustomBoolField of type Bool: 123", err.Error())

	intArrayPayload, err := payload.Encode([]int{1, 2})
	s.NoError(err)
	fields = map[string]*commonpb.Payload{
		"CustomIntField": intArrayPayload,
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace)
	s.NoError(err)

	fields = map[string]*commonpb.Payload{
		"StartTime": intPayload,
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace)
	s.Error(err)
	s.Equal("StartTime attribute can't be set in SearchAttributes", err.Error())

	mockLogger, ok := saValidator.logger.(*log.MockLogger)
	s.True(ok)
	mockLogger.EXPECT().Warn("Setting BuildIDs as a SearchAttribute is invalid and should be avoided.").Times(1)

	saPayload, err := EncodeValue([]string{"a"}, enumspb.INDEXED_VALUE_TYPE_TEXT)
	s.NoError(err)
	attr.IndexedFields = map[string]*commonpb.Payload{
		"BuildIds": saPayload,
	}
	err = saValidator.Validate(attr, namespace)
	s.NoError(err)
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidate_SuppressError() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	saValidator := NewValidator(
		NewTestProvider(),
		NewTestMapperProvider(nil),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfTotalLimit),
		s.mockVisibilityManager,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		log.NewMockLogger(gomock.NewController(s.T())),
	)

	namespace := "namespace"
	attr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"ParentWorkflowId": payload.EncodeString("foo"),
		},
	}

	err := saValidator.Validate(attr, namespace)
	s.NoError(err)
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidate_Mapper() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	saValidator := NewValidator(
		NewTestProvider(),
		NewTestMapperProvider(&TestMapper{}),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfTotalLimit),
		s.mockVisibilityManager,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		log.NewMockLogger(gomock.NewController(s.T())),
	)

	namespace := "test-namespace"
	var attr *commonpb.SearchAttributes

	err := saValidator.Validate(attr, namespace)
	s.Nil(err)

	intPayload, err := payload.Encode(1)
	s.NoError(err)
	fields := map[string]*commonpb.Payload{
		"CustomIntField": intPayload,
	}
	attr = &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
	err = saValidator.Validate(attr, namespace)
	s.NoError(err)

	fields = map[string]*commonpb.Payload{
		"CustomIntField": intPayload,
	}
	attr = &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
	err = saValidator.Validate(attr, "test-namespace")
	s.NoError(err)

	fields = map[string]*commonpb.Payload{
		"InvalidKey": payload.EncodeString("1"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace)
	s.Error(err)
	s.Equal("search attribute AliasForInvalidKey is not defined", err.Error())

	err = saValidator.Validate(attr, "error-namespace")
	s.Error(err)
	s.EqualError(err, "mapper error")

	fields = map[string]*commonpb.Payload{
		"CustomTextField": payload.EncodeString("1"),
		"CustomBoolField": payload.EncodeString("123"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace)
	s.Error(err)
	s.Equal("invalid value for search attribute AliasForCustomBoolField of type Bool: 123", err.Error())
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidateSize() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	saValidator := NewValidator(
		NewTestProvider(),
		NewTestMapperProvider(nil),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfTotalLimit),
		s.mockVisibilityManager,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		log.NewMockLogger(gomock.NewController(s.T())),
	)

	namespace := "namespace"

	fields := map[string]*commonpb.Payload{
		"CustomKeywordField": payload.EncodeString("123456"),
	}
	attr := &commonpb.SearchAttributes{
		IndexedFields: fields,
	}

	attr.IndexedFields = fields
	err := saValidator.ValidateSize(attr, namespace)
	s.Error(err)
	s.Equal("search attribute CustomKeywordField value size 8 exceeds size limit 5", err.Error())

	fields = map[string]*commonpb.Payload{
		"CustomKeywordField": payload.EncodeString("123"),
		"CustomTextField":    payload.EncodeString("12"),
	}
	attr.IndexedFields = fields
	err = saValidator.ValidateSize(attr, namespace)
	s.Error(err)
	s.Equal("total size of search attributes 106 exceeds size limit 20", err.Error())
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidateSize_Mapper() {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	saValidator := NewValidator(
		NewTestProvider(),
		NewTestMapperProvider(&TestMapper{}),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfTotalLimit),
		s.mockVisibilityManager,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		log.NewMockLogger(gomock.NewController(s.T())),
	)

	namespace := "test-namespace"

	fields := map[string]*commonpb.Payload{
		"CustomKeywordField": payload.EncodeString("123456"),
	}
	attr := &commonpb.SearchAttributes{
		IndexedFields: fields,
	}

	attr.IndexedFields = fields
	err := saValidator.ValidateSize(attr, namespace)
	s.Error(err)
	s.Equal("search attribute AliasForCustomKeywordField value size 8 exceeds size limit 5", err.Error())

	fields = map[string]*commonpb.Payload{
		"CustomKeywordField": payload.EncodeString("123"),
		"CustomTextField":    payload.EncodeString("12"),
	}
	attr.IndexedFields = fields
	err = saValidator.ValidateSize(attr, namespace)
	s.Error(err)
	s.Equal("total size of search attributes 106 exceeds size limit 20", err.Error())
}
