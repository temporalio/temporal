package searchattribute

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/mock/gomock"
)

type searchAttributesValidatorSuite struct {
	suite.Suite

	ctrl *gomock.Controller

	mockVisibilityManager *manager.MockVisibilityManager
	mockMetricsHandler    *metrics.MockHandler
}

func TestSearchAttributesValidatorSuite(t *testing.T) {
	ctrl := gomock.NewController(t)
	s := &searchAttributesValidatorSuite{
		ctrl: ctrl,

		mockVisibilityManager: manager.NewMockVisibilityManager(ctrl),
		mockMetricsHandler:    metrics.NewMockHandler(ctrl),
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

func (s *searchAttributesValidatorSuite) newValidator(
	searchAttributesProvider Provider,
	searchAttributesMapperProvider MapperProvider,
	allowList bool,
	suppressErrorSetSystemSearchAttribute bool,
) *Validator {
	numOfKeysLimit := 2
	sizeOfValueLimit := 5
	sizeOfTotalLimit := 20

	return NewValidator(
		searchAttributesProvider,
		searchAttributesMapperProvider,
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(numOfKeysLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfValueLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(sizeOfTotalLimit),
		s.mockVisibilityManager,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(allowList),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(suppressErrorSetSystemSearchAttribute),
		s.mockMetricsHandler,
		log.NewNoopLogger(),
	)
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidate() {
	saValidator := s.newValidator(
		NewTestProvider(),
		NewTestMapperProvider(nil),
		true,
		false,
	)

	namespace := "namespace"
	var attr *commonpb.SearchAttributes

	err := saValidator.Validate(attr, namespace)
	s.NoError(err)

	intPayload, err := payload.Encode(1)
	s.NoError(err)
	fields := map[string]*commonpb.Payload{
		"Int01": intPayload,
	}
	attr = &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
	err = saValidator.Validate(attr, namespace)
	s.NoError(err)

	fields = map[string]*commonpb.Payload{
		"Int01":     intPayload,
		"Keyword01": payload.EncodeString("keyword"),
		"Bool01":    payload.EncodeString("true"),
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
		"Text01": payload.EncodeString("1"),
		"Bool01": payload.EncodeString("123"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace)
	s.Error(err)
	s.Equal("invalid value for search attribute Bool01 of type Bool: 123", err.Error())

	intArrayPayload, err := payload.Encode([]int{1, 2})
	s.NoError(err)
	fields = map[string]*commonpb.Payload{
		"Int01": intArrayPayload,
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

	// Validate Deployment related search attributes
	deploymentRestrictedAttributes := []string{
		sadefs.TemporalWorkerDeploymentVersion,
		sadefs.TemporalWorkerDeployment,
		sadefs.TemporalWorkflowVersioningBehavior,
	}

	for _, restrictedAttr := range deploymentRestrictedAttributes {
		fields = map[string]*commonpb.Payload{
			restrictedAttr: payload.EncodeString("1.0.0"),
		}
		attr.IndexedFields = fields
		err = saValidator.Validate(attr, namespace)
		s.Error(err)
		s.Equal(fmt.Sprintf("%s attribute can't be set in SearchAttributes", restrictedAttr), err.Error())
	}
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidate_EmitAcceptedInvalidValueKeywordList() {
	saValidator := s.newValidator(
		NewTestProvider(),
		NewTestMapperProvider(nil),
		false,
		false,
	)

	namespace := "namespace"
	attr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"KeywordList01": sadefs.MustEncodeValue([]any{nil}, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
		},
	}

	mockCounter := metrics.NewMockCounterIface(s.ctrl)
	mockCounter.EXPECT().Record(int64(1), metrics.NamespaceTag(namespace))
	s.mockMetricsHandler.EXPECT().Counter(acceptedInvalidValueKeywordList.Name()).Return(mockCounter)

	err := saValidator.Validate(attr, namespace)
	s.NoError(err)
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidate_SuppressError() {
	saValidator := s.newValidator(
		NewTestProvider(),
		NewTestMapperProvider(nil),
		false,
		true,
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
	saValidator := s.newValidator(
		NewTestProvider(),
		NewTestMapperProvider(&TestMapper{}),
		false,
		false,
	)

	namespace := "test-namespace"
	var attr *commonpb.SearchAttributes

	err := saValidator.Validate(attr, namespace)
	s.Nil(err)

	intPayload, err := payload.Encode(1)
	s.NoError(err)
	fields := map[string]*commonpb.Payload{
		"Int01": intPayload,
	}
	attr = &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
	err = saValidator.Validate(attr, namespace)
	s.NoError(err)

	fields = map[string]*commonpb.Payload{
		"Int01": intPayload,
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
	s.Require().EqualError(err, "Namespace error-namespace has no mapping defined for field name InvalidKey")

	fields = map[string]*commonpb.Payload{
		"Text01": payload.EncodeString("1"),
		"Bool01": payload.EncodeString("123"),
	}
	attr.IndexedFields = fields
	err = saValidator.Validate(attr, namespace)
	s.Error(err)
	s.Equal("invalid value for search attribute AliasForBool01 of type Bool: 123", err.Error())
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidateSize() {
	saValidator := s.newValidator(
		NewTestProvider(),
		NewTestMapperProvider(nil),
		false,
		false,
	)

	namespace := "namespace"

	fields := map[string]*commonpb.Payload{
		"Keyword01": payload.EncodeString("123456"),
	}
	attr := &commonpb.SearchAttributes{
		IndexedFields: fields,
	}

	attr.IndexedFields = fields
	err := saValidator.ValidateSize(attr, namespace)
	s.Error(err)
	s.Equal("search attribute Keyword01 value size 8 exceeds size limit 5", err.Error())

	fields = map[string]*commonpb.Payload{
		"Keyword01": payload.EncodeString("123"),
		"Text01":    payload.EncodeString("12"),
	}
	attr.IndexedFields = fields
	err = saValidator.ValidateSize(attr, namespace)
	s.Error(err)
	s.Equal("total size of search attributes 88 exceeds size limit 20", err.Error())
}

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidateSize_Mapper() {
	saValidator := s.newValidator(
		NewTestProvider(),
		NewTestMapperProvider(&TestMapper{}),
		false,
		false,
	)

	namespace := "test-namespace"

	fields := map[string]*commonpb.Payload{
		"Keyword01": payload.EncodeString("123456"),
	}
	attr := &commonpb.SearchAttributes{
		IndexedFields: fields,
	}

	attr.IndexedFields = fields
	err := saValidator.ValidateSize(attr, namespace)
	s.Error(err)
	s.Equal("search attribute AliasForKeyword01 value size 8 exceeds size limit 5", err.Error())

	fields = map[string]*commonpb.Payload{
		"Keyword01": payload.EncodeString("123"),
		"Text01":    payload.EncodeString("12"),
	}
	attr.IndexedFields = fields
	err = saValidator.ValidateSize(attr, namespace)
	s.Error(err)
	s.Equal("total size of search attributes 88 exceeds size limit 20", err.Error())
}
