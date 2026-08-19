package searchattribute

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
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
	*require.Assertions

	ctrl *gomock.Controller

	mockVisibilityManager *manager.MockVisibilityManager
	mockMetricsHandler    *metrics.MockHandler
}

func TestSearchAttributesValidatorSuite(t *testing.T) {
	suite.Run(t, &searchAttributesValidatorSuite{})
}

func (s *searchAttributesValidatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctrl = gomock.NewController(s.T())
	s.mockMetricsHandler = metrics.NewMockHandler(s.ctrl)
	s.mockVisibilityManager = manager.NewMockVisibilityManager(s.ctrl)
	s.mockVisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()
	s.mockVisibilityManager.EXPECT().
		ValidateCustomSearchAttributes(gomock.Any()).
		DoAndReturn(
			func(searchAttributes map[string]any) (map[string]any, error) {
				return searchAttributes, nil
			},
		).
		AnyTimes()
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
		true,  // allowList
		false, // suppressErrorSetSystemSearchAttribute
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

	mockCounter := metrics.NewMockCounterIface(s.ctrl)
	mockCounter.EXPECT().Record(
		int64(1),
		gomock.Any(),
		metrics.StringTag("search_attribute_type", "Int"),
		metrics.StringTag("allow_list", "true"),
	)
	s.mockMetricsHandler.EXPECT().Counter(invalidListValues.Name()).Return(mockCounter)
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

func (s *searchAttributesValidatorSuite) TestSearchAttributesValidate_EmitInvalidListValues() {
	intArrayPayload, err := payload.Encode([]int{1, 2})
	s.NoError(err)

	s.Run("AllowListFalse_RecordsMetricAndReturnsError", func() {
		saValidator := s.newValidator(
			NewTestProvider(),
			NewTestMapperProvider(nil),
			false, // allowList
			false,
		)

		namespace := "namespace"
		attr := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"Int01": intArrayPayload,
			},
		}

		mockCounter := metrics.NewMockCounterIface(s.ctrl)
		mockCounter.EXPECT().Record(
			int64(1),
			metrics.NamespaceTag(namespace),
			metrics.StringTag("search_attribute_type", "Int"),
			metrics.StringTag("allow_list", "false"),
		)
		s.mockMetricsHandler.EXPECT().Counter(invalidListValues.Name()).Return(mockCounter)

		err := saValidator.Validate(attr, namespace)
		s.Error(err)
		s.Equal("invalid value for search attribute Int01 of type Int: [1 2]", err.Error())
	})

	s.Run("KeywordListType_DoesNotRecordMetric", func() {
		saValidator := s.newValidator(
			NewTestProvider(),
			NewTestMapperProvider(nil),
			true,
			false,
		)

		namespace := "namespace"
		attr := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"KeywordList01": sadefs.MustEncodeValue(
					[]string{"a", "b"},
					enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
				),
			},
		}

		// No metric expectation - the mock would fail if Counter were called.
		err := saValidator.Validate(attr, namespace)
		s.NoError(err)
	})

	s.Run("EmptyList_DoesNotRecordMetric", func() {
		saValidator := s.newValidator(
			NewTestProvider(),
			NewTestMapperProvider(nil),
			true,
			false,
		)

		emptyArrayPayload, err := payload.Encode([]int{})
		s.NoError(err)

		namespace := "namespace"
		attr := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"Int01": emptyArrayPayload,
			},
		}

		// No metric expectation - empty list is acceptable for backwards
		// compatibility to unset a custom search attribute.
		err = saValidator.Validate(attr, namespace)
		s.NoError(err)
	})

	s.Run("ScalarValue_DoesNotRecordMetric", func() {
		saValidator := s.newValidator(
			NewTestProvider(),
			NewTestMapperProvider(nil),
			true,
			false,
		)

		intPayload, err := payload.Encode(1)
		s.NoError(err)

		namespace := "namespace"
		attr := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"Int01": intPayload,
			},
		}

		// No metric expectation - scalar JSON values must not be treated as lists.
		err = saValidator.Validate(attr, namespace)
		s.NoError(err)
	})

	s.Run("KeywordStringContainingBracket_DoesNotRecordMetric", func() {
		saValidator := s.newValidator(
			NewTestProvider(),
			NewTestMapperProvider(nil),
			true,
			false,
		)

		namespace := "namespace"
		attr := &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				// JSON-encoded string starts with `"`, not `[`.
				"Keyword01": payload.EncodeString("[abc]"),
			},
		}

		// No metric expectation - the leading `"` means this is not a list.
		err := saValidator.Validate(attr, namespace)
		s.NoError(err)
	})
}

func TestIsNonEmptyListValues(t *testing.T) {
	jsonMeta := map[string][]byte{
		converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON),
	}
	binaryMeta := map[string][]byte{
		converter.MetadataEncoding: []byte("binary/plain"),
	}

	tests := []struct {
		name    string
		payload *commonpb.Payload
		want    bool
	}{
		{
			name:    "nil payload",
			payload: nil,
			want:    false,
		},
		{
			name:    "non-JSON encoding",
			payload: &commonpb.Payload{Metadata: binaryMeta, Data: []byte("[1,2]")},
			want:    false,
		},
		{
			name:    "missing encoding metadata",
			payload: &commonpb.Payload{Data: []byte("[1,2]")},
			want:    false,
		},
		{
			name:    "empty data",
			payload: &commonpb.Payload{Metadata: jsonMeta, Data: nil},
			want:    false,
		},
		{
			name:    "empty list",
			payload: &commonpb.Payload{Metadata: jsonMeta, Data: []byte("[]")},
			want:    false,
		},
		{
			name:    "scalar int",
			payload: &commonpb.Payload{Metadata: jsonMeta, Data: []byte("1")},
			want:    false,
		},
		{
			name:    "scalar string containing bracket",
			payload: &commonpb.Payload{Metadata: jsonMeta, Data: []byte(`"[abc]"`)},
			want:    false,
		},
		{
			name:    "json null",
			payload: &commonpb.Payload{Metadata: jsonMeta, Data: []byte("null")},
			want:    false,
		},
		{
			name:    "non-empty list of ints",
			payload: &commonpb.Payload{Metadata: jsonMeta, Data: []byte("[1,2]")},
			want:    true,
		},
		{
			name:    "non-empty list of strings",
			payload: &commonpb.Payload{Metadata: jsonMeta, Data: []byte(`["a","b"]`)},
			want:    true,
		},
		{
			name:    "non-empty list with single element",
			payload: &commonpb.Payload{Metadata: jsonMeta, Data: []byte("[1]")},
			want:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isNonEmptyListValues(tc.payload))
		})
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
