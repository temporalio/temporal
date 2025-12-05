package frontend

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/worker/deletenamespace"
	delnserrors "go.temporal.io/server/service/worker/deletenamespace/errors"
	"go.uber.org/mock/gomock"
	expmaps "golang.org/x/exp/maps"
	"google.golang.org/grpc/health"
)

var (
	testNamespace = "test-namespace"
	testIndexName = "test-index-name"
)

type (
	operatorHandlerSuite struct {
		suite.Suite

		controller   *gomock.Controller
		mockResource *resourcetest.Test

		handler *OperatorHandlerImpl
	}
)

func TestOperatorHandlerSuite(t *testing.T) {
	s := new(operatorHandlerSuite)
	suite.Run(t, s)
}

func (s *operatorHandlerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockResource = resourcetest.NewTest(s.controller, primitives.FrontendService)
	s.mockResource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(uuid.NewString()).AnyTimes()

	endpointClient := newNexusEndpointClient(
		newNexusEndpointClientConfig(dynamicconfig.NewNoopCollection()),
		s.mockResource.NamespaceCache,
		s.mockResource.MatchingClient,
		s.mockResource.NexusEndpointManager,
		s.mockResource.Logger,
	)

	args := NewOperatorHandlerImplArgs{
		&Config{NumHistoryShards: 4},
		s.mockResource.Logger,
		s.mockResource.GetSDKClientFactory(),
		s.mockResource.GetMetricsHandler(),
		s.mockResource.GetVisibilityManager(),
		s.mockResource.GetSearchAttributesManager(),
		health.NewServer(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetClusterMetadataManager(),
		s.mockResource.GetClusterMetadata(),
		s.mockResource.GetClientFactory(),
		s.mockResource.NamespaceCache,
		endpointClient,
	}
	s.handler = NewOperatorHandlerImpl(args)
	s.handler.Start()
}

func (s *operatorHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.handler.Stop()
}

func (s *operatorHandlerSuite) Run(name string, subtest func()) bool {
	oldController := s.controller
	oldMockResource := s.mockResource
	oldHandler := s.handler

	return s.Suite.Run(name, func() {
		s.SetupTest()
		defer func() {
			s.TearDownTest()
			s.controller = oldController
			s.mockResource = oldMockResource
			s.handler = oldHandler
		}()

		subtest()
	})
}

func (s *operatorHandlerSuite) Test_AddSearchAttributes() {
	ctx := context.Background()
	testCases := []struct {
		name                      string
		request                   *operatorservice.AddSearchAttributesRequest
		storeNames                []string
		indexName                 string
		getSearchAttributesCalled bool
		getSearchAttributesErr    error
		addInternalSuccess        bool
		expectedErrMsg            string
	}{
		{
			name:           "fail: request is nil",
			request:        nil,
			expectedErrMsg: errRequestNotSet.Error(),
		},
		{
			name:           "fail: search attributes not set in request",
			request:        &operatorservice.AddSearchAttributesRequest{},
			expectedErrMsg: errSearchAttributesNotSet.Error(),
		},
		{
			name: "fail: cannot add reserved name search attribute",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			expectedErrMsg: fmt.Sprintf(errSearchAttributeIsReservedMessage, "WorkflowId"),
		},
		{
			name: "fail: unknown search attribute type",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.IndexedValueType(-1),
				},
			},
			expectedErrMsg: fmt.Sprintf(errUnknownSearchAttributeTypeMessage, -1),
		},
		{
			name: "fail: cannot get existing search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeNames:                []string{elasticsearch.PersistenceName},
			getSearchAttributesCalled: true,
			getSearchAttributesErr:    errors.New("mock error get search attributes"),
			expectedErrMsg:            "mock error get search attributes",
		},
		{
			name: "success",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeNames:                []string{elasticsearch.PersistenceName},
			indexName:                 testIndexName,
			getSearchAttributesCalled: true,
			addInternalSuccess:        true,
			expectedErrMsg:            "",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.mockResource.VisibilityManager.EXPECT().GetStoreNames().Return(tc.storeNames).AnyTimes()
			s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(tc.indexName).AnyTimes()

			saTypeMap := searchattribute.TestNameTypeMap()
			if len(tc.storeNames) > 0 && tc.storeNames[0] == elasticsearch.PersistenceName {
				saTypeMap = searchattribute.TestEsNameTypeMap()
			}

			if tc.getSearchAttributesCalled {
				s.mockResource.SearchAttributesManager.EXPECT().
					GetSearchAttributes(tc.indexName, true).
					Return(saTypeMap, tc.getSearchAttributesErr)
			}

			if tc.addInternalSuccess {
				s.mockResource.VisibilityManager.EXPECT().
					AddSearchAttributes(
						gomock.Any(),
						&manager.AddSearchAttributesRequest{SearchAttributes: tc.request.SearchAttributes},
					).
					Return(nil)
				s.mockResource.SearchAttributesManager.EXPECT().
					SaveSearchAttributes(
						gomock.Any(),
						tc.indexName,
						gomock.Any(),
					).
					Return(nil)
			}

			_, err := s.handler.AddSearchAttributes(ctx, tc.request)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_AddSearchAttributes_DualVisibility() {
	ctx := context.Background()
	testCases := []struct {
		name              string
		request           *operatorservice.AddSearchAttributesRequest
		addVisManager1    bool
		addVisManager1Err error
		addVisManager2    bool
		addVisManager2Err error
		expectedErrMsg    string
	}{
		{
			name: "success",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			addVisManager1: true,
			addVisManager2: true,
		},
		{
			name: "fail: failed to add search attributes to visibility manager 1",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			addVisManager1:    true,
			addVisManager1Err: errors.New("mock error add vis manager 1"),
			expectedErrMsg:    "mock error add vis manager 1",
		},
		{
			name: "fail: failed to add search attributes to visibility manager 2",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			addVisManager1:    true,
			addVisManager2:    true,
			addVisManager2Err: errors.New("mock error add vis manager 2"),
			expectedErrMsg:    "mock error add vis manager 2",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			mockVisManager1 := manager.NewMockVisibilityManager(s.controller)
			mockVisManager2 := manager.NewMockVisibilityManager(s.controller)
			mockManagerSelector := visibility.NewMockmanagerSelector(s.controller)
			mockDualVisManager := visibility.NewVisibilityManagerDual(
				mockVisManager1,
				mockVisManager2,
				mockManagerSelector,
				dynamicconfig.GetBoolPropertyFn(false),
			)
			s.handler.visibilityMgr = mockDualVisManager

			testIndexName1 := testIndexName + "-1"
			testIndexName2 := testIndexName + "-2"
			mockVisManager1.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName}).AnyTimes()
			mockVisManager1.EXPECT().GetIndexName().Return(testIndexName1).AnyTimes()
			mockVisManager2.EXPECT().GetStoreNames().Return([]string{mysql.PluginName}).AnyTimes()
			mockVisManager2.EXPECT().GetIndexName().Return(testIndexName2).AnyTimes()

			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(testIndexName1, true).
				Return(searchattribute.TestNameTypeMap(), nil).
				AnyTimes()

			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(testIndexName2, true).
				Return(searchattribute.TestNameTypeMap(), nil).
				AnyTimes()

			if tc.addVisManager1 {
				mockVisManager1.EXPECT().AddSearchAttributes(
					gomock.Any(),
					&manager.AddSearchAttributesRequest{SearchAttributes: tc.request.SearchAttributes},
				).Return(nil)
				s.mockResource.SearchAttributesManager.EXPECT().
					SaveSearchAttributes(gomock.Any(), testIndexName1, gomock.Any()).
					Return(tc.addVisManager1Err)
			}

			if tc.addVisManager2 {
				s.mockResource.ClientFactory.EXPECT().
					NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
					Return(nil, s.mockResource.GetFrontendClient(), nil)
				s.mockResource.FrontendClient.EXPECT().
					DescribeNamespace(
						gomock.Any(),
						&workflowservice.DescribeNamespaceRequest{Namespace: testNamespace},
					).
					Return(
						&workflowservice.DescribeNamespaceResponse{
							Config: &namespacepb.NamespaceConfig{
								CustomSearchAttributeAliases: searchattribute.TestAliases,
							},
						},
						nil,
					)
				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(gomock.Any(), gomock.Any()).
					Return(&workflowservice.UpdateNamespaceResponse{}, tc.addVisManager2Err)
			}

			_, err := s.handler.AddSearchAttributes(ctx, tc.request)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_AddSearchAttributesInternal() {
	ctx := context.Background()
	testCases := []struct {
		name                   string
		request                *operatorservice.AddSearchAttributesRequest
		storeName              string
		indexName              string
		getSearchAttributesErr error
		addEsCalled            bool
		addEsSchemaErr         error
		addEsMetadataErr       error
		addSqlCalled           bool
		addSqlErr              error
		expectedErrMsg         string
	}{
		{
			name: "fail: cannot get search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeName:              elasticsearch.PersistenceName,
			indexName:              testIndexName,
			getSearchAttributesErr: errors.New("mock error get search attributes"),
			expectedErrMsg:         "Unable to get search attributes: mock error get search attributes",
		},
		{
			name: "fail: cannot add search attributes to elasticsearch visibility",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeName:      elasticsearch.PersistenceName,
			indexName:      testIndexName,
			addEsCalled:    true,
			addEsSchemaErr: errors.New("mock error add es schema"),
			expectedErrMsg: "mock error add es schema",
		},
		{
			name: "fail: cannot save search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeName:        elasticsearch.PersistenceName,
			indexName:        testIndexName,
			addEsCalled:      true,
			addEsMetadataErr: errors.New("mock error add es metadata"),
			expectedErrMsg:   "mock error add es metadata",
		},
		{
			name: "success: add search attributes to elasticsearch visibility",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeName:      elasticsearch.PersistenceName,
			indexName:      testIndexName,
			addEsCalled:    true,
			expectedErrMsg: "",
		},
		{
			name: "fail: cannot add search attributes to sql visibility",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			storeName:      mysql.PluginName,
			indexName:      testIndexName,
			addSqlCalled:   true,
			addSqlErr:      errors.New("mock error add sql mapping"),
			expectedErrMsg: "mock error add sql mapping",
		},
		{
			name: "success: add search attributes to sql visibility",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			storeName:      mysql.PluginName,
			indexName:      testIndexName,
			addSqlCalled:   true,
			expectedErrMsg: "",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			saTypeMap := searchattribute.TestNameTypeMap()
			if tc.storeName == elasticsearch.PersistenceName {
				saTypeMap = searchattribute.TestEsNameTypeMap()
			}

			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(tc.indexName, true).
				Return(saTypeMap, tc.getSearchAttributesErr)
			s.mockResource.VisibilityManager.EXPECT().GetStoreNames().Return([]string{tc.storeName})
			s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(tc.indexName)

			if tc.addEsCalled {
				s.mockResource.VisibilityManager.EXPECT().AddSearchAttributes(
					gomock.Any(),
					&manager.AddSearchAttributesRequest{SearchAttributes: tc.request.SearchAttributes},
				).Return(tc.addEsSchemaErr)

				if tc.addEsSchemaErr == nil {
					expectedNewCustomSearchAttributes := util.CloneMapNonNil(saTypeMap.Custom())
					maps.Copy(expectedNewCustomSearchAttributes, tc.request.SearchAttributes)
					s.mockResource.SearchAttributesManager.EXPECT().
						SaveSearchAttributes(gomock.Any(), tc.indexName, expectedNewCustomSearchAttributes).
						Return(tc.addEsMetadataErr)
				}
			}

			if tc.addSqlCalled {
				s.mockResource.ClientFactory.EXPECT().
					NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
					Return(nil, s.mockResource.GetFrontendClient(), nil)

				s.mockResource.FrontendClient.EXPECT().
					DescribeNamespace(
						gomock.Any(),
						&workflowservice.DescribeNamespaceRequest{Namespace: testNamespace},
					).
					Return(
						&workflowservice.DescribeNamespaceResponse{
							Config: &namespacepb.NamespaceConfig{
								CustomSearchAttributeAliases: searchattribute.TestAliases,
							},
						},
						nil,
					)

				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(gomock.Any(), gomock.Any()).
					Return(&workflowservice.UpdateNamespaceResponse{}, tc.addSqlErr)
			}

			err := s.handler.addSearchAttributesInternal(ctx, tc.request, s.mockResource.VisibilityManager)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_AddSearchAttributesElasticsearch() {
	ctx := context.Background()
	testCases := []struct {
		name                  string
		request               *operatorservice.AddSearchAttributesRequest
		passValidation        bool
		customAttributesToAdd map[string]enumspb.IndexedValueType
		addEsSchemaErr        error
		addEsMetadataErr      error
		expectedErrMsg        string
	}{
		{
			name: "success",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			passValidation: true,
			customAttributesToAdd: map[string]enumspb.IndexedValueType{
				"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			expectedErrMsg: "",
		},
		{
			name: "success: search attribute already exists",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			expectedErrMsg: "",
		},
		{
			name: "success: mix new and already exists search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr":         enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			passValidation: true,
			customAttributesToAdd: map[string]enumspb.IndexedValueType{
				"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			expectedErrMsg: "",
		},

		{
			name: "fail: cannot add elasticsearch schema",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			passValidation: true,
			customAttributesToAdd: map[string]enumspb.IndexedValueType{
				"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			addEsSchemaErr: errors.New("mock error add es schema"),
			expectedErrMsg: fmt.Sprintf(
				errUnableToSaveSearchAttributesMessage,
				errors.New("mock error add es schema"),
			),
		},
		{
			name: "fail: cannot save search attributes metadata",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			passValidation: true,
			customAttributesToAdd: map[string]enumspb.IndexedValueType{
				"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			addEsMetadataErr: errors.New("mock error save metadata"),
			expectedErrMsg: fmt.Sprintf(
				errUnableToSaveSearchAttributesMessage,
				errors.New("mock error save metadata"),
			),
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			saTypeMap := searchattribute.TestEsNameTypeMap()
			s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName)
			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(testIndexName, true).
				Return(saTypeMap, nil)

			if tc.passValidation {
				s.mockResource.VisibilityManager.EXPECT().AddSearchAttributes(
					gomock.Any(),
					&manager.AddSearchAttributesRequest{SearchAttributes: tc.customAttributesToAdd},
				).Return(tc.addEsSchemaErr)

				if tc.addEsSchemaErr == nil {
					expectedNewCustomSearchAttributes := util.CloneMapNonNil(saTypeMap.Custom())
					maps.Copy(expectedNewCustomSearchAttributes, tc.customAttributesToAdd)
					s.mockResource.SearchAttributesManager.EXPECT().
						SaveSearchAttributes(gomock.Any(), testIndexName, expectedNewCustomSearchAttributes).
						Return(tc.addEsMetadataErr)
				}
			}

			err := s.handler.addSearchAttributesElasticsearch(
				ctx,
				tc.request,
				s.mockResource.VisibilityManager,
			)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_AddSearchAttributesSQL() {
	ctx := context.Background()
	testCases := []struct {
		name                        string
		request                     *operatorservice.AddSearchAttributesRequest
		customSearchAttributesToAdd []string
		getFrontendClientErr        error
		describeNamespaceCalled     bool
		describeNamespaceErr        error
		updateNamespaceCalled       bool
		updateNamespaceErr          error
		expectedErrMsg              string
	}{
		{
			name: "success",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			customSearchAttributesToAdd: []string{"CustomAttr"},
			describeNamespaceCalled:     true,
			updateNamespaceCalled:       true,
			expectedErrMsg:              "",
		},
		{
			name: "success: search attribute already exists",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			describeNamespaceCalled: true,
			expectedErrMsg:          "",
		},
		{
			name: "success: mix new and already exists search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr":         enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			customSearchAttributesToAdd: []string{"CustomAttr"},
			describeNamespaceCalled:     true,
			updateNamespaceCalled:       true,
			expectedErrMsg:              "",
		},

		{
			name: "fail: cannot get frontend client",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			getFrontendClientErr: errors.New("mock error get frontend client"),
			expectedErrMsg:       "mock error get frontend client",
		},
		{
			name: "fail: namespace not set in request",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			expectedErrMsg: errNamespaceNotSet.Error(),
		},
		{
			name: "fail: cannot describe namespace",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			describeNamespaceCalled: true,
			describeNamespaceErr:    errors.New("mock error describe namespace"),
			expectedErrMsg:          fmt.Sprintf(errUnableToGetNamespaceInfoMessage, testNamespace, "mock error describe namespace"),
		},
		{
			name: "fail: too many search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					// there is already one keyword search attribute defined in TestNameTypeMap
					"CustomAttr1": enumspb.INDEXED_VALUE_TYPE_INT,
					"CustomAttr2": enumspb.INDEXED_VALUE_TYPE_INT,
					"CustomAttr3": enumspb.INDEXED_VALUE_TYPE_INT,
					"CustomAttr4": enumspb.INDEXED_VALUE_TYPE_INT,
				},
				Namespace: testNamespace,
			},
			describeNamespaceCalled: true,
			expectedErrMsg: fmt.Sprintf(
				errTooManySearchAttributesMessage,
				3,
				enumspb.INDEXED_VALUE_TYPE_INT,
			),
		},
		{
			name: "fail: cannot update namespace",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			customSearchAttributesToAdd: []string{"CustomAttr"},
			describeNamespaceCalled:     true,
			updateNamespaceCalled:       true,
			updateNamespaceErr:          errors.New("mock error update namespace"),
			expectedErrMsg: fmt.Sprintf(
				errUnableToSaveSearchAttributesMessage,
				"mock error update namespace",
			),
		},
		{
			name: "fail: update namespace race condition",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			customSearchAttributesToAdd: []string{"CustomAttr"},
			describeNamespaceCalled:     true,
			updateNamespaceCalled:       true,
			updateNamespaceErr:          errCustomSearchAttributeFieldAlreadyAllocated,
			expectedErrMsg:              errRaceConditionAddingSearchAttributes.Error(),
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			saTypeMap := searchattribute.TestNameTypeMap()
			s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName)
			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(testIndexName, true).
				Return(saTypeMap, nil)

			s.mockResource.ClientFactory.EXPECT().
				NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
				Return(nil, s.mockResource.GetFrontendClient(), tc.getFrontendClientErr)

			if tc.describeNamespaceCalled {
				s.mockResource.FrontendClient.EXPECT().
					DescribeNamespace(
						gomock.Any(),
						&workflowservice.DescribeNamespaceRequest{Namespace: testNamespace},
					).
					Return(
						&workflowservice.DescribeNamespaceResponse{
							Config: &namespacepb.NamespaceConfig{CustomSearchAttributeAliases: searchattribute.TestAliases},
						},
						tc.describeNamespaceErr,
					)
			}

			if tc.updateNamespaceCalled {
				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(gomock.Any(), gomock.Any()).
					DoAndReturn(func(
						ctx context.Context,
						r *workflowservice.UpdateNamespaceRequest,
						opts ...any,
					) (*workflowservice.UpdateNamespaceResponse, error) {
						s.Len(r.Config.CustomSearchAttributeAliases, len(tc.customSearchAttributesToAdd))
						aliases := expmaps.Values(r.Config.CustomSearchAttributeAliases)
						for _, saName := range tc.customSearchAttributesToAdd {
							s.Contains(aliases, saName)
						}
						return &workflowservice.UpdateNamespaceResponse{}, tc.updateNamespaceErr
					})
			}

			err := s.handler.addSearchAttributesSQL(
				ctx,
				tc.request,
				s.mockResource.VisibilityManager,
			)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_ListSearchAttributes_Elasticsearch() {
	handler := s.handler
	ctx := context.Background()

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true)
	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()
	s.mockResource.SearchAttributesManager.EXPECT().GetSearchAttributes(testIndexName, true).Return(searchattribute.TestEsNameTypeMap(), nil)
	resp, err := handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)

	s.mockResource.SearchAttributesManager.EXPECT().GetSearchAttributes(testIndexName, true).Return(searchattribute.NameTypeMap{}, errors.New("random error"))
	resp, err = handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.Error(err)
	s.Nil(resp)
}

func (s *operatorHandlerSuite) Test_ListSearchAttributes_SQL() {
	handler := s.handler
	ctx := context.Background()

	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(false)
	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()
	s.mockResource.ClientFactory.EXPECT().
		NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
		Return(nil, s.mockResource.GetFrontendClient(), nil).
		AnyTimes()
	s.mockResource.FrontendClient.EXPECT().
		DescribeNamespace(gomock.Any(), &workflowservice.DescribeNamespaceRequest{Namespace: testNamespace}).
		Return(
			&workflowservice.DescribeNamespaceResponse{
				Config: &namespacepb.NamespaceConfig{CustomSearchAttributeAliases: searchattribute.TestAliases},
			},
			nil,
		).
		AnyTimes()

	s.mockResource.SearchAttributesManager.EXPECT().
		GetSearchAttributes(testIndexName, true).
		Return(searchattribute.TestNameTypeMap(), nil)
	resp, err := handler.ListSearchAttributes(
		ctx,
		&operatorservice.ListSearchAttributesRequest{Namespace: testNamespace},
	)
	s.NoError(err)
	s.NotNil(resp)

	s.mockResource.SearchAttributesManager.EXPECT().
		GetSearchAttributes(testIndexName, true).
		Return(searchattribute.NameTypeMap{}, errors.New("random error"))
	resp, err = handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.Error(err)
	s.Nil(resp)
}

func (s *operatorHandlerSuite) Test_RemoveSearchAttributes_Elasticsearch() {
	ctx := context.Background()

	type test struct {
		Name       string
		Request    *operatorservice.RemoveSearchAttributesRequest
		SaveCalled bool
		Expected   error
	}
	testCases := []test{
		{
			Name: "success",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"CustomKeywordField",
				},
			},
			SaveCalled: true,
		},
		{
			Name: "reserved search attribute",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute not found",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
			},
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.Name, func() {
			saTypeMap := searchattribute.TestEsNameTypeMap()
			s.mockResource.VisibilityManager.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName})
			s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName)
			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(testIndexName, true).
				Return(saTypeMap, nil)

			if testCase.SaveCalled {
				expectedNewCustomSA := maps.Clone(saTypeMap.Custom())
				for _, sa := range testCase.Request.SearchAttributes {
					delete(expectedNewCustomSA, sa)
				}
				s.mockResource.SearchAttributesManager.EXPECT().
					SaveSearchAttributes(gomock.Any(), testIndexName, expectedNewCustomSA).
					Return(nil)
			}

			resp, err := s.handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			if testCase.Expected != nil {
				s.Nil(resp)
			} else {
				s.NotNil(resp)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_RemoveSearchAttributes_SQL() {
	ctx := context.Background()

	type test struct {
		Name       string
		Request    *operatorservice.RemoveSearchAttributesRequest
		SaveCalled bool
		Expected   error
	}
	testCases := []test{
		{
			Name: "success",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"CustomKeywordField",
				},
				Namespace: testNamespace,
			},
			SaveCalled: true,
		},
		{
			Name: "reserved search attribute",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
				Namespace: testNamespace,
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute not found",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
				Namespace: testNamespace,
			},
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.Name, func() {
			s.mockResource.VisibilityManager.EXPECT().GetStoreNames().Return([]string{mysql.PluginName})
			s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName)
			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(testIndexName, true).
				Return(searchattribute.TestNameTypeMap(), nil)
			s.mockResource.ClientFactory.EXPECT().
				NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
				Return(nil, s.mockResource.GetFrontendClient(), nil)
			s.mockResource.FrontendClient.EXPECT().
				DescribeNamespace(
					gomock.Any(),
					&workflowservice.DescribeNamespaceRequest{Namespace: testNamespace},
				).
				Return(
					&workflowservice.DescribeNamespaceResponse{
						Config: &namespacepb.NamespaceConfig{
							CustomSearchAttributeAliases: searchattribute.TestAliases,
						},
					},
					nil,
				)

			if testCase.SaveCalled {
				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(
						gomock.Any(),
						&workflowservice.UpdateNamespaceRequest{
							Namespace: testNamespace,
							Config: &namespacepb.NamespaceConfig{
								CustomSearchAttributeAliases: map[string]string{
									"Keyword01": "",
								},
							},
						},
					).
					Return(&workflowservice.UpdateNamespaceResponse{}, nil)
			}

			resp, err := s.handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			if testCase.Expected != nil {
				s.Nil(resp)
			} else {
				s.NotNil(resp)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_DeleteNamespace() {
	handler := s.handler
	ctx := context.Background()

	// Nil request.
	resp, err := handler.DeleteNamespace(ctx, nil)
	s.Equal(&serviceerror.InvalidArgument{Message: "Request is nil."}, err)
	s.Nil(resp)

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()

	handler.config = &Config{
		DeleteNamespaceDeleteActivityRPS:                    dynamicconfig.GetIntPropertyFn(22),
		DeleteNamespacePageSize:                             dynamicconfig.GetIntPropertyFn(8),
		DeleteNamespacePagesPerExecution:                    dynamicconfig.GetIntPropertyFn(78),
		DeleteNamespaceConcurrentDeleteExecutionsActivities: dynamicconfig.GetIntPropertyFn(3),
		DeleteNamespaceNamespaceDeleteDelay:                 dynamicconfig.GetDurationPropertyFn(22 * time.Hour),
	}

	// Start workflow failed.
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(nil, errors.New("start failed"))
	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "test-namespace",
	})
	s.Error(err)
	s.Equal("Unable to start temporal-sys-delete-namespace-workflow workflow: start failed.", err.Error())
	s.Nil(resp)

	// Workflow failed.
	mockRun := mocksdk.NewMockWorkflowRun(s.controller)
	mockRun.EXPECT().Get(gomock.Any(), gomock.Any()).Return(errors.New("workflow failed"))
	const RunId = "9a9f668a-58b1-427e-bed6-bf1401049f7d"
	mockRun.EXPECT().GetRunID().Return(RunId)
	mockRun.EXPECT().GetID().Return("test-workflow-id")
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun, nil)
	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "test-namespace",
	})
	s.Error(err)
	var sysWfErr *serviceerror.SystemWorkflow
	s.ErrorAs(err, &sysWfErr)
	s.Equal(RunId, sysWfErr.WorkflowExecution.RunId)
	s.Equal(fmt.Sprintf("System Workflow with WorkflowId test-workflow-id and RunId %s returned an error: workflow failed", RunId), err.Error())
	s.Nil(resp)

	// Workflow failed because of validation error (an attempt to delete system namespace).
	mockRun2 := mocksdk.NewMockWorkflowRun(s.controller)
	mockRun2.EXPECT().Get(gomock.Any(), gomock.Any()).Return(delnserrors.NewFailedPrecondition("unable to delete system namespace", nil))
	mockRun2.EXPECT().GetRunID().Return(RunId)
	mockRun2.EXPECT().GetID().Return("test-workflow-id")
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun2, nil)
	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "temporal-system",
	})
	s.Error(err)
	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionErr)
	s.Equal("unable to delete system namespace", failedPreconditionErr.Error())
	s.Nil(resp)

	// Workflow failed because of validation error (an attempt to delete system namespace).
	mockRun3 := mocksdk.NewMockWorkflowRun(s.controller)
	mockRun3.EXPECT().Get(gomock.Any(), gomock.Any()).Return(delnserrors.NewInvalidArgument("only one of namespace or namespace ID must be set", nil))
	mockRun3.EXPECT().GetRunID().Return(RunId)
	mockRun3.EXPECT().GetID().Return("test-workflow-id")
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun3, nil)
	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace:   "temporal-system",
		NamespaceId: "c13c01a7-3887-4eda-ba4b-9a07a6359e7e",
	})
	s.Error(err)
	var invalidArgErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgErr)
	s.Equal("only one of namespace or namespace ID must be set", invalidArgErr.Error())
	s.Nil(resp)

	// Success case.
	mockRun.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, valuePtr interface{}) error {
		wfResult := valuePtr.(*deletenamespace.DeleteNamespaceWorkflowResult)
		wfResult.DeletedNamespace = "test-namespace-deleted-ka2te"
		wfResult.DeletedNamespaceID = "c13c01a7-3887-4eda-ba4b-9a07a6359e7e"
		return nil
	})
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun, nil)

	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "test-namespace",
	})
	s.NoError(err)
	s.NotNil(resp)
	s.Equal("test-namespace-deleted-ka2te", resp.DeletedNamespace)

	// Success case with id.
	mockRun.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, valuePtr interface{}) error {
		wfResult := valuePtr.(*deletenamespace.DeleteNamespaceWorkflowResult)
		wfResult.DeletedNamespace = "test-namespace-deleted-ka2te"
		wfResult.DeletedNamespaceID = "c13c01a7-3887-4eda-ba4b-9a07a6359e7e"
		return nil
	})
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun, nil)

	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		NamespaceId: "c13c01a7-3887-4eda-ba4b-9a07a6359e7e",
	})
	s.NoError(err)
	s.NotNil(resp)
	s.Equal("test-namespace-deleted-ka2te", resp.DeletedNamespace)
}

func (s *operatorHandlerSuite) Test_RemoveRemoteCluster_Success() {
	var clusterName = "cluster"
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{clusterName: {}})
	s.mockResource.ClusterMetadataMgr.EXPECT().DeleteClusterMetadata(
		gomock.Any(),
		&persistence.DeleteClusterMetadataRequest{ClusterName: clusterName},
	).Return(nil)

	_, err := s.handler.RemoveRemoteCluster(context.Background(), &operatorservice.RemoveRemoteClusterRequest{ClusterName: clusterName})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_RemoveRemoteCluster_Error() {
	var clusterName = "cluster"
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{clusterName: {}})
	s.mockResource.ClusterMetadataMgr.EXPECT().DeleteClusterMetadata(
		gomock.Any(),
		&persistence.DeleteClusterMetadataRequest{ClusterName: clusterName},
	).Return(fmt.Errorf("test error"))

	_, err := s.handler.RemoveRemoteCluster(context.Background(), &operatorservice.RemoveRemoteClusterRequest{ClusterName: clusterName})
	s.Error(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordFound_Success() {
	var rpcAddress = uuid.NewString()
	var httpAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()
	var recordVersion int64 = 5

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterID,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordNotFound_Success() {
	var rpcAddress = uuid.NewString()
	var httpAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterID,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ClusterNameConflict() {
	var rpcAddress = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              s.mockResource.ClusterMetadata.GetCurrentClusterName(),
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_FailoverVersionIncrementMismatch() {
	var rpcAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ShardCount_Invalid() {
	var rpcAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        5,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ShardCount_Multiple() {
	var rpcAddress = uuid.NewString()
	var httpAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()
	var recordVersion int64 = 5

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			ClusterId:                clusterID,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_GlobalNamespaceDisabled() {
	var rpcAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: false,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_InitialFailoverVersionConflict() {
	var rpcAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		uuid.NewString(): {InitialFailoverVersion: 0},
	})
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_DescribeCluster_Error() {
	var rpcAddress = uuid.NewString()

	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		nil,
		fmt.Errorf("test error"),
	)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_GetClusterMetadata_Error() {
	var rpcAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		fmt.Errorf("test error"),
	)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_Error() {
	var rpcAddress = uuid.NewString()
	var httpAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterID,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, fmt.Errorf("test error"))
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	s.Error(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_NotApplied_Error() {
	var rpcAddress = uuid.NewString()
	var httpAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterID,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}
