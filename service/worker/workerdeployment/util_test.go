package workerdeployment

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/worker_versioning"
	"go.uber.org/mock/gomock"
)

// testMaxIDLengthLimit is the current default value used by dynamic config for
// MaxIDLengthLimit
const (
	testNamespace        = "deployment-test"
	testDeployment       = "A"
	testBuildID          = "xyz"
	testMaxIDLengthLimit = 1000
)

type (
	deploymentWorkflowClientSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		ns                 *namespace.Namespace
		mockNamespaceCache *namespace.MockRegistry
		mockHistoryClient  *historyservicemock.MockHistoryServiceClient
		VisibilityManager  *manager.MockVisibilityManager
		workerDeployment   *deploymentpb.Deployment
		deploymentClient   *ClientImpl
		sync.Mutex
	}
)

func (d *deploymentWorkflowClientSuite) SetupSuite() {
}

func (d *deploymentWorkflowClientSuite) TearDownSuite() {
}

func (d *deploymentWorkflowClientSuite) SetupTest() {
	d.Assertions = require.New(d.T())
	d.Lock()
	defer d.Unlock()
	d.controller = gomock.NewController(d.T())
	d.controller = gomock.NewController(d.T())
	d.ns, d.mockNamespaceCache = createMockNamespaceCache(d.controller, testNamespace)
	d.VisibilityManager = manager.NewMockVisibilityManager(d.controller)
	d.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(d.controller)
	d.workerDeployment = &deploymentpb.Deployment{
		SeriesName: testDeployment,
		BuildId:    testBuildID,
	}
	d.deploymentClient = &ClientImpl{
		historyClient:     d.mockHistoryClient,
		visibilityManager: d.VisibilityManager,
	}

}

func createMockNamespaceCache(controller *gomock.Controller, nsName namespace.Name) (*namespace.Namespace, *namespace.MockRegistry) {
	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: nsName.String()}, nil, "")
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(ns.Name(), nil).AnyTimes()
	return ns, mockNamespaceCache
}

func TestDeploymentWorkflowClientSuite(t *testing.T) {
	d := new(deploymentWorkflowClientSuite)
	suite.Run(t, d)
}

func (d *deploymentWorkflowClientSuite) TestValidateVersionWfParams() {
	testCases := []struct {
		Description   string
		FieldName     string
		Input         string
		ExpectedError error
	}{
		{
			Description:   "Empty Field",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         "",
			ExpectedError: serviceerror.NewInvalidArgument("WorkerDeploymentName cannot be empty"),
		},
		{
			Description:   "Large Field",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         strings.Repeat("s", 1000),
			ExpectedError: serviceerror.NewInvalidArgument("size of WorkerDeploymentName larger than the maximum allowed"),
		},
		{
			Description:   "Valid field",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         "A",
			ExpectedError: nil,
		},
		{
			Description:   "Invalid buildID",
			FieldName:     worker_versioning.WorkerDeploymentBuildIDFieldName,
			Input:         "__unversioned__",
			ExpectedError: serviceerror.NewInvalidArgument("BuildID cannot start with '__'"),
		},
		{
			Description:   "Invalid buildID",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         "__my_dep",
			ExpectedError: serviceerror.NewInvalidArgument("WorkerDeploymentName cannot start with '__'"),
		},
		{
			Description:   "Valid buildID",
			FieldName:     worker_versioning.WorkerDeploymentBuildIDFieldName,
			Input:         "valid_build__id",
			ExpectedError: nil,
		},
		{
			Description:   "Invalid deploymentName",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         "A/B",
			ExpectedError: nil,
		},
		{
			Description:   "Invalid deploymentName",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         "A.B",
			ExpectedError: serviceerror.NewInvalidArgument("worker deployment name cannot contain '.'"),
		},
	}

	for _, test := range testCases {
		fieldName := test.FieldName
		field := test.Input
		err := validateVersionWfParams(fieldName, field, testMaxIDLengthLimit)

		if test.ExpectedError == nil {
			d.NoError(err)
			continue
		}

		var invalidArgument *serviceerror.InvalidArgument
		d.ErrorAs(err, &invalidArgument)
		d.Equal(test.ExpectedError.Error(), err.Error())
	}
}

func TestDecodeWorkerDeploymentMemoTolerateUnknownFields(t *testing.T) {
	t.Parallel()
	decodeAndValidateMemo(t, "testdata/memo_with_last_current_time.json", "test-deployment", "build-1")
}

func TestDecodeWorkerDeploymentMemoTolerateMissingFields(t *testing.T) {
	t.Parallel()
	decodeAndValidateMemo(t, "testdata/memo_without_last_current_time.json", "test-deployment", "build-1")
}

func decodeAndValidateMemo(t *testing.T, filePath, deploymentName, buildID string) {
	jsonData, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// Create a payload with json/protobuf encoding (matching SDK's ProtoJSONPayloadConverter)
	payload := &commonpb.Payload{
		Metadata: map[string][]byte{
			"encoding":    []byte("json/protobuf"),
			"messageType": []byte("temporal.server.api.deployment.v1.WorkerDeploymentWorkflowMemo"),
		},
		Data: jsonData,
	}

	// Create a memo with the payload
	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			WorkerDeploymentMemoField: payload,
		},
	}

	// Decode should succeed even if the payload contains unknown fields
	result, err := DecodeWorkerDeploymentMemo(memo)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify known fields are decoded correctly
	require.Equal(t, deploymentName, result.DeploymentName)
	require.NotNil(t, result.CreateTime)
	require.NotNil(t, result.RoutingConfig)
	require.Equal(t, deploymentName, result.RoutingConfig.GetCurrentDeploymentVersion().GetDeploymentName())
	require.Equal(t, buildID, result.RoutingConfig.GetCurrentDeploymentVersion().GetBuildId())
}

//nolint:revive
func (d *deploymentWorkflowClientSuite) TestGenerateVersionWorkflowID() {
	// testCases := []struct {
	//	series, buildID string
	//	expected        string
	// }{
	//	{
	//		series:   "test",
	//		buildID:  "build",
	//		expected: "temporal-sys-deployment:test:build",
	//	},
	//	{
	//		series:   "|es|",
	//		buildID:  "bu:ld",
	//		expected: "temporal-sys-deployment:||es||:bu|:ld",
	//	},
	//	{
	//		series:   "test:|",
	//		buildID:  "|build",
	//		expected: "temporal-sys-deployment:test|:||:||build",
	//	},
	//	{
	//		series:   "test|",
	//		buildID:  ":|build",
	//		expected: "temporal-sys-deployment:test||:|:||build",
	//	},
	// }
	//
	// for _, test := range testCases {
	//	d.Equal(test.expected, GenerateVersionWorkflowID(test.series, test.buildID))
	// }
}
