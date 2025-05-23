package deployment

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
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
		deploymentClient   *DeploymentClientImpl
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
	d.deploymentClient = &DeploymentClientImpl{
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

func (d *deploymentWorkflowClientSuite) TestValidateDeploymentWfParams() {
	testCases := []struct {
		Description   string
		FieldName     string
		Input         string
		ExpectedError error
	}{
		{
			Description:   "Empty Field",
			FieldName:     SeriesFieldName,
			Input:         "",
			ExpectedError: serviceerror.NewInvalidArgument("DeploymentSeries cannot be empty"),
		},
		{
			Description:   "Large Field",
			FieldName:     SeriesFieldName,
			Input:         strings.Repeat("s", 1000),
			ExpectedError: serviceerror.NewInvalidArgument("size of DeploymentSeries larger than the maximum allowed"),
		},
		{
			Description:   "Valid field",
			FieldName:     SeriesFieldName,
			Input:         "A",
			ExpectedError: nil,
		},
	}

	for _, test := range testCases {
		fieldName := test.FieldName
		field := test.Input
		err := ValidateDeploymentWfParams(fieldName, field, testMaxIDLengthLimit)

		if test.ExpectedError == nil {
			d.NoError(err)
			continue
		}

		var invalidArgument *serviceerror.InvalidArgument
		d.ErrorAs(err, &invalidArgument)
		d.Equal(test.ExpectedError.Error(), err.Error())
	}
}

func (d *deploymentWorkflowClientSuite) TestGenerateDeploymentWorkflowID() {
	testCases := []struct {
		series, buildID string
		expected        string
	}{
		{
			series:   "test",
			buildID:  "build",
			expected: "temporal-sys-deployment:test:build",
		},
		{
			series:   "|es|",
			buildID:  "bu:ld",
			expected: "temporal-sys-deployment:||es||:bu|:ld",
		},
		{
			series:   "test:|",
			buildID:  "|build",
			expected: "temporal-sys-deployment:test|:||:||build",
		},
		{
			series:   "test|",
			buildID:  ":|build",
			expected: "temporal-sys-deployment:test||:|:||build",
		},
	}

	for _, test := range testCases {
		d.Equal(test.expected, GenerateDeploymentWorkflowID(test.series, test.buildID))
	}
}
