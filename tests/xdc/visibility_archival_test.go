package xdc

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/await"
	server "go.temporal.io/server/temporal"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	visibilityArchivalSimulationSuite struct {
		xdcBaseSuite

		store *simulatedVisibilityStore
	}

	simulatedVisibilityStore struct {
		sync.Mutex
		calls   map[string][]simulatedVisibilityCall
		writes  map[string]int
		records map[string]*archiverspb.VisibilityRecord
	}

	simulatedVisibilityCall struct {
		cluster              string
		uri                  string
		deduplicationEnabled bool
	}

	simulatedHistoryArchiver struct{}

	simulatedVisibilityArchiver struct {
		cluster string
		store   *simulatedVisibilityStore
	}
)

func TestVisibilityArchivalSimulationSuite(t *testing.T) {
	suite.Run(t, new(visibilityArchivalSimulationSuite))
}

func (s *visibilityArchivalSimulationSuite) SetupSuite() {
	s.store = &simulatedVisibilityStore{
		calls:   make(map[string][]simulatedVisibilityCall),
		writes:  make(map[string]int),
		records: make(map[string]*archiverspb.VisibilityRecord),
	}
	s.enableArchival = true
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.ArchivalProcessorArchiveDelay.Key():    time.Duration(0),
		dynamicconfig.ArchivalProcessorMaxPollInterval.Key(): time.Second,
	}
	s.serverOptionsByCluster = map[int][]server.ServerOption{
		0: s.archiverOptions("cluster-0"),
		1: s.archiverOptions("cluster-1"),
	}
	s.setupSuite()
}

func (s *visibilityArchivalSimulationSuite) SetupTest() {
	s.setupTest()
}

func (s *visibilityArchivalSimulationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *visibilityArchivalSimulationSuite) TestBothClustersUseSharedVisibilityArchive() {
	for _, deduplicationEnabled := range []bool{false, true} {
		s.Run(fmt.Sprintf("DeduplicationEnabled=%t", deduplicationEnabled), func() {
			var cleanups []func()
			for _, cluster := range s.clusters {
				cleanups = append(cleanups, cluster.OverrideDynamicConfig(
					s.T(),
					dynamicconfig.EnableVisibilityArchivalRecordDeduplication,
					deduplicationEnabled,
				))
			}
			defer func() {
				for i := len(cleanups) - 1; i >= 0; i-- {
					cleanups[i]()
				}
			}()

			s.archiveWorkflowAndAssert(deduplicationEnabled)
		})
	}
}

func (s *visibilityArchivalSimulationSuite) archiveWorkflowAndAssert(deduplicationEnabled bool) {
	ns := s.createGlobalNamespace()
	workflowID := "visibility-archival-simulation-" + uuid.NewString()
	workflowType := "visibility-archival-simulation-type"
	taskQueue := "visibility-archival-simulation-task-queue"

	startResponse, err := s.clusters[0].FrontendClient().StartWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           ns,
			WorkflowId:          workflowID,
			WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			WorkflowRunTimeout:  durationpb.New(time.Minute),
			WorkflowTaskTimeout: durationpb.New(time.Second),
			Identity:            "visibility-archival-simulation",
		},
	)
	s.Require().NoError(err)

	_, err = s.clusters[0].FrontendClient().TerminateWorkflowExecution(
		testcore.NewContext(),
		&workflowservice.TerminateWorkflowExecutionRequest{
			Namespace: ns,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      startResponse.GetRunId(),
			},
			Reason:   "trigger archival",
			Identity: "visibility-archival-simulation",
		},
	)
	s.Require().NoError(err)

	await.Require(testcore.NewContext(), s.T(), func(t *await.T) {
		calls, writes, record := s.store.snapshot(startResponse.GetRunId())
		clusters := make(map[string]struct{})
		URIs := make(map[string]struct{})
		for _, call := range calls {
			clusters[call.cluster] = struct{}{}
			URIs[call.uri] = struct{}{}
			require.Equal(t, deduplicationEnabled, call.deduplicationEnabled)
		}
		require.Len(t, clusters, 2)
		require.Len(t, URIs, 1)
		require.NotNil(t, record)
		require.Equal(t, startResponse.GetRunId(), record.GetRunId())
		if deduplicationEnabled {
			require.Equal(t, 1, writes)
		} else {
			require.GreaterOrEqual(t, writes, 2)
			require.Equal(t, len(calls), writes)
		}
	}, testTimeout, replicationCheckInterval)
}

func (s *visibilityArchivalSimulationSuite) archiverOptions(cluster string) []server.ServerOption {
	historyFactory := provider.CustomHistoryArchiverFactoryFunc(
		func(provider.NewCustomHistoryArchiverParams) (archiver.HistoryArchiver, error) {
			return &simulatedHistoryArchiver{}, nil
		},
	)
	visibilityFactory := provider.CustomVisibilityArchiverFactoryFunc(
		func(provider.NewCustomVisibilityArchiverParams) (archiver.VisibilityArchiver, error) {
			return &simulatedVisibilityArchiver{
				cluster: cluster,
				store:   s.store,
			}, nil
		},
	)
	return []server.ServerOption{
		server.WithCustomHistoryArchiverFactory(historyFactory),
		server.WithCustomVisibilityArchiverFactory(visibilityFactory),
	}
}

func (s *simulatedVisibilityStore) archive(
	cluster string,
	URI archiver.URI,
	request *archiverspb.VisibilityRecord,
	deduplicationEnabled bool,
) {
	s.Lock()
	defer s.Unlock()

	runID := request.GetRunId()
	s.calls[runID] = append(s.calls[runID], simulatedVisibilityCall{
		cluster:              cluster,
		uri:                  URI.String(),
		deduplicationEnabled: deduplicationEnabled,
	})
	existingRecord := s.records[runID]
	if deduplicationEnabled && proto.Equal(existingRecord, request) {
		return
	}
	s.records[runID] = proto.Clone(request).(*archiverspb.VisibilityRecord)
	s.writes[runID]++
}

func (s *simulatedVisibilityStore) snapshot(runID string) ([]simulatedVisibilityCall, int, *archiverspb.VisibilityRecord) {
	s.Lock()
	defer s.Unlock()

	var record *archiverspb.VisibilityRecord
	if s.records[runID] != nil {
		record = proto.Clone(s.records[runID]).(*archiverspb.VisibilityRecord)
	}
	return append([]simulatedVisibilityCall(nil), s.calls[runID]...), s.writes[runID], record
}

func (*simulatedHistoryArchiver) Archive(context.Context, archiver.URI, *archiver.ArchiveHistoryRequest, ...archiver.ArchiveOption) error {
	return nil
}

func (*simulatedHistoryArchiver) Get(context.Context, archiver.URI, *archiver.GetHistoryRequest) (*archiver.GetHistoryResponse, error) {
	return nil, nil
}

func (*simulatedHistoryArchiver) ValidateURI(archiver.URI) error {
	return nil
}

func (a *simulatedVisibilityArchiver) Archive(
	_ context.Context,
	URI archiver.URI,
	request *archiverspb.VisibilityRecord,
	opts ...archiver.ArchiveOption,
) error {
	featureCatalog := archiver.GetFeatureCatalog(opts...)
	a.store.archive(a.cluster, URI, request, featureCatalog.VisibilityArchivalRecordDeduplication)
	return nil
}

func (*simulatedVisibilityArchiver) Query(
	context.Context,
	archiver.URI,
	*archiver.QueryVisibilityRequest,
	searchattribute.NameTypeMap,
) (*archiver.QueryVisibilityResponse, error) {
	return nil, nil
}

func (*simulatedVisibilityArchiver) ValidateURI(archiver.URI) error {
	return nil
}
