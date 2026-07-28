package cassandra

import (
	"context"
	"errors"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/util"
)

func TestListNexusEndpointsUsesSameQueryForPageToken(t *testing.T) {
	endpointID := "11111111-1111-1111-1111-111111111111"
	token := []byte("page-token")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateListEndpointsFirstPageQuery:
				require.Empty(t, args)
				id, err := gocql.ParseUUID(endpointID)
				require.NoError(t, err)
				return &recordingQuery{
					iter: &recordingIter{
						mapRows: []map[string]any{
							{
								"id":            id,
								"version":       int64(1),
								"data":          []byte("endpoint"),
								"data_encoding": enumspb.ENCODING_TYPE_PROTO3.String(),
							},
						},
					},
				}
			case templateGetTableVersion:
				return &recordingQuery{scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 1
					return nil
				}}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store := &NexusEndpointStore{session: session}

	resp, err := store.ListNexusEndpoints(t.Context(), &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: 1,
		NextPageToken:         token,
		PageSize:              2,
	})

	require.NoError(t, err)
	require.Len(t, resp.Endpoints, 1)
	require.Len(t, session.queries, 2)
	require.Equal(t, templateListEndpointsFirstPageQuery, session.queries[0].stmt)
	require.Equal(t, token, session.queries[0].query.pageState)
}

func TestListNexusEndpointsFirstPageChecksTableVersion(t *testing.T) {
	iter := &recordingIter{
		mapRows: []map[string]any{
			{
				"version": int64(2),
			},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListEndpointsFirstPageQuery, stmt)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &NexusEndpointStore{session: session}

	_, err := store.ListNexusEndpoints(t.Context(), &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: 1,
		PageSize:              10,
	})

	require.ErrorIs(t, err, p.ErrNexusTableVersionConflict)
	require.Equal(t, 1, iter.closeCalls)
}

func TestListNexusEndpointsClosesIteratorOnEndpointRowError(t *testing.T) {
	iter := &recordingIter{
		mapRows: []map[string]any{
			{
				"id":      "endpoint-id",
				"version": int64(1),
			},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListEndpointsFirstPageQuery, stmt)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &NexusEndpointStore{session: session}

	resp, err := store.ListNexusEndpoints(t.Context(), &p.ListNexusEndpointsRequest{
		NextPageToken: []byte("next-page"),
		PageSize:      10,
	})

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, 1, iter.closeCalls)
}

func TestListNexusEndpointsFirstPageClosesIteratorOnVersionRowError(t *testing.T) {
	iter := &recordingIter{
		mapRows: []map[string]any{
			{
				"version": "not-int64",
			},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListEndpointsFirstPageQuery, stmt)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &NexusEndpointStore{session: session}

	resp, err := store.ListNexusEndpoints(t.Context(), &p.ListNexusEndpointsRequest{
		PageSize: 10,
	})

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, 1, iter.closeCalls)
}

func TestListQueuesUsesSameQueryForPageToken(t *testing.T) {
	pageState := []byte("page-token")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetQueueNamesQuery, stmt)
			require.Equal(t, []any{p.QueueTypeHistoryDLQ}, args)
			return &recordingQuery{
				iter: &recordingIter{pageState: pageState},
			}
		},
	}
	store := &queueV2Store{session: session}

	resp, err := store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType:     p.QueueTypeHistoryDLQ,
		PageSize:      10,
		NextPageToken: pageState,
	})

	require.NoError(t, err)
	require.Empty(t, resp.Queues)
	require.Equal(t, pageState, resp.NextPageToken)
	require.Len(t, session.queries, 1)
}

func TestListQueuesReturnsIteratorPageState(t *testing.T) {
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: p.FirstQueueMessageID,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateGetQueueNamesQuery:
				require.Equal(t, []any{p.QueueTypeHistoryDLQ}, args)
				return &recordingQuery{
					iter: &recordingIter{
						pageState: []byte("next-page"),
						scanRows: [][]any{
							{"queue-0", queueBytes, enumspb.ENCODING_TYPE_PROTO3.String(), int64(0)},
						},
					},
				}
			case TemplateGetMaxMessageIDQuery:
				return &recordingQuery{
					scanFn: func(dest ...any) error {
						return gocql.ErrNotFound
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store := &queueV2Store{session: session}

	resp, err := store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		PageSize:  1,
	})

	require.NoError(t, err)
	require.Len(t, resp.Queues, 1)
	require.Equal(t, []byte("next-page"), resp.NextPageToken)
}

func TestListQueuesFillsLogicalPageAcrossCassandraPages(t *testing.T) {
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: p.FirstQueueMessageID,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	firstPageState := []byte("first-page")
	nextPageState := []byte("next-page")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateGetQueueNamesQuery:
				require.Equal(t, []any{p.QueueTypeHistoryDLQ}, args)
				return &recordingQuery{
					iterFn: func(q *recordingQuery) cgocql.Iter {
						switch string(q.pageState) {
						case "":
							require.Equal(t, 3, q.pageSize)
							return &recordingIter{
								pageState: firstPageState,
								scanRows: [][]any{
									{"queue-0", queueBytes, enumspb.ENCODING_TYPE_PROTO3.String(), int64(0)},
								},
							}
						case string(firstPageState):
							require.Equal(t, 2, q.pageSize)
							return &recordingIter{
								pageState: nextPageState,
								scanRows: [][]any{
									{"queue-1", queueBytes, enumspb.ENCODING_TYPE_PROTO3.String(), int64(0)},
									{"queue-2", queueBytes, enumspb.ENCODING_TYPE_PROTO3.String(), int64(0)},
								},
							}
						default:
							t.Fatalf("unexpected page state: %q", q.pageState)
							return nil
						}
					},
				}
			case TemplateGetMaxMessageIDQuery:
				return &recordingQuery{
					scanFn: func(dest ...any) error {
						return gocql.ErrNotFound
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store := &queueV2Store{session: session}

	resp, err := store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		PageSize:  3,
	})

	require.NoError(t, err)
	require.Len(t, resp.Queues, 3)
	require.Equal(t, []byte("next-page"), resp.NextPageToken)
	require.Equal(t, []string{"queue-0", "queue-1", "queue-2"}, []string{
		resp.Queues[0].QueueName,
		resp.Queues[1].QueueName,
		resp.Queues[2].QueueName,
	})
}

func TestListQueuesQueryKeepsUpgradeCompatibleQueueSchema(t *testing.T) {
	require.Contains(t, templateGetQueueNamesQuery, "ALLOW FILTERING")
}

func TestListQueuesClosesIteratorOnMetadataError(t *testing.T) {
	iter := &recordingIter{
		scanRows: [][]any{
			{"bad-queue", []byte("metadata"), "invalid-encoding", int64(1)},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetQueueNamesQuery, stmt)
			require.Equal(t, []any{p.QueueTypeHistoryDLQ}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := NewQueueV2Store(session, log.NewNoopLogger())

	resp, err := store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType: p.QueueTypeHistoryDLQ,
		PageSize:  10,
	})

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, 1, iter.closeCalls)
}

func TestScheduledTaskQueriesHaveSeparatedPredicates(t *testing.T) {
	require.NotContains(t, templateGetHistoryScheduledTasksQuery, "?and")
	require.NotContains(t, templateGetTimerTasksQuery, "?and")
}

func TestQueueMessageInsertsUseConditionalWrites(t *testing.T) {
	require.Contains(t, templateEnqueueMessageQuery, "IF NOT EXISTS")
	require.Contains(t, TemplateEnqueueMessageQuery, "IF NOT EXISTS")
}

func TestQueueMessageInsertsSupportRegularWrites(t *testing.T) {
	require.NotContains(t, templateEnqueueMessageWithoutCASQuery, "IF")
	require.NotContains(t, TemplateEnqueueMessageWithoutCASQuery, "IF")
}

func TestHistoryNodeSchemaPartitionsByTreeAndBranch(t *testing.T) {
	requireHistoryNodeSchemaPartitionsByTreeAndBranch(t, "../../../schema/cassandra/temporal/schema.cql")
	requireHistoryNodeSchemaPartitionsByTreeAndBranch(t, "../../../schema/cassandra/temporal/versioned/v1.0/schema.cql")
}

func TestHistoryNodeRangeDeleteUsesBranchPartition(t *testing.T) {
	require.Contains(t, v2templateRangeDeleteHistoryNode, "WHERE tree_id = ? AND branch_id = ?")
}

func TestLegacyQueueMessageIDRangeSchemaPartitionsByQueueType(t *testing.T) {
	requireLegacyQueueMessageIDRangeSchemaPartitionsByQueueType(t, "../../../schema/cassandra/temporal/schema.cql")
	requireLegacyQueueMessageIDRangeSchemaPartitionsByQueueType(t, "../../../schema/cassandra/temporal/versioned/v1.0/schema.cql")
	requireLegacyQueueMessageIDRangeSchemaPartitionsByQueueType(t, "../../../schema/cassandra/temporal/versioned/v1.14/queue_message_id_ranges.cql")
}

func TestQueueMetadataSchemaKeepsUpgradeCompatiblePrimaryKey(t *testing.T) {
	requireQueueMetadataSchemaKeepsUpgradeCompatiblePrimaryKey(t, "../../../schema/cassandra/temporal/schema.cql")
	requireQueueMetadataSchemaKeepsUpgradeCompatiblePrimaryKey(t, "../../../schema/cassandra/temporal/versioned/v1.9/queues.cql")
}

func TestQueueMessageIDRangeSchemaPartitionsByQueue(t *testing.T) {
	requireQueueMessageIDRangeSchemaPartitionsByQueue(t, "../../../schema/cassandra/temporal/schema.cql")
	requireQueueMessageIDRangeSchemaPartitionsByQueue(t, "../../../schema/cassandra/temporal/versioned/v1.9/queues.cql")
	requireQueueMessageIDRangeSchemaPartitionsByQueue(t, "../../../schema/cassandra/temporal/versioned/v1.14/queue_message_id_ranges.cql")
}

func requireHistoryNodeSchemaPartitionsByTreeAndBranch(t *testing.T, schema string) {
	statements, err := p.LoadAndSplitQuery([]string{schema})
	require.NoError(t, err)

	for _, stmt := range statements {
		if strings.Contains(stmt, "CREATE TABLE history_node") {
			require.Contains(t, stmt, "PRIMARY KEY ((tree_id, branch_id), node_id, txn_id")
			return
		}
	}
	require.Fail(t, "missing history_node schema")
}

func requireLegacyQueueMessageIDRangeSchemaPartitionsByQueueType(t *testing.T, schema string) {
	statements, err := p.LoadAndSplitQuery([]string{schema})
	require.NoError(t, err)

	for _, stmt := range statements {
		if strings.Contains(stmt, "queue_message_id_range") && !strings.Contains(stmt, "queue_message_id_ranges") {
			require.Contains(t, stmt, "PRIMARY KEY (queue_type)")
			return
		}
	}
	require.Fail(t, "missing queue_message_id_range schema")
}

func requireQueueMetadataSchemaKeepsUpgradeCompatiblePrimaryKey(t *testing.T, schema string) {
	statements, err := p.LoadAndSplitQuery([]string{schema})
	require.NoError(t, err)

	for _, stmt := range statements {
		if strings.Contains(stmt, "CREATE TABLE queues") {
			require.NotContains(t, stmt, "queue_bucket")
			require.Contains(t, stmt, "PRIMARY KEY ((queue_type, queue_name))")
			return
		}
	}
	require.Fail(t, "missing queues schema")
}

func requireQueueMessageIDRangeSchemaPartitionsByQueue(t *testing.T, schema string) {
	statements, err := p.LoadAndSplitQuery([]string{schema})
	require.NoError(t, err)

	for _, stmt := range statements {
		if strings.Contains(stmt, "queue_message_id_ranges") {
			require.Contains(t, stmt, "PRIMARY KEY ((queue_type, queue_name))")
			return
		}
	}
	require.Fail(t, "missing queue_message_id_ranges schema")
}

func TestGetClusterMembersOmitsAllowFilteringForPartitionScan(t *testing.T) {
	stmt := recordGetClusterMembersQuery(t, &p.GetClusterMembersRequest{})

	require.NotContains(t, stmt, templateAllowFiltering)
	require.Equal(t, templateGetClusterMembership, stmt)
}

func TestGetClusterMembersOmitsAllowFilteringForFullPrimaryKeyLookup(t *testing.T) {
	stmt := recordGetClusterMembersQuery(t, &p.GetClusterMembersRequest{
		HostIDEquals: []byte("host-id"),
		RoleEquals:   p.Matching,
	})

	require.NotContains(t, stmt, templateAllowFiltering)
	require.Contains(t, stmt, templateWithHostIDSuffix)
	require.Contains(t, stmt, templateWithRoleSuffix)
}

func TestGetClusterMembersKeepsAllowFilteringForSecondaryFilters(t *testing.T) {
	for _, request := range []*p.GetClusterMembersRequest{
		{HostIDEquals: []byte("host-id")},
		{RPCAddressEquals: net.ParseIP("127.0.0.1")},
		{SessionStartedAfter: time.Now().UTC()},
		{LastHeartbeatWithin: time.Minute},
	} {
		stmt := recordGetClusterMembersQuery(t, request)

		require.Contains(t, stmt, templateAllowFiltering)
	}
}

func TestCountTaskQueuesByBuildIDUsesLimit(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateLimitedCountTaskQueueByBuildIDQuery, stmt)
			require.Equal(t, []any{"namespace-id", "build-id", 2}, args)
			return &recordingQuery{
				iter: &recordingIter{
					scanRows: [][]any{
						{"task-queue-1"},
						{"task-queue-2"},
					},
				},
			}
		},
	}
	store := userDataStore{Session: session}

	count, err := store.CountTaskQueuesByBuildId(t.Context(), &p.CountTaskQueuesByBuildIdRequest{
		NamespaceID: "namespace-id",
		BuildID:     "build-id",
		Limit:       2,
	})

	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, []string{templateLimitedCountTaskQueueByBuildIDQuery}, recordedStatements(session.queries))
}

func TestCountTaskQueuesByBuildIDConvertsExactCountError(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateCountTaskQueueByBuildIDQuery, stmt)
			require.Equal(t, []any{"namespace-id", "build-id"}, args)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return errors.New("cassandra unavailable")
				},
			}
		},
	}
	store := userDataStore{Session: session}

	count, err := store.CountTaskQueuesByBuildId(t.Context(), &p.CountTaskQueuesByBuildIdRequest{
		NamespaceID: "namespace-id",
		BuildID:     "build-id",
	})

	require.Error(t, err)
	require.Zero(t, count)
	require.Contains(t, err.Error(), "CountTaskQueuesByBuildId")
}

func TestGetTaskQueuesByBuildIDUsesNextPageToken(t *testing.T) {
	pageToken := []byte("next-page")
	queryCalls := 0
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			queryCalls++
			require.Equal(t, templateListTaskQueueNamesByBuildIdQuery, stmt)
			require.Equal(t, []any{"namespace-id", "build-id"}, args)
			switch queryCalls {
			case 1:
				return &recordingQuery{
					iter: &recordingIter{
						mapRows: []map[string]any{
							{"task_queue_name": "task-queue-1"},
						},
						pageState: pageToken,
					},
				}
			case 2:
				return &recordingQuery{
					iter: &recordingIter{
						mapRows: []map[string]any{
							{"task_queue_name": "task-queue-2"},
						},
					},
				}
			default:
				t.Fatalf("unexpected query call: %d", queryCalls)
				return nil
			}
		},
	}
	store := userDataStore{Session: session}

	taskQueues, err := store.GetTaskQueuesByBuildId(t.Context(), &p.GetTaskQueuesByBuildIdRequest{
		NamespaceID: "namespace-id",
		BuildID:     "build-id",
	})

	require.NoError(t, err)
	require.Equal(t, []string{"task-queue-1", "task-queue-2"}, taskQueues)
	require.Len(t, session.queries, 2)
	require.Empty(t, session.queries[0].query.pageState)
	require.Equal(t, pageToken, session.queries[1].query.pageState)
	require.Equal(t, listTaskQueueNamesByBuildIdPageSize, session.queries[0].query.pageSize)
	require.Equal(t, listTaskQueueNamesByBuildIdPageSize, session.queries[1].query.pageSize)
}

func TestGetTaskQueuesByBuildIDDropsRepeatedEmptyPageToken(t *testing.T) {
	pageToken := []byte("same-page")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListTaskQueueNamesByBuildIdQuery, stmt)
			require.Equal(t, []any{"namespace-id", "build-id"}, args)
			return &recordingQuery{
				iter: &recordingIter{
					pageState: pageToken,
				},
			}
		},
	}
	store := userDataStore{Session: session}

	taskQueues, err := store.GetTaskQueuesByBuildId(t.Context(), &p.GetTaskQueuesByBuildIdRequest{
		NamespaceID: "namespace-id",
		BuildID:     "build-id",
	})

	require.NoError(t, err)
	require.Empty(t, taskQueues)
	require.Len(t, session.queries, 2)
	require.Empty(t, session.queries[0].query.pageState)
	require.Equal(t, pageToken, session.queries[1].query.pageState)
}

func TestListConcreteExecutionsClosesIterator(t *testing.T) {
	iter := &recordingIter{
		closeErr: errors.New("close failed"),
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListWorkflowExecutionQuery, stmt)
			require.Equal(t, []any{int32(7), rowTypeExecution}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &MutableStateStore{Session: session}

	response, err := store.ListConcreteExecutions(t.Context(), &p.ListConcreteExecutionsRequest{
		ShardID:  7,
		PageSize: 100,
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.Contains(t, err.Error(), "ListConcreteExecutions")
	require.Len(t, session.queries, 1)
	require.Equal(t, 1, iter.closeCalls)
}

func TestListConcreteExecutionsClosesIteratorOnRowError(t *testing.T) {
	iter := &recordingIter{
		mapRows: []map[string]any{
			{"execution": "not-bytes"},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListWorkflowExecutionQuery, stmt)
			require.Equal(t, []any{int32(7), rowTypeExecution}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &MutableStateStore{Session: session}

	response, err := store.ListConcreteExecutions(t.Context(), &p.ListConcreteExecutionsRequest{
		ShardID:  7,
		PageSize: 100,
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.Contains(t, err.Error(), "execution")
	require.Equal(t, 1, iter.closeCalls)
}

func TestGetTasksV1ClosesIteratorOnRowError(t *testing.T) {
	iter := &recordingIter{
		mapRows: []map[string]any{
			{"task_id": int64(1)},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetTasksQuery, stmt)
			require.Equal(t, []any{"namespace-id", "task-queue", enumspb.TASK_QUEUE_TYPE_WORKFLOW, rowTypeTask, int64(1), int64(10)}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &matchingTaskStoreV1{Session: session}

	response, err := store.GetTasks(t.Context(), &p.GetTasksRequest{
		NamespaceID:        "namespace-id",
		TaskQueue:          "task-queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: 10,
		PageSize:           10,
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.Equal(t, 1, iter.closeCalls)
}

func TestGetTasksV2ClosesIteratorOnRowError(t *testing.T) {
	iter := &recordingIter{
		mapRows: []map[string]any{
			{"task_id": int64(1)},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetTasksQuery_v2_limit, stmt)
			require.Equal(t, []any{
				"namespace-id",
				"task-queue",
				enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				rowTypeTask,
				int64(1),
				int64(1),
				rowTypeTask,
				int64(math.MaxInt64),
				int64(math.MaxInt64),
				10,
			}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := &matchingTaskStoreV2{Session: session}

	response, err := store.GetTasks(t.Context(), &p.GetTasksRequest{
		NamespaceID:        "namespace-id",
		TaskQueue:          "task-queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		InclusiveMinPass:   1,
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           10,
		UseLimit:           true,
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.Equal(t, 1, iter.closeCalls)
}

func TestReadHistoryBranchReverseUsesBranchPartition(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadHistoryNodeReverse, stmt)
			require.Equal(t, []any{treeID, branchID, int64(10), int64(20)}, args)
			return &recordingQuery{
				iter: &recordingIter{},
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(t, err)

	_, err = store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
		BranchToken:  branchToken,
		BranchID:     branchID,
		MinNodeID:    10,
		MaxNodeID:    20,
		PageSize:     100,
		ReverseOrder: true,
	})

	require.NoError(t, err)
	require.Len(t, session.queries, 1)
}

func TestReadHistoryBranchReturnsPageTokenAfterScan(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	pageToken := []byte("next-page")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadHistoryNode, stmt)
			require.Equal(t, []any{treeID, branchID, int64(1), int64(10)}, args)
			return &recordingQuery{
				iter: &recordingIter{
					mapRows: []map[string]any{
						{
							"node_id":       int64(1),
							"prev_txn_id":   int64(0),
							"txn_id":        int64(1),
							"data":          []byte("events"),
							"data_encoding": enumspb.ENCODING_TYPE_PROTO3.String(),
						},
					},
					pageState:               pageToken,
					pageStateAfterExhausted: true,
				},
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(t, err)

	response, err := store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
		BranchToken: branchToken,
		BranchID:    branchID,
		MinNodeID:   1,
		MaxNodeID:   10,
		PageSize:    1,
	})

	require.NoError(t, err)
	require.Equal(t, pageToken, response.NextPageToken)
	require.Len(t, response.Nodes, 1)
}

func TestReadHistoryBranchClosesIteratorOnRowError(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	iter := &recordingIter{
		mapRows: []map[string]any{
			{
				"node_id": int64(1),
				"txn_id":  int64(1),
			},
		},
	}
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadHistoryNode, stmt)
			require.Equal(t, []any{treeID, branchID, int64(1), int64(10)}, args)
			return &recordingQuery{
				iter: iter,
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(t, err)

	response, err := store.ReadHistoryBranch(t.Context(), &p.InternalReadHistoryBranchRequest{
		BranchToken: branchToken,
		BranchID:    branchID,
		MinNodeID:   1,
		MaxNodeID:   10,
		PageSize:    1,
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.Equal(t, 1, iter.closeCalls)
}

func TestGetAllHistoryTreeBranchesReturnsPageTokenAfterScan(t *testing.T) {
	pageToken := []byte("next-page")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateScanAllTreeBranches, stmt)
			return &recordingQuery{
				iter: &recordingIter{
					scanRows: [][]any{
						{"tree-id", "branch-id", []byte("branch"), enumspb.ENCODING_TYPE_PROTO3.String()},
					},
					pageState:               pageToken,
					pageStateAfterExhausted: true,
				},
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())

	response, err := store.GetAllHistoryTreeBranches(t.Context(), &p.GetAllHistoryTreeBranchesRequest{
		PageSize: 1,
	})

	require.NoError(t, err)
	require.Equal(t, pageToken, response.NextPageToken)
	require.Len(t, response.Branches, 1)
}

func TestGetHistoryTreeContainingBranchUsesPostScanPageToken(t *testing.T) {
	const (
		treeID   = "11111111-1111-1111-1111-111111111111"
		branchID = "22222222-2222-2222-2222-222222222222"
	)
	pageToken := []byte("next-page")
	firstPage := true
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, v2templateReadAllBranches, stmt)
			require.Equal(t, []any{treeID}, args)
			return &recordingQuery{
				iterFn: func(*recordingQuery) cgocql.Iter {
					if firstPage {
						firstPage = false
						return &recordingIter{
							scanRows: [][]any{
								{"branch-1", []byte("branch-1-data"), enumspb.ENCODING_TYPE_PROTO3.String()},
							},
							pageState:               pageToken,
							pageStateAfterExhausted: true,
						}
					}
					return &recordingIter{
						scanRows: [][]any{
							{"branch-2", []byte("branch-2-data"), enumspb.ENCODING_TYPE_PROTO3.String()},
						},
					}
				},
			}
		},
	}
	store := NewHistoryStore(session, serialization.NewSerializer())
	branchToken, err := store.NewHistoryBranch("", "", "", treeID, util.Ptr(branchID), nil, 0, 0, 0)
	require.NoError(t, err)

	response, err := store.GetHistoryTreeContainingBranch(t.Context(), &p.InternalGetHistoryTreeContainingBranchRequest{
		BranchToken: branchToken,
	})

	require.NoError(t, err)
	require.Len(t, response.TreeInfos, 2)
	require.Len(t, session.queries, 1)
	require.Equal(t, pageToken, session.queries[0].query.pageState)
}

func TestQueueEnqueueCachesMessageIDRange(t *testing.T) {
	var maxReads int
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateGetQueueMessageIDRangeQuery:
				require.Equal(t, []any{p.NamespaceReplicationQueueType}, args)
				return &recordingQuery{
					scanFn: func(dest ...any) error {
						return gocql.ErrNotFound
					},
				}
			case templateGetLastMessageIDQuery:
				require.Equal(t, []any{p.NamespaceReplicationQueueType}, args)
				maxReads++
				return &recordingQuery{
					mapScanFn: func(dest map[string]any) error {
						dest["message_id"] = int64(42)
						return nil
					},
				}
			case templateCreateQueueMessageIDRangeQuery:
				require.Equal(t, []any{p.NamespaceReplicationQueueType, int64(1067), int64(0)}, args)
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return true, nil
					},
				}
			case templateEnqueueMessageQuery:
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return true, nil
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store, err := NewQueueStore(p.NamespaceReplicationQueueType, session, log.NewNoopLogger())
	require.NoError(t, err)

	err = store.EnqueueMessage(t.Context(), &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	})
	require.NoError(t, err)
	err = store.EnqueueMessage(t.Context(), &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	})
	require.NoError(t, err)

	require.Equal(t, 1, maxReads)
	require.Equal(t, []string{
		templateGetQueueMessageIDRangeQuery,
		templateGetLastMessageIDQuery,
		templateCreateQueueMessageIDRangeQuery,
		templateEnqueueMessageQuery,
		templateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
	require.Equal(t, int64(43), session.queries[3].args[1])
	require.Equal(t, int64(44), session.queries[4].args[1])
}

func TestQueueEnqueueMessageIDRangeConflictRefreshesRange(t *testing.T) {
	var maxReads int
	var rangeReads int
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateGetQueueMessageIDRangeQuery:
				rangeReads++
				return &recordingQuery{
					scanFn: func(dest ...any) error {
						if rangeReads == 1 {
							return gocql.ErrNotFound
						}
						*dest[0].(*int64) = 100
						*dest[1].(*int64) = 7
						return nil
					},
				}
			case templateGetLastMessageIDQuery:
				maxReads++
				return &recordingQuery{
					mapScanFn: func(dest map[string]any) error {
						dest["message_id"] = int64(42)
						return nil
					},
				}
			case templateCreateQueueMessageIDRangeQuery:
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return false, nil
					},
				}
			case templateUpdateQueueMessageIDRangeQuery:
				require.Equal(t, []any{int64(1124), int64(8), p.NamespaceReplicationQueueType, int64(7)}, args)
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return true, nil
					},
				}
			case templateEnqueueMessageQuery:
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return true, nil
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store, err := NewQueueStore(p.NamespaceReplicationQueueType, session, log.NewNoopLogger())
	require.NoError(t, err)

	err = store.EnqueueMessage(t.Context(), &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	})
	require.NoError(t, err)
	err = store.EnqueueMessage(t.Context(), &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	})
	require.NoError(t, err)

	require.Equal(t, 1, maxReads)
	require.Equal(t, []string{
		templateGetQueueMessageIDRangeQuery,
		templateGetLastMessageIDQuery,
		templateCreateQueueMessageIDRangeQuery,
		templateGetQueueMessageIDRangeQuery,
		templateUpdateQueueMessageIDRangeQuery,
		templateEnqueueMessageQuery,
		templateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
	require.Equal(t, int64(100), session.queries[5].args[1])
	require.Equal(t, int64(101), session.queries[6].args[1])
}

func TestQueueEnqueueReservesNewRangeAfterCachedRangeExhausted(t *testing.T) {
	rangeReads := 0
	insertCalls := 0
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateGetQueueMessageIDRangeQuery:
				rangeReads++
				require.Equal(t, []any{p.NamespaceReplicationQueueType}, args)
				return &recordingQuery{
					scanFn: func(dest ...any) error {
						*dest[0].(*int64) = queueMessageIDRangeAllocationSize
						*dest[1].(*int64) = 7
						return nil
					},
				}
			case templateUpdateQueueMessageIDRangeQuery:
				require.Equal(t, []any{
					queueMessageIDRangeAllocationSize * 2,
					int64(8),
					p.NamespaceReplicationQueueType,
					int64(7),
				}, args)
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return true, nil
					},
				}
			case templateEnqueueMessageQuery:
				insertCalls++
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return true, nil
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store, err := NewQueueStore(p.NamespaceReplicationQueueType, session, log.NewNoopLogger())
	require.NoError(t, err)
	cassandraStore := store.(*QueueStore)
	cassandraStore.messageIDRanges.Store(p.NamespaceReplicationQueueType, queueMessageIDRange{
		nextMessageID:         queueMessageIDRangeAllocationSize - 1,
		exclusiveMaxMessageID: queueMessageIDRangeAllocationSize,
	})

	err = store.EnqueueMessage(t.Context(), &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	})
	require.NoError(t, err)
	err = store.EnqueueMessage(t.Context(), &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	})
	require.NoError(t, err)

	require.Equal(t, 1, rangeReads)
	require.Equal(t, 2, insertCalls)
	require.Equal(t, []string{
		templateEnqueueMessageQuery,
		templateGetQueueMessageIDRangeQuery,
		templateUpdateQueueMessageIDRangeQuery,
		templateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
	require.Equal(t, queueMessageIDRangeAllocationSize-1, session.queries[0].args[1])
	require.Equal(t, queueMessageIDRangeAllocationSize, session.queries[3].args[1])
}

func TestQueueEnqueueMessageIDConflictReturnsError(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateEnqueueMessageQuery:
				require.Equal(t, []any{
					p.NamespaceReplicationQueueType,
					int64(42),
					[]byte("message"),
					enumspb.ENCODING_TYPE_PROTO3.String(),
				}, args)
				return &recordingQuery{
					mapScanCASFn: func(map[string]any) (bool, error) {
						return false, nil
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store, err := NewQueueStore(p.NamespaceReplicationQueueType, session, log.NewNoopLogger())
	require.NoError(t, err)
	cassandraStore := store.(*QueueStore)
	cassandraStore.messageIDRanges.Store(p.NamespaceReplicationQueueType, queueMessageIDRange{
		nextMessageID:         42,
		exclusiveMaxMessageID: 43,
	})

	err = store.EnqueueMessage(t.Context(), &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("message"),
	})
	require.ErrorIs(t, err, ErrEnqueueMessageConflict)
	require.Equal(t, []string{
		templateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
}

func TestQueueEnqueueCanDisableInsertCAS(t *testing.T) {
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			switch stmt {
			case templateEnqueueMessageWithoutCASQuery:
				require.Equal(t, []any{
					p.NamespaceReplicationQueueType,
					int64(42),
					[]byte("message"),
					enumspb.ENCODING_TYPE_PROTO3.String(),
				}, args)
				return &recordingQuery{
					execFn: func() error {
						return nil
					},
				}
			default:
				t.Fatalf("unexpected query: %s", stmt)
				return nil
			}
		},
	}
	store, err := NewQueueStore(p.NamespaceReplicationQueueType, session, log.NewNoopLogger(), true)
	require.NoError(t, err)
	cassandraStore := store.(*QueueStore)
	cassandraStore.messageIDRanges.Store(p.NamespaceReplicationQueueType, queueMessageIDRange{
		nextMessageID:         42,
		exclusiveMaxMessageID: 43,
	})

	err = store.EnqueueMessage(t.Context(), &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("message"),
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		templateEnqueueMessageWithoutCASQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2EnqueueCachesKnownQueue(t *testing.T) {
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: p.FirstQueueMessageID,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery, TemplateCreateQueueMessageIDRangeQuery, TemplateEnqueueMessageQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetQueueQuery:
			t.Fatal("enqueue should use queue existence cache after CreateQueue")
		case TemplateGetQueueMessageIDRangeQuery, TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return gocql.ErrNotFound
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err = store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
	})
	require.NoError(t, err)
	_, err = store.EnqueueMessage(t.Context(), &p.InternalEnqueueMessageRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
		Blob: &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         queueBytes,
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		TemplateCreateQueueQuery,
		TemplateGetQueueMessageIDRangeQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateCreateQueueMessageIDRangeQuery,
		TemplateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2EnqueueCachesMessageIDRange(t *testing.T) {
	session := &recordingSession{
		t: t,
	}
	getRangeCalls := 0
	createRangeCalls := 0
	insertCalls := 0
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetQueueQuery:
			t.Fatal("enqueue should use queue metadata cache after CreateQueue")
		case TemplateGetQueueMessageIDRangeQuery:
			getRangeCalls++
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return gocql.ErrNotFound
				},
			}
		case TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return gocql.ErrNotFound
				},
			}
		case TemplateCreateQueueMessageIDRangeQuery:
			createRangeCalls++
			require.Equal(t, int64(queueV2MessageIDRangeAllocationSize), args[2])
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateEnqueueMessageQuery:
			insertCalls++
			require.Equal(t, int64(insertCalls-1), args[3])
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err := store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
	})
	require.NoError(t, err)
	for i := range 2 {
		resp, err := store.EnqueueMessage(t.Context(), &p.InternalEnqueueMessageRequest{
			QueueType: p.QueueTypeHistoryNormal,
			QueueName: "test-queue",
			Blob: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         []byte("message"),
			},
		})
		require.NoError(t, err)
		require.Equal(t, int64(i), resp.Metadata.ID)
	}
	require.Equal(t, 1, getRangeCalls)
	require.Equal(t, 1, createRangeCalls)
	require.Equal(t, 2, insertCalls)
	require.Equal(t, []string{
		TemplateCreateQueueQuery,
		TemplateGetQueueMessageIDRangeQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateCreateQueueMessageIDRangeQuery,
		TemplateEnqueueMessageQuery,
		TemplateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2EnqueueSeedsMissingMessageIDRangeFromExistingMessages(t *testing.T) {
	const (
		queueName    = "test-queue"
		maxMessageID = int64(42)
	)
	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetQueueQuery:
			t.Fatal("enqueue should use queue metadata cache after CreateQueue")
		case TemplateGetQueueMessageIDRangeQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return gocql.ErrNotFound
				},
			}
		case TemplateGetMaxMessageIDQuery:
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName, 0}, args)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*int64) = maxMessageID
					return nil
				},
			}
		case TemplateCreateQueueMessageIDRangeQuery:
			require.Equal(t, []any{
				p.QueueTypeHistoryNormal,
				queueName,
				maxMessageID + 1 + queueV2MessageIDRangeAllocationSize,
				int64(0),
			}, args)
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateEnqueueMessageQuery:
			require.Equal(t, maxMessageID+1, args[3])
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err := store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
	})
	require.NoError(t, err)
	resp, err := store.EnqueueMessage(t.Context(), &p.InternalEnqueueMessageRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		Blob: &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte("message"),
		},
	})
	require.NoError(t, err)
	require.Equal(t, maxMessageID+1, resp.Metadata.ID)
	require.Equal(t, []string{
		TemplateCreateQueueQuery,
		TemplateGetQueueMessageIDRangeQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateCreateQueueMessageIDRangeQuery,
		TemplateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2EnqueueMessageIDRangeConflictRefreshesRange(t *testing.T) {
	session := &recordingSession{
		t: t,
	}
	getRangeCalls := 0
	createRangeCalls := 0
	updateRangeCalls := 0
	insertCalls := 0
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetQueueQuery:
			t.Fatal("enqueue should use queue metadata cache after CreateQueue")
		case TemplateGetQueueMessageIDRangeQuery:
			getRangeCalls++
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					switch getRangeCalls {
					case 1:
						return gocql.ErrNotFound
					case 2:
						*dest[0].(*int64) = int64(queueV2MessageIDRangeAllocationSize)
						*dest[1].(*int64) = 0
						return nil
					default:
						t.Fatalf("unexpected message ID range query: %d", getRangeCalls)
						return nil
					}
				},
			}
		case TemplateGetMaxMessageIDQuery:
			require.Equal(t, 1, getRangeCalls)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return gocql.ErrNotFound
				},
			}
		case TemplateCreateQueueMessageIDRangeQuery:
			createRangeCalls++
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return false, nil
				},
			}
		case TemplateUpdateQueueMessageIDRangeQuery:
			updateRangeCalls++
			require.Equal(t, int64(queueV2MessageIDRangeAllocationSize*2), args[0])
			require.Equal(t, int64(1), args[1])
			require.Equal(t, int64(0), args[4])
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateEnqueueMessageQuery:
			insertCalls++
			require.Equal(t, int64(queueV2MessageIDRangeAllocationSize+insertCalls-1), args[3])
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err := store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
	})
	require.NoError(t, err)
	for i := range 2 {
		resp, err := store.EnqueueMessage(t.Context(), &p.InternalEnqueueMessageRequest{
			QueueType: p.QueueTypeHistoryNormal,
			QueueName: "test-queue",
			Blob: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         []byte("message"),
			},
		})
		require.NoError(t, err)
		require.Equal(t, int64(queueV2MessageIDRangeAllocationSize+i), resp.Metadata.ID)
	}
	require.Equal(t, 2, getRangeCalls)
	require.Equal(t, 1, createRangeCalls)
	require.Equal(t, 1, updateRangeCalls)
	require.Equal(t, 2, insertCalls)
	require.Equal(t, []string{
		TemplateCreateQueueQuery,
		TemplateGetQueueMessageIDRangeQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateCreateQueueMessageIDRangeQuery,
		TemplateGetQueueMessageIDRangeQuery,
		TemplateUpdateQueueMessageIDRangeQuery,
		TemplateEnqueueMessageQuery,
		TemplateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2EnqueueReservesNewRangeAfterCachedRangeExhausted(t *testing.T) {
	queueType := p.QueueTypeHistoryNormal
	queueName := "test-queue"
	rangeReads := 0
	updateRangeCalls := 0
	insertCalls := 0
	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			t.Fatal("enqueue should use seeded queue metadata cache")
		case TemplateGetQueueMessageIDRangeQuery:
			rangeReads++
			require.Equal(t, []any{queueType, queueName}, args)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*int64) = int64(queueV2MessageIDRangeAllocationSize)
					*dest[1].(*int64) = int64(7)
					return nil
				},
			}
		case TemplateUpdateQueueMessageIDRangeQuery:
			updateRangeCalls++
			require.Equal(t, []any{
				int64(queueV2MessageIDRangeAllocationSize * 2),
				int64(8),
				queueType,
				queueName,
				int64(7),
			}, args)
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateEnqueueMessageQuery:
			insertCalls++
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	cassandraStore := store.(*queueV2Store)
	key := queueV2Key{queueType: queueType, queueName: queueName}
	cassandraStore.knownQueues.Store(key, &Queue{
		Metadata: &persistencespb.Queue{
			Partitions: map[int32]*persistencespb.QueuePartition{
				0: {
					MinMessageId: p.FirstQueueMessageID,
				},
			},
		},
		Version: int64(1),
	})
	cassandraStore.messageIDRanges.Store(key, queueV2MessageIDRange{
		nextMessageID:         queueV2MessageIDRangeAllocationSize - 1,
		exclusiveMaxMessageID: queueV2MessageIDRangeAllocationSize,
	})

	for i := range 2 {
		resp, err := store.EnqueueMessage(t.Context(), &p.InternalEnqueueMessageRequest{
			QueueType: queueType,
			QueueName: queueName,
			Blob: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         []byte("message"),
			},
		})
		require.NoError(t, err)
		require.Equal(t, int64(queueV2MessageIDRangeAllocationSize-1+i), resp.Metadata.ID)
	}

	require.Equal(t, 1, rangeReads)
	require.Equal(t, 1, updateRangeCalls)
	require.Equal(t, 2, insertCalls)
	require.Equal(t, []string{
		TemplateEnqueueMessageQuery,
		TemplateGetQueueMessageIDRangeQuery,
		TemplateUpdateQueueMessageIDRangeQuery,
		TemplateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
	require.Equal(t, int64(queueV2MessageIDRangeAllocationSize-1), session.queries[0].args[3])
	require.Equal(t, int64(queueV2MessageIDRangeAllocationSize), session.queries[3].args[3])
}

func TestQueueV2EnqueueMessageIDConflictReturnsError(t *testing.T) {
	queueType := p.QueueTypeHistoryNormal
	queueName := "test-queue"
	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			t.Fatal("enqueue should use seeded queue metadata cache")
		case TemplateEnqueueMessageQuery:
			require.Equal(t, []any{
				queueType,
				queueName,
				0,
				int64(42),
				[]byte("message"),
				enumspb.ENCODING_TYPE_PROTO3.String(),
			}, args)
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return false, nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	cassandraStore := store.(*queueV2Store)
	key := queueV2Key{queueType: queueType, queueName: queueName}
	cassandraStore.knownQueues.Store(key, &Queue{
		Metadata: &persistencespb.Queue{
			Partitions: map[int32]*persistencespb.QueuePartition{
				0: {
					MinMessageId: p.FirstQueueMessageID,
				},
			},
		},
		Version: int64(1),
	})
	cassandraStore.messageIDRanges.Store(key, queueV2MessageIDRange{
		nextMessageID:         42,
		exclusiveMaxMessageID: 43,
	})

	_, err := store.EnqueueMessage(t.Context(), &p.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte("message"),
		},
	})
	require.ErrorIs(t, err, ErrEnqueueMessageConflict)
	require.Equal(t, []string{
		TemplateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2EnqueueCanDisableInsertCAS(t *testing.T) {
	queueType := p.QueueTypeHistoryNormal
	queueName := "test-queue"
	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			t.Fatal("enqueue should use seeded queue metadata cache")
		case TemplateEnqueueMessageWithoutCASQuery:
			require.Equal(t, []any{
				queueType,
				queueName,
				0,
				int64(42),
				[]byte("message"),
				enumspb.ENCODING_TYPE_PROTO3.String(),
			}, args)
			return &recordingQuery{
				execFn: func() error {
					return nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger(), true)
	cassandraStore := store.(*queueV2Store)
	key := queueV2Key{queueType: queueType, queueName: queueName}
	cassandraStore.knownQueues.Store(key, &Queue{
		Metadata: &persistencespb.Queue{
			Partitions: map[int32]*persistencespb.QueuePartition{
				0: {
					MinMessageId: p.FirstQueueMessageID,
				},
			},
		},
		Version: int64(1),
	})
	cassandraStore.messageIDRanges.Store(key, queueV2MessageIDRange{
		nextMessageID:         42,
		exclusiveMaxMessageID: 43,
	})

	_, err := store.EnqueueMessage(t.Context(), &p.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte("message"),
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		TemplateEnqueueMessageWithoutCASQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2ConcurrentEnqueueSerializesLocalWriters(t *testing.T) {
	session := &recordingSession{
		t: t,
	}
	insertStarted := make(chan struct{})
	releaseInsert := make(chan struct{})
	getRangeCalls := 0
	createRangeCalls := 0
	insertCalls := 0
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetQueueQuery:
			t.Fatal("enqueue should use queue metadata cache after CreateQueue")
		case TemplateGetQueueMessageIDRangeQuery:
			getRangeCalls++
			require.Equal(t, 1, getRangeCalls)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return gocql.ErrNotFound
				},
			}
		case TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					return gocql.ErrNotFound
				},
			}
		case TemplateCreateQueueMessageIDRangeQuery:
			createRangeCalls++
			require.Equal(t, 1, createRangeCalls)
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateEnqueueMessageQuery:
			insertCalls++
			require.Equal(t, int64(insertCalls-1), args[3])
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					if insertCalls == 1 {
						close(insertStarted)
						<-releaseInsert
					}
					return true, nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err := store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	enqueue := func() {
		defer wg.Done()
		_, err := store.EnqueueMessage(t.Context(), &p.InternalEnqueueMessageRequest{
			QueueType: p.QueueTypeHistoryNormal,
			QueueName: "test-queue",
			Blob: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         []byte("message"),
			},
		})
		errs <- err
	}
	wg.Add(1)
	go enqueue()
	<-insertStarted
	wg.Add(1)
	go enqueue()
	close(releaseInsert)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1, getRangeCalls)
	require.Equal(t, 1, createRangeCalls)
	require.Equal(t, 2, insertCalls)
	require.Equal(t, []string{
		TemplateCreateQueueQuery,
		TemplateGetQueueMessageIDRangeQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateCreateQueueMessageIDRangeQuery,
		TemplateEnqueueMessageQuery,
		TemplateEnqueueMessageQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2ReadMessagesCachesKnownQueue(t *testing.T) {
	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetQueueQuery:
			t.Fatal("read should use queue metadata cache after CreateQueue")
		case TemplateGetMessagesQuery:
			return &recordingQuery{
				iter: &recordingIter{},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err := store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
	})
	require.NoError(t, err)
	_, err = store.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: "test-queue",
		PageSize:  100,
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		TemplateCreateQueueQuery,
		TemplateGetMessagesQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2RangeDeleteUpdatesCachedQueue(t *testing.T) {
	const queueName = "test-queue"
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: p.FirstQueueMessageID,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
	}
	getQueueCalls := 0
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			getQueueCalls++
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName}, args)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*[]byte) = queueBytes
					*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
					*dest[2].(*int64) = 0
					return nil
				},
			}
		case TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 3
					return nil
				},
			}
		case TemplateRangeDeleteMessagesQuery:
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName, 0, int64(0), int64(1)}, args)
			return &recordingQuery{}
		case TemplateUpdateQueueMetadataQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetMessagesQuery:
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName, 0, int64(2), 100}, args)
			return &recordingQuery{
				iter: &recordingIter{},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	resp, err := store.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), resp.MessagesDeleted)
	_, err = store.ReadMessages(t.Context(), &p.InternalReadMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		PageSize:  100,
	})
	require.NoError(t, err)
	require.Equal(t, 1, getQueueCalls)
	require.Equal(t, []string{
		TemplateGetQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateRangeDeleteMessagesQuery,
		TemplateUpdateQueueMetadataQuery,
		TemplateGetMessagesQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2RangeDeleteUsesCachedQueue(t *testing.T) {
	const queueName = "test-queue"
	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateCreateQueueQuery, TemplateUpdateQueueMetadataQuery:
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return true, nil
				},
			}
		case TemplateGetQueueQuery:
			t.Fatal("range delete should use queue metadata cache after CreateQueue")
		case TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 3
					return nil
				},
			}
		case TemplateRangeDeleteMessagesQuery:
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName, 0, int64(0), int64(1)}, args)
			return &recordingQuery{}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err := store.CreateQueue(t.Context(), &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
	})
	require.NoError(t, err)
	resp, err := store.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), resp.MessagesDeleted)
	require.Equal(t, []string{
		TemplateCreateQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateRangeDeleteMessagesQuery,
		TemplateUpdateQueueMetadataQuery,
	}, recordedStatements(session.queries))
}

func TestQueueV2RangeDeleteBelowMinSkipsMaxMessageIDRead(t *testing.T) {
	const queueName = "test-queue"
	queueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: 10,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
	}
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			require.Equal(t, []any{p.QueueTypeHistoryNormal, queueName}, args)
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*[]byte) = queueBytes
					*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
					*dest[2].(*int64) = 3
					return nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	resp, err := store.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 9,
		},
	})
	require.NoError(t, err)
	require.Zero(t, resp.MessagesDeleted)
	require.Equal(t, []string{TemplateGetQueueQuery}, recordedStatements(session.queries))
}

func TestQueueV2UpdateConflictInvalidatesCachedQueue(t *testing.T) {
	const queueName = "test-queue"
	initialQueueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: p.FirstQueueMessageID,
			},
		},
	}).Marshal()
	require.NoError(t, err)
	refreshedQueueBytes, err := (&persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: 2,
			},
		},
	}).Marshal()
	require.NoError(t, err)

	session := &recordingSession{
		t: t,
	}
	getQueueCalls := 0
	updateCalls := 0
	session.queryFn = func(stmt string, args ...any) cgocql.Query {
		switch stmt {
		case TemplateGetQueueQuery:
			getQueueCalls++
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					switch getQueueCalls {
					case 1:
						*dest[0].(*[]byte) = initialQueueBytes
						*dest[2].(*int64) = 0
					case 2:
						*dest[0].(*[]byte) = refreshedQueueBytes
						*dest[2].(*int64) = 1
					default:
						t.Fatalf("unexpected get queue call: %d", getQueueCalls)
					}
					*dest[1].(*string) = enumspb.ENCODING_TYPE_PROTO3.String()
					return nil
				},
			}
		case TemplateGetMaxMessageIDQuery:
			return &recordingQuery{
				scanFn: func(dest ...any) error {
					*dest[0].(*int64) = 4
					return nil
				},
			}
		case TemplateRangeDeleteMessagesQuery:
			return &recordingQuery{}
		case TemplateUpdateQueueMetadataQuery:
			updateCalls++
			return &recordingQuery{
				mapScanCASFn: func(map[string]any) (bool, error) {
					return updateCalls == 2, nil
				},
			}
		default:
			t.Fatalf("unexpected query: %s", stmt)
		}
		return nil
	}

	store := NewQueueV2Store(session, log.NewNoopLogger())
	_, err = store.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 1,
		},
	})
	require.ErrorIs(t, err, ErrUpdateQueueConflict)
	_, err = store.RangeDeleteMessages(t.Context(), &p.InternalRangeDeleteMessagesRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: p.MessageMetadata{
			ID: 2,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, getQueueCalls)
	require.Equal(t, 2, updateCalls)
	require.Equal(t, []string{
		TemplateGetQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateRangeDeleteMessagesQuery,
		TemplateUpdateQueueMetadataQuery,
		TemplateGetQueueQuery,
		TemplateGetMaxMessageIDQuery,
		TemplateRangeDeleteMessagesQuery,
		TemplateUpdateQueueMetadataQuery,
	}, recordedStatements(session.queries))
}

type recordedQuery struct {
	stmt  string
	args  []any
	query *recordingQuery
}

type recordingSession struct {
	t       *testing.T
	queryFn func(stmt string, args ...any) cgocql.Query
	queries []recordedQuery
}

func (s *recordingSession) Query(stmt string, args ...any) cgocql.Query {
	q := s.queryFn(stmt, args...)
	rq, ok := q.(*recordingQuery)
	require.True(s.t, ok)
	s.queries = append(s.queries, recordedQuery{stmt: stmt, args: args, query: rq})
	return q
}

func (s *recordingSession) NewBatch(cgocql.BatchType) *cgocql.Batch {
	s.t.Fatal("unexpected NewBatch")
	return nil
}

func (s *recordingSession) ExecuteBatch(*cgocql.Batch) error {
	s.t.Fatal("unexpected ExecuteBatch")
	return nil
}

func (s *recordingSession) MapExecuteBatchCAS(*cgocql.Batch, map[string]any) (bool, cgocql.Iter, error) {
	s.t.Fatal("unexpected MapExecuteBatchCAS")
	return false, nil, nil
}

func (s *recordingSession) AwaitSchemaAgreement(context.Context) error {
	s.t.Fatal("unexpected AwaitSchemaAgreement")
	return nil
}

func (s *recordingSession) Close() {}

type recordingQuery struct {
	iter         cgocql.Iter
	iterFn       func(*recordingQuery) cgocql.Iter
	pageSize     int
	pageState    []byte
	execFn       func() error
	scanFn       func(dest ...any) error
	mapScanFn    func(map[string]any) error
	mapScanCASFn func(map[string]any) (bool, error)
}

func (q *recordingQuery) Exec() error {
	if q.execFn != nil {
		return q.execFn()
	}
	return nil
}

func (q *recordingQuery) Scan(dest ...any) error {
	return q.scanFn(dest...)
}

func (q *recordingQuery) ScanCAS(...any) (bool, error) {
	return false, nil
}

func (q *recordingQuery) MapScan(dest map[string]any) error {
	if q.mapScanFn != nil {
		return q.mapScanFn(dest)
	}
	return nil
}

func (q *recordingQuery) MapScanCAS(dest map[string]any) (bool, error) {
	if q.mapScanCASFn != nil {
		return q.mapScanCASFn(dest)
	}
	return false, nil
}

func (q *recordingQuery) Iter() cgocql.Iter {
	if q.iterFn != nil {
		return q.iterFn(q)
	}
	return q.iter
}

func (q *recordingQuery) PageSize(pageSize int) cgocql.Query {
	q.pageSize = pageSize
	return q
}

func (q *recordingQuery) PageState(pageState []byte) cgocql.Query {
	q.pageState = pageState
	return q
}

func (q *recordingQuery) WithContext(context.Context) cgocql.Query {
	return q
}

func (q *recordingQuery) WithTimestamp(int64) cgocql.Query {
	return q
}

func (q *recordingQuery) Consistency(cgocql.Consistency) cgocql.Query {
	return q
}

func (q *recordingQuery) Bind(...any) cgocql.Query {
	return q
}

func (q *recordingQuery) Idempotent(bool) cgocql.Query {
	return q
}

func (q *recordingQuery) SetSpeculativeExecutionPolicy(cgocql.SpeculativeExecutionPolicy) cgocql.Query {
	return q
}

func recordedStatements(queries []recordedQuery) []string {
	statements := make([]string, len(queries))
	for i, query := range queries {
		statements[i] = query.stmt
	}
	return statements
}

func recordGetClusterMembersQuery(t *testing.T, request *p.GetClusterMembersRequest) string {
	t.Helper()

	session := &recordingSession{
		t: t,
		queryFn: func(string, ...any) cgocql.Query {
			return &recordingQuery{
				iter: &recordingIter{},
			}
		},
	}
	store := &ClusterMetadataStore{session: session, logger: log.NewNoopLogger()}

	_, err := store.GetClusterMembers(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, session.queries, 1)
	return session.queries[0].stmt
}

type recordingIter struct {
	scanRows                [][]any
	mapRows                 []map[string]any
	pageState               []byte
	pageStateAfterExhausted bool
	scanIdx                 int
	mapIdx                  int
	closeErr                error
	closeCalls              int
}

func (i *recordingIter) Scan(dest ...any) bool {
	if i.scanIdx >= len(i.scanRows) {
		return false
	}
	row := i.scanRows[i.scanIdx]
	i.scanIdx++
	for idx := range dest {
		switch d := dest[idx].(type) {
		case *string:
			*d = row[idx].(string)
		case *[]byte:
			*d = row[idx].([]byte)
		case *int64:
			*d = row[idx].(int64)
		default:
			panic("unsupported scan destination")
		}
	}
	return true
}

func (i *recordingIter) MapScan(dest map[string]any) bool {
	if i.mapIdx >= len(i.mapRows) {
		return false
	}
	for key, value := range i.mapRows[i.mapIdx] {
		dest[key] = value
	}
	i.mapIdx++
	return true
}

func (i *recordingIter) PageState() []byte {
	if i.pageStateAfterExhausted && (i.scanIdx < len(i.scanRows) || i.mapIdx < len(i.mapRows)) {
		return nil
	}
	return i.pageState
}

func (i *recordingIter) Close() error {
	i.closeCalls++
	return i.closeErr
}
