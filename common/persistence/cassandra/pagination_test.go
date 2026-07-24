package cassandra

import (
	"context"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	cgocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
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
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateListEndpointsFirstPageQuery, stmt)
			return &recordingQuery{
				iter: &recordingIter{
					mapRows: []map[string]any{
						{
							"version": int64(2),
						},
					},
				},
			}
		},
	}
	store := &NexusEndpointStore{session: session}

	_, err := store.ListNexusEndpoints(t.Context(), &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: 1,
		PageSize:              10,
	})

	require.ErrorIs(t, err, p.ErrNexusTableVersionConflict)
}

func TestListQueuesDropsRepeatedEmptyPageToken(t *testing.T) {
	token := []byte("same-page-token")
	session := &recordingSession{
		t: t,
		queryFn: func(stmt string, args ...any) cgocql.Query {
			require.Equal(t, templateGetQueueNamesQuery, stmt)
			return &recordingQuery{
				iter: &recordingIter{
					pageState: token,
				},
			}
		},
	}
	store := &queueV2Store{session: session}

	resp, err := store.ListQueues(t.Context(), &p.InternalListQueuesRequest{
		QueueType:     p.QueueTypeHistoryDLQ,
		PageSize:      10,
		NextPageToken: token,
	})

	require.NoError(t, err)
	require.Empty(t, resp.Queues)
	require.Empty(t, resp.NextPageToken)
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
	iter      cgocql.Iter
	pageSize  int
	pageState []byte
	scanFn    func(dest ...any) error
}

func (q *recordingQuery) Exec() error {
	return nil
}

func (q *recordingQuery) Scan(dest ...any) error {
	return q.scanFn(dest...)
}

func (q *recordingQuery) ScanCAS(...any) (bool, error) {
	return false, nil
}

func (q *recordingQuery) MapScan(map[string]any) error {
	return nil
}

func (q *recordingQuery) MapScanCAS(map[string]any) (bool, error) {
	return false, nil
}

func (q *recordingQuery) Iter() cgocql.Iter {
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

type recordingIter struct {
	scanRows  [][]any
	mapRows   []map[string]any
	pageState []byte
	scanIdx   int
	mapIdx    int
	closeErr  error
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
	return i.pageState
}

func (i *recordingIter) Close() error {
	return i.closeErr
}
