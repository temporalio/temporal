package queues

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

type (
	convertSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestConvertSuite(t *testing.T) {
	s := new(convertSuite)
	suite.Run(t, s)
}

func (s *convertSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *convertSuite) TestConvertPredicate_All() {
	predicate := predicates.Universal[tasks.Task]()
	s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate), nil))
}

func (s *convertSuite) TestConvertPredicate_Empty() {
	predicate := predicates.Empty[tasks.Task]()
	s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate), nil))
}

func (s *convertSuite) TestConvertPredicate_And() {
	testCases := []tasks.Predicate{
		predicates.And(
			predicates.Universal[tasks.Task](),
			predicates.Empty[tasks.Task](),
		),
		predicates.And(
			predicates.Or[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.NewString()}),
				tasks.NewNamespacePredicate([]string{uuid.NewString()}),
			),
			predicates.Or[tasks.Task](
				tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
					{TaskGroup: "g1", NamespaceID: "n1", Destination: "d1"},
				}),
				tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
					{TaskGroup: "g2", NamespaceID: "n2", Destination: "d2"},
				}),
			),
		),
		predicates.And(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.NewString()}),
				tasks.NewNamespacePredicate([]string{uuid.NewString()}),
			),
		),
		predicates.And(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.NewString()}),
				tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
					{TaskGroup: "g1", NamespaceID: "n1", Destination: "d1"},
				}),
			),
		),
	}

	for _, predicate := range testCases {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate), nil))
	}
}

func (s *convertSuite) TestConvertPredicate_Or() {
	testCases := []tasks.Predicate{
		predicates.Or(
			predicates.Universal[tasks.Task](),
			predicates.Empty[tasks.Task](),
		),
		predicates.Or(
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.NewString()}),
				tasks.NewNamespacePredicate([]string{uuid.NewString()}),
			),
			predicates.And[tasks.Task](
				tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
					{TaskGroup: "g1", NamespaceID: "n1", Destination: "d1"},
				}),
				tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
					{TaskGroup: "g2", NamespaceID: "n2", Destination: "d2"},
				}),
			),
		),
		predicates.Or(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.NewString()}),
				tasks.NewNamespacePredicate([]string{uuid.NewString()}),
			),
		),
		predicates.Or(
			predicates.Not(predicates.Empty[tasks.Task]()),
			predicates.And[tasks.Task](
				tasks.NewNamespacePredicate([]string{uuid.NewString()}),
				tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
					{TaskGroup: "g1", NamespaceID: "n1", Destination: "d1"},
				}),
			),
		),
	}

	for _, predicate := range testCases {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate), nil))
	}
}

func (s *convertSuite) TestConvertPredicate_Not() {
	testCases := []tasks.Predicate{
		predicates.Not(predicates.Universal[tasks.Task]()),
		predicates.Not(predicates.Empty[tasks.Task]()),
		predicates.Not(predicates.And[tasks.Task](
			tasks.NewNamespacePredicate([]string{uuid.NewString()}),
			tasks.NewOutboundTaskPredicate(nil),
		)),
		predicates.Not(predicates.Or[tasks.Task](
			tasks.NewNamespacePredicate([]string{uuid.NewString()}),
			tasks.NewOutboundTaskPredicate(nil),
		)),
		predicates.Not(predicates.Not(predicates.Empty[tasks.Task]())),
		predicates.Not[tasks.Task](tasks.NewNamespacePredicate([]string{uuid.NewString()})),
		predicates.Not[tasks.Task](tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
			{TaskGroup: "g1", NamespaceID: "n1", Destination: "d1"},
		})),
	}

	for _, predicate := range testCases {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate), nil))
	}
}

func (s *convertSuite) TestConvertPredicate_NamespaceID() {
	testCases := []tasks.Predicate{
		tasks.NewNamespacePredicate(nil),
		tasks.NewNamespacePredicate([]string{}),
		tasks.NewNamespacePredicate([]string{uuid.NewString(), uuid.NewString(), uuid.NewString()}),
	}

	for _, predicate := range testCases {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate), nil))
	}
}

func (s *convertSuite) TestConvertPredicate_TaskType() {
	testCases := []tasks.Predicate{
		tasks.NewTypePredicate(nil),
		tasks.NewTypePredicate([]enumsspb.TaskType{}),
		tasks.NewTypePredicate([]enumsspb.TaskType{
			enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
			enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		}),
	}

	for _, predicate := range testCases {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate), nil))
	}
}

func (s *convertSuite) TestConvertTaskKey() {
	key := NewRandomKey()
	s.Equal(key, FromPersistenceTaskKey(
		ToPersistenceTaskKey(key),
	))
}

func (s *convertSuite) TestConvertTaskRange() {
	r := NewRandomRange()
	s.Equal(r, FromPersistenceRange(
		ToPersistenceRange(r),
	))
}

func (s *convertSuite) TestConvertScope() {
	scope := NewScope(
		NewRandomRange(),
		tasks.NewNamespacePredicate([]string{uuid.NewString(), uuid.NewString()}),
	)

	s.Equal(scope, FromPersistenceScope(
		ToPersistenceScope(scope),
		nil,
	))
}

func TestFromPersistenceScope_ResolvesIndexes(t *testing.T) {
	first := uuid.New()
	second := uuid.New()
	table := newNamespaceIDTable([][]byte{first[:], second[:]})
	r := NewRandomRange()

	persisted := &persistencespb.QueueSliceScope{
		Range: ToPersistenceRange(r),
		Predicate: &persistencespb.Predicate{
			PredicateType: enumsspb.PREDICATE_TYPE_NAMESPACE_ID,
			Attributes: &persistencespb.Predicate_NamespaceIdPredicateAttributes{
				NamespaceIdPredicateAttributes: &persistencespb.NamespaceIdPredicateAttributes{
					NamespaceIdIndexes: []int32{0, 1},
				},
			},
		},
	}

	want := NewScope(r, tasks.NewNamespacePredicate([]string{first.String(), second.String()}))
	require.Equal(t, want, FromPersistenceScope(persisted, table))
}

func namespaceIDPredicateProto(
	attributes *persistencespb.NamespaceIdPredicateAttributes,
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_NAMESPACE_ID,
		Attributes: &persistencespb.Predicate_NamespaceIdPredicateAttributes{
			NamespaceIdPredicateAttributes: attributes,
		},
	}
}

func TestFromPersistenceQueueState_ResolvesNamespaceIDTable(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	inline := uuid.NewString()
	r0, r1, r2 := NewRandomRange(), NewRandomRange(), NewRandomRange()
	watermark := tasks.NewKey(time.Unix(0, rand.Int63()).UTC(), 0)

	tests := []struct {
		name  string
		state *persistencespb.QueueState
		want  map[int64][]Scope
	}{
		{
			name: "indexes resolve against the table across readers",
			state: &persistencespb.QueueState{
				NamespaceIdTable:             [][]byte{a[:], b[:], c[:]},
				ExclusiveReaderHighWatermark: ToPersistenceTaskKey(watermark),
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {Scopes: []*persistencespb.QueueSliceScope{
						{
							Range: ToPersistenceRange(r0),
							Predicate: namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
								NamespaceIdIndexes: []int32{0, 1},
							}),
						},
						{
							Range: ToPersistenceRange(r1),
							Predicate: namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
								NamespaceIdIndexes: []int32{2},
							}),
						},
					}},
					1: {Scopes: []*persistencespb.QueueSliceScope{
						{
							Range: ToPersistenceRange(r2),
							Predicate: namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
								NamespaceIds:       []string{inline},
								NamespaceIdIndexes: []int32{1},
							}),
						},
					}},
				},
			},
			want: map[int64][]Scope{
				0: {
					NewScope(r0, tasks.NewNamespacePredicate([]string{a.String(), b.String()})),
					NewScope(r1, tasks.NewNamespacePredicate([]string{c.String()})),
				},
				1: {
					NewScope(r2, tasks.NewNamespacePredicate([]string{inline, b.String()})),
				},
			},
		},
		{
			name: "no table, inline strings only",
			state: &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: ToPersistenceTaskKey(watermark),
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {Scopes: []*persistencespb.QueueSliceScope{
						{
							Range: ToPersistenceRange(r0),
							Predicate: namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
								NamespaceIds: []string{inline},
							}),
						},
					}},
				},
			},
			want: map[int64][]Scope{
				0: {NewScope(r0, tasks.NewNamespacePredicate([]string{inline}))},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t,
				&queueState{readerScopes: tc.want, exclusiveReaderHighWatermark: watermark},
				FromPersistenceQueueState(tc.state),
			)
		})
	}
}

func (s *convertSuite) TestConvertQueueState() {
	readerScopes := map[int64][]Scope{
		0: {},
		1: {
			NewScope(
				NewRandomRange(),
				tasks.NewNamespacePredicate([]string{uuid.NewString(), uuid.NewString()}),
			),
		},
		123: {
			NewScope(
				NewRandomRange(),
				tasks.NewNamespacePredicate([]string{uuid.NewString(), uuid.NewString()}),
			),
			NewScope(
				NewRandomRange(),
				tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
					{TaskGroup: "g1", NamespaceID: "n1", Destination: "d1"},
					{TaskGroup: "g2", NamespaceID: "n2", Destination: "d2"},
				}),
			),
		},
	}

	queueState := &queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: tasks.NewKey(time.Unix(0, rand.Int63()).UTC(), 0),
	}

	s.Equal(queueState, FromPersistenceQueueState(
		ToPersistenceQueueState(queueState),
	))
}

func TestNamespaceIDTable_Resolve(t *testing.T) {
	first := uuid.New()
	second := uuid.New()
	populated := newNamespaceIDTable([][]byte{first[:], second[:]})
	malformed := newNamespaceIDTable([][]byte{first[:15]})

	tests := []struct {
		name   string
		table  *namespaceIDTable
		index  int32
		want   string
		wantOK bool
	}{
		{name: "nil table", table: nil, index: 0},
		{name: "empty table", table: newNamespaceIDTable(nil), index: 0},
		{name: "first entry", table: populated, index: 0, want: first.String(), wantOK: true},
		{name: "second entry", table: populated, index: 1, want: second.String(), wantOK: true},
		{name: "index past the end", table: populated, index: 2},
		{name: "negative index", table: populated, index: -1},
		{name: "entry shorter than 16 bytes", table: malformed, index: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := tc.table.resolve(tc.index)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestFromPersistenceNamespaceIDPredicate_ResolvesIndexes(t *testing.T) {
	inline := uuid.NewString()
	first := uuid.New()
	second := uuid.New()
	table := newNamespaceIDTable([][]byte{first[:], second[:]})

	tests := []struct {
		name       string
		attributes *persistencespb.NamespaceIdPredicateAttributes
		table      *namespaceIDTable
		want       []string
	}{
		{
			name:       "strings only",
			attributes: &persistencespb.NamespaceIdPredicateAttributes{NamespaceIds: []string{inline}},
			table:      nil,
			want:       []string{inline},
		},
		{
			name:       "indexes only",
			attributes: &persistencespb.NamespaceIdPredicateAttributes{NamespaceIdIndexes: []int32{1, 0}},
			table:      table,
			want:       []string{first.String(), second.String()},
		},
		{
			name: "strings and indexes",
			attributes: &persistencespb.NamespaceIdPredicateAttributes{
				NamespaceIds:       []string{inline},
				NamespaceIdIndexes: []int32{1},
			},
			table: table,
			want:  []string{inline, second.String()},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t,
				tasks.NewNamespacePredicate(tc.want),
				FromPersistenceNamespaceIDPredicate(tc.attributes, tc.table),
			)
		})
	}
}

func TestFromPersistencePredicate_ResolvesIndexesInCompositePredicates(t *testing.T) {
	first := uuid.New()
	second := uuid.New()
	table := newNamespaceIDTable([][]byte{first[:], second[:]})

	indexed := func(index int32) *persistencespb.Predicate {
		return &persistencespb.Predicate{
			PredicateType: enumsspb.PREDICATE_TYPE_NAMESPACE_ID,
			Attributes: &persistencespb.Predicate_NamespaceIdPredicateAttributes{
				NamespaceIdPredicateAttributes: &persistencespb.NamespaceIdPredicateAttributes{
					NamespaceIdIndexes: []int32{index},
				},
			},
		}
	}
	not := func(p *persistencespb.Predicate) *persistencespb.Predicate {
		return &persistencespb.Predicate{
			PredicateType: enumsspb.PREDICATE_TYPE_NOT,
			Attributes: &persistencespb.Predicate_NotPredicateAttributes{
				NotPredicateAttributes: &persistencespb.NotPredicateAttributes{Predicate: p},
			},
		}
	}
	and := func(ps ...*persistencespb.Predicate) *persistencespb.Predicate {
		return &persistencespb.Predicate{
			PredicateType: enumsspb.PREDICATE_TYPE_AND,
			Attributes: &persistencespb.Predicate_AndPredicateAttributes{
				AndPredicateAttributes: &persistencespb.AndPredicateAttributes{Predicates: ps},
			},
		}
	}
	or := func(ps ...*persistencespb.Predicate) *persistencespb.Predicate {
		return &persistencespb.Predicate{
			PredicateType: enumsspb.PREDICATE_TYPE_OR,
			Attributes: &persistencespb.Predicate_OrPredicateAttributes{
				OrPredicateAttributes: &persistencespb.OrPredicateAttributes{Predicates: ps},
			},
		}
	}

	firstNamespace := tasks.NewNamespacePredicate([]string{first.String()})
	secondNamespace := tasks.NewNamespacePredicate([]string{second.String()})

	tests := []struct {
		name      string
		predicate *persistencespb.Predicate
		want      tasks.Predicate
	}{
		{
			name:      "not",
			predicate: not(indexed(0)),
			want:      predicates.Not[tasks.Task](firstNamespace),
		},
		{
			name:      "and",
			predicate: and(indexed(0), indexed(1)),
			want:      predicates.And[tasks.Task](firstNamespace, secondNamespace),
		},
		{
			name:      "or",
			predicate: or(indexed(0), indexed(1)),
			want:      predicates.Or[tasks.Task](firstNamespace, secondNamespace),
		},
		{
			name:      "not wrapping and",
			predicate: not(and(indexed(0), indexed(1))),
			want:      predicates.Not[tasks.Task](predicates.And[tasks.Task](firstNamespace, secondNamespace)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, FromPersistencePredicate(tc.predicate, table))
		})
	}
}
