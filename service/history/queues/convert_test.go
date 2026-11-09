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
	"google.golang.org/protobuf/proto"
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
	s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate, nil), nil))
}

func (s *convertSuite) TestConvertPredicate_Empty() {
	predicate := predicates.Empty[tasks.Task]()
	s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate, nil), nil))
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
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate, nil), nil))
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
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate, nil), nil))
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
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate, nil), nil))
	}
}

func (s *convertSuite) TestConvertPredicate_NamespaceID() {
	testCases := []tasks.Predicate{
		tasks.NewNamespacePredicate(nil),
		tasks.NewNamespacePredicate([]string{}),
		tasks.NewNamespacePredicate([]string{uuid.NewString(), uuid.NewString(), uuid.NewString()}),
	}

	for _, predicate := range testCases {
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate, nil), nil))
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
		s.Equal(predicate, FromPersistencePredicate(ToPersistencePredicate(predicate, nil), nil))
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

	got, _ := FromPersistenceScope(ToPersistenceScope(scope, nil), nil)
	s.Equal(scope, got)
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
	got, _ := FromPersistenceScope(persisted, table)
	require.Equal(t, want, got)
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

func notPredicateProto(p *persistencespb.Predicate) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_NOT,
		Attributes: &persistencespb.Predicate_NotPredicateAttributes{
			NotPredicateAttributes: &persistencespb.NotPredicateAttributes{Predicate: p},
		},
	}
}

func andPredicateProto(ps ...*persistencespb.Predicate) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_AND,
		Attributes: &persistencespb.Predicate_AndPredicateAttributes{
			AndPredicateAttributes: &persistencespb.AndPredicateAttributes{Predicates: ps},
		},
	}
}

func TestFromPersistenceScope_UnresolvableIndexFallsBackToUniversal(t *testing.T) {
	known := uuid.New()
	table := newNamespaceIDTable([][]byte{known[:]})
	resolvable := namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
		NamespaceIdIndexes: []int32{0},
	})
	unresolvable := namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
		NamespaceIdIndexes: []int32{5},
	})
	universal := predicates.Universal[tasks.Task]()
	r := NewRandomRange()

	tests := []struct {
		name         string
		predicate    *persistencespb.Predicate
		table        *namespaceIDTable
		want         tasks.Predicate
		wantReadLoss bool
	}{
		{
			name:         "bare leaf",
			predicate:    unresolvable,
			table:        table,
			want:         universal,
			wantReadLoss: true,
		},
		{
			name:         "inside not",
			predicate:    notPredicateProto(unresolvable),
			table:        table,
			want:         universal,
			wantReadLoss: true,
		},
		{
			name:         "inside and, next to a resolvable sibling",
			predicate:    andPredicateProto(resolvable, unresolvable),
			table:        table,
			want:         universal,
			wantReadLoss: true,
		},
		{
			name:         "all indexes resolvable",
			predicate:    resolvable,
			table:        table,
			want:         tasks.NewNamespacePredicate([]string{known.String()}),
			wantReadLoss: false,
		},
		{
			// FromPersistenceQueueState always builds a table, so this is not a production path.
			// A nil table cannot count lookups, so its unresolved indexes are dropped, not given the universal predicate.
			name:         "nil table is not detected",
			predicate:    unresolvable,
			table:        nil,
			want:         tasks.NewNamespacePredicate(nil),
			wantReadLoss: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			persisted := &persistencespb.QueueSliceScope{
				Range:     ToPersistenceRange(r),
				Predicate: tc.predicate,
			}

			got, lostResolutionOnRead := FromPersistenceScope(persisted, tc.table)

			require.Equal(t, NewScope(r, tc.want), got)
			require.Equal(t, tc.wantReadLoss, lostResolutionOnRead)
		})
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

func TestFromPersistenceQueueState_CountsScopesThatLostResolutionOnRead(t *testing.T) {
	known := uuid.New()
	r0, r1, r2, r3 := NewRandomRange(), NewRandomRange(), NewRandomRange(), NewRandomRange()
	watermark := tasks.NewKey(time.Unix(0, rand.Int63()).UTC(), 0)
	resolvable := namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
		NamespaceIdIndexes: []int32{0},
	})
	oneUnresolvable := namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
		NamespaceIdIndexes: []int32{7},
	})
	twoUnresolvable := namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
		NamespaceIdIndexes: []int32{8, 9},
	})

	state := &persistencespb.QueueState{
		NamespaceIdTable:             [][]byte{known[:]},
		ExclusiveReaderHighWatermark: ToPersistenceTaskKey(watermark),
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			0: {Scopes: []*persistencespb.QueueSliceScope{
				{Range: ToPersistenceRange(r0), Predicate: resolvable},
				{Range: ToPersistenceRange(r1), Predicate: twoUnresolvable},
			}},
			1: {Scopes: []*persistencespb.QueueSliceScope{
				{Range: ToPersistenceRange(r2), Predicate: oneUnresolvable},
				{Range: ToPersistenceRange(r3), Predicate: resolvable},
			}},
		},
	}

	got := FromPersistenceQueueState(state)

	universal := predicates.Universal[tasks.Task]()
	knownPredicate := tasks.NewNamespacePredicate([]string{known.String()})
	require.Equal(t, map[int64][]Scope{
		0: {NewScope(r0, knownPredicate), NewScope(r1, universal)},
		1: {NewScope(r2, universal), NewScope(r3, knownPredicate)},
	}, got.readerScopes)
	require.Equal(t, 2, got.lostResolutionOnReadCount)
}

func TestFromPersistenceQueueState_ExpandsPredicateGroups(t *testing.T) {
	ns := uuid.NewString()
	r0 := NewRange(tasks.NewImmediateKey(100), tasks.NewImmediateKey(200))
	r1 := NewRange(tasks.NewImmediateKey(200), tasks.NewImmediateKey(300))
	r2 := NewRange(tasks.NewImmediateKey(300), tasks.NewImmediateKey(400))
	r3 := NewRange(tasks.NewImmediateKey(400), tasks.NewImmediateKey(500))
	otherNS := uuid.NewString()
	watermark := tasks.NewImmediateKey(1000)
	groupPredicate := namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
		NamespaceIds: []string{ns},
	})
	otherGroupPredicate := namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
		NamespaceIds: []string{otherNS},
	})

	tests := []struct {
		name   string
		reader *persistencespb.QueueReaderState
		want   []Scope
	}{
		{
			name: "group only",
			reader: &persistencespb.QueueReaderState{
				PredicateGroups: []*persistencespb.QueueSlicePredicateGroup{
					{
						Predicate: groupPredicate,
						Ranges:    []*persistencespb.QueueSliceRange{ToPersistenceRange(r0), ToPersistenceRange(r1)},
					},
				},
			},
			want: []Scope{
				NewScope(r0, tasks.NewNamespacePredicate([]string{ns})),
				NewScope(r1, tasks.NewNamespacePredicate([]string{ns})),
			},
		},
		{
			name: "group ranges straddle a plain scope",
			reader: &persistencespb.QueueReaderState{
				Scopes: []*persistencespb.QueueSliceScope{
					{Range: ToPersistenceRange(r1), Predicate: otherGroupPredicate},
				},
				PredicateGroups: []*persistencespb.QueueSlicePredicateGroup{
					{
						Predicate: groupPredicate,
						Ranges:    []*persistencespb.QueueSliceRange{ToPersistenceRange(r0), ToPersistenceRange(r2)},
					},
				},
			},
			want: []Scope{
				NewScope(r0, tasks.NewNamespacePredicate([]string{ns})),
				NewScope(r1, tasks.NewNamespacePredicate([]string{otherNS})),
				NewScope(r2, tasks.NewNamespacePredicate([]string{ns})),
			},
		},
		{
			name: "two groups interleave",
			reader: &persistencespb.QueueReaderState{
				PredicateGroups: []*persistencespb.QueueSlicePredicateGroup{
					{
						Predicate: groupPredicate,
						Ranges:    []*persistencespb.QueueSliceRange{ToPersistenceRange(r0), ToPersistenceRange(r2)},
					},
					{
						Predicate: otherGroupPredicate,
						Ranges:    []*persistencespb.QueueSliceRange{ToPersistenceRange(r1), ToPersistenceRange(r3)},
					},
				},
			},
			want: []Scope{
				NewScope(r0, tasks.NewNamespacePredicate([]string{ns})),
				NewScope(r1, tasks.NewNamespacePredicate([]string{otherNS})),
				NewScope(r2, tasks.NewNamespacePredicate([]string{ns})),
				NewScope(r3, tasks.NewNamespacePredicate([]string{otherNS})),
			},
		},
		{
			name: "ranges listed out of order within a group",
			reader: &persistencespb.QueueReaderState{
				PredicateGroups: []*persistencespb.QueueSlicePredicateGroup{
					{
						Predicate: groupPredicate,
						Ranges:    []*persistencespb.QueueSliceRange{ToPersistenceRange(r2), ToPersistenceRange(r0)},
					},
				},
			},
			want: []Scope{
				NewScope(r0, tasks.NewNamespacePredicate([]string{ns})),
				NewScope(r2, tasks.NewNamespacePredicate([]string{ns})),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: ToPersistenceTaskKey(watermark),
				ReaderStates:                 map[int64]*persistencespb.QueueReaderState{0: tc.reader},
			}

			got := FromPersistenceQueueState(state)

			require.Equal(t, map[int64][]Scope{0: tc.want}, got.readerScopes)
		})
	}
}

func TestFromPersistenceQueueState_GroupWithUnresolvableIndexCountsEachScope(t *testing.T) {
	known := uuid.New()
	r0 := NewRange(tasks.NewImmediateKey(100), tasks.NewImmediateKey(200))
	r1 := NewRange(tasks.NewImmediateKey(200), tasks.NewImmediateKey(300))
	r2 := NewRange(tasks.NewImmediateKey(300), tasks.NewImmediateKey(400))
	watermark := tasks.NewImmediateKey(1000)

	state := &persistencespb.QueueState{
		NamespaceIdTable:             [][]byte{known[:]},
		ExclusiveReaderHighWatermark: ToPersistenceTaskKey(watermark),
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			0: {PredicateGroups: []*persistencespb.QueueSlicePredicateGroup{
				{
					Predicate: namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
						NamespaceIdIndexes: []int32{7},
					}),
					Ranges: []*persistencespb.QueueSliceRange{ToPersistenceRange(r0), ToPersistenceRange(r2)},
				},
				{
					Predicate: namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
						NamespaceIdIndexes: []int32{0},
					}),
					Ranges: []*persistencespb.QueueSliceRange{ToPersistenceRange(r1)},
				},
			}},
		},
	}

	got := FromPersistenceQueueState(state)

	universal := predicates.Universal[tasks.Task]()
	require.Equal(t, map[int64][]Scope{
		0: {
			NewScope(r0, universal),
			NewScope(r1, tasks.NewNamespacePredicate([]string{known.String()})),
			NewScope(r2, universal),
		},
	}, got.readerScopes)
	require.Equal(t, 2, got.lostResolutionOnReadCount)
}

func TestFromPersistenceQueueState_ReadsEveryFormat(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	inline := uuid.NewString()
	r0 := NewRange(tasks.NewImmediateKey(100), tasks.NewImmediateKey(200))
	r1 := NewRange(tasks.NewImmediateKey(200), tasks.NewImmediateKey(300))
	r2 := NewRange(tasks.NewImmediateKey(300), tasks.NewImmediateKey(400))
	r4 := NewRange(tasks.NewImmediateKey(500), tasks.NewImmediateKey(600))
	watermark := tasks.NewImmediateKey(1000)

	tests := []struct {
		name  string
		state *persistencespb.QueueState
		want  map[int64][]Scope
	}{
		{
			// Hand-built, so it keeps checking the format every existing shard holds after the
			// writer changes.
			name: "plain format with no table and no groups is read unchanged",
			state: &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: ToPersistenceTaskKey(watermark),
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {Scopes: []*persistencespb.QueueSliceScope{
						{
							Range: ToPersistenceRange(r0),
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
							},
						},
						{
							Range: ToPersistenceRange(r1),
							Predicate: namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
								NamespaceIds: []string{inline},
							}),
						},
					}},
					1: {Scopes: []*persistencespb.QueueSliceScope{
						{
							Range: ToPersistenceRange(r2),
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_OUTBOUND_TASK,
								Attributes: &persistencespb.Predicate_OutboundTaskPredicateAttributes{
									OutboundTaskPredicateAttributes: &persistencespb.OutboundTaskPredicateAttributes{
										Groups: []*persistencespb.OutboundTaskPredicateAttributes_Group{
											{TaskGroup: "g1", NamespaceId: "n1", Destination: "d1"},
										},
									},
								},
							},
						},
					}},
				},
			},
			want: map[int64][]Scope{
				0: {
					NewScope(r0, predicates.Universal[tasks.Task]()),
					NewScope(r1, tasks.NewNamespacePredicate([]string{inline})),
				},
				1: {
					NewScope(r2, tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
						{TaskGroup: "g1", NamespaceID: "n1", Destination: "d1"},
					})),
				},
			},
		},
		{
			name: "table and groups together in one state",
			state: &persistencespb.QueueState{
				NamespaceIdTable:             [][]byte{a[:], b[:], c[:]},
				ExclusiveReaderHighWatermark: ToPersistenceTaskKey(watermark),
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {
						Scopes: []*persistencespb.QueueSliceScope{
							{
								Range: ToPersistenceRange(r2),
								Predicate: namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
									NamespaceIds:       []string{inline},
									NamespaceIdIndexes: []int32{2},
								}),
							},
						},
						PredicateGroups: []*persistencespb.QueueSlicePredicateGroup{
							{
								Predicate: namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
									NamespaceIdIndexes: []int32{0, 1},
								}),
								Ranges: []*persistencespb.QueueSliceRange{ToPersistenceRange(r4), ToPersistenceRange(r0)},
							},
						},
					},
					1: {PredicateGroups: []*persistencespb.QueueSlicePredicateGroup{
						{
							Predicate: namespaceIDPredicateProto(&persistencespb.NamespaceIdPredicateAttributes{
								NamespaceIdIndexes: []int32{1},
							}),
							Ranges: []*persistencespb.QueueSliceRange{ToPersistenceRange(r1)},
						},
					}},
				},
			},
			want: map[int64][]Scope{
				0: {
					NewScope(r0, tasks.NewNamespacePredicate([]string{a.String(), b.String()})),
					NewScope(r2, tasks.NewNamespacePredicate([]string{inline, c.String()})),
					NewScope(r4, tasks.NewNamespacePredicate([]string{a.String(), b.String()})),
				},
				1: {
					NewScope(r1, tasks.NewNamespacePredicate([]string{b.String()})),
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FromPersistenceQueueState(tc.state)

			require.Equal(t, tc.want, got.readerScopes)
			require.Equal(t, 0, got.lostResolutionOnReadCount)
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
		ToPersistenceQueueState(queueState, false),
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

func TestNamespaceIDTable_CountsUnresolved(t *testing.T) {
	valid := uuid.New()
	table := newNamespaceIDTable([][]byte{valid[:], valid[:15]})

	_, ok := table.resolve(0)
	require.True(t, ok)
	require.Equal(t, 0, table.unresolvedIndexes)

	for _, index := range []int32{1, 2, -1} {
		_, ok = table.resolve(index)
		require.False(t, ok)
	}
	require.Equal(t, 3, table.unresolvedIndexes)
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

func TestToPersistenceNamespaceIDPredicate_WritesIDsSorted(t *testing.T) {
	// Eight IDs in unsorted order: map iteration order matches sorted order once in 8! runs.
	ids := []string{
		"00000000-0000-0000-0000-00000000000e",
		"00000000-0000-0000-0000-000000000003",
		"00000000-0000-0000-0000-00000000000a",
		"00000000-0000-0000-0000-000000000001",
		"00000000-0000-0000-0000-00000000000c",
		"00000000-0000-0000-0000-000000000007",
		"00000000-0000-0000-0000-00000000000b",
		"00000000-0000-0000-0000-000000000005",
	}

	got := ToPersistenceNamespaceIDPredicate(tasks.NewNamespacePredicate(ids), nil)

	require.Equal(t,
		[]string{
			"00000000-0000-0000-0000-000000000001",
			"00000000-0000-0000-0000-000000000003",
			"00000000-0000-0000-0000-000000000005",
			"00000000-0000-0000-0000-000000000007",
			"00000000-0000-0000-0000-00000000000a",
			"00000000-0000-0000-0000-00000000000b",
			"00000000-0000-0000-0000-00000000000c",
			"00000000-0000-0000-0000-00000000000e",
		},
		got.GetNamespaceIdPredicateAttributes().NamespaceIds,
	)
}

func TestNamespaceIDTableBuilder_InternsValidUUIDs(t *testing.T) {
	first := uuid.MustParse("00000000-0000-0000-0000-00000000000b")
	second := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	builder := newNamespaceIDTableBuilder()

	// Interned reports whether the ID went into the table; the other outcome is it stays a plain string.
	type result struct {
		Index    int32
		Interned bool
	}
	steps := []struct {
		name string
		id   uuid.UUID
		want result
	}{
		{"a new ID takes the first free slot, 0", first, result{Index: 0, Interned: true}},
		{"another new ID takes the next slot, 1", second, result{Index: 1, Interned: true}},
		{"an ID already in the table returns its existing slot, 0", first, result{Index: 0, Interned: true}},
	}
	for _, step := range steps {
		index, interned := builder.intern(step.id.String())
		require.Equal(t, step.want, result{Index: index, Interned: interned}, step.name)
	}

	require.Equal(t, [][]byte{first[:], second[:]}, builder.entries(),
		"entries are the 16-byte IDs in slot order, with no duplicate for the repeated ID")
}

func TestNamespaceIDTableBuilder_RefusesIDsTheReaderWouldRewrite(t *testing.T) {
	// The reader rebuilds an entry as lowercase hyphenated text, so an ID is only safe to intern
	// if that text equals the original. Anything else must stay a plain string.
	tests := []struct {
		name string
		id   string
	}{
		{"empty string is not a UUID", ""},
		{"free text is not a UUID", "not-a-uuid"},
		{"uppercase would come back lowercase", "00000000-0000-0000-0000-00000000000A"},
		{"no hyphens would come back hyphenated", "0000000000000000000000000000000b"},
		{"braces would come back without braces", "{00000000-0000-0000-0000-00000000000b}"},
		{"urn prefix would come back without the prefix", "urn:uuid:00000000-0000-0000-0000-00000000000b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := newNamespaceIDTableBuilder()

			_, interned := builder.intern(tc.id)

			require.False(t, interned, "intern(%q) must refuse it", tc.id)
			require.Empty(t, builder.entries(), "a refused ID must not add a table entry")
		})
	}
}

func TestNamespaceIDTableBuilder_NilBuilderInternsNothing(t *testing.T) {
	// A nil builder is how the writer says "write the old form", so it must refuse even a valid UUID.
	var builder *namespaceIDTableBuilder

	_, interned := builder.intern("00000000-0000-0000-0000-00000000000b")

	require.False(t, interned, "a nil builder must refuse every ID")
	require.Empty(t, builder.entries(), "a nil builder has no table entries")
}

func TestToPersistenceNamespaceIDPredicate_WritesIndexesWhenGivenABuilder(t *testing.T) {
	a := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	b := uuid.MustParse("00000000-0000-0000-0000-00000000000b")
	c := uuid.MustParse("00000000-0000-0000-0000-00000000000c")
	// Supplied out of order; IDs are written sorted, so slots are assigned a, b, c.
	predicate := tasks.NewNamespacePredicate([]string{c.String(), a.String(), b.String()})

	type written struct {
		NamespaceIDs []string
		Indexes      []int32
	}
	tests := []struct {
		name        string
		builder     *namespaceIDTableBuilder
		want        written
		wantEntries [][]byte
	}{
		{
			name:        "with a builder the IDs become table indexes and none stay as strings",
			builder:     newNamespaceIDTableBuilder(),
			want:        written{Indexes: []int32{0, 1, 2}},
			wantEntries: [][]byte{a[:], b[:], c[:]},
		},
		{
			name:    "with no builder the IDs stay as strings, the old form",
			builder: nil,
			want:    written{NamespaceIDs: []string{a.String(), b.String(), c.String()}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attributes := ToPersistenceNamespaceIDPredicate(predicate, tc.builder).GetNamespaceIdPredicateAttributes()

			require.Equal(t, tc.want, written{
				NamespaceIDs: attributes.NamespaceIds,
				Indexes:      attributes.NamespaceIdIndexes,
			})
			require.Equal(t, tc.wantEntries, tc.builder.entries(), "table entries in slot order")
		})
	}
}

func TestToPersistenceNamespaceIDPredicate_KeepsNonUUIDIDsAsStrings(t *testing.T) {
	a := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	b := uuid.MustParse("00000000-0000-0000-0000-00000000000b")
	// Sorts before both UUIDs, so if a refused ID took a slot, a and b would not get 0 and 1.
	notUUID := "0000"
	builder := newNamespaceIDTableBuilder()

	attributes := ToPersistenceNamespaceIDPredicate(
		tasks.NewNamespacePredicate([]string{b.String(), notUUID, a.String()}),
		builder,
	).GetNamespaceIdPredicateAttributes()

	require.Equal(t, []string{notUUID}, attributes.NamespaceIds, "the non-UUID ID stays a string")
	require.Equal(t, []int32{0, 1}, attributes.NamespaceIdIndexes, "the UUIDs take slots 0 and 1; the refused ID takes none")
	require.Equal(t, [][]byte{a[:], b[:]}, builder.entries(), "the table holds only the UUIDs")
}

// namespaceLeaves returns the namespace attributes of every leaf in the tree, in traversal order.
func namespaceLeaves(predicate *persistencespb.Predicate) []*persistencespb.NamespaceIdPredicateAttributes {
	switch predicate.PredicateType {
	case enumsspb.PREDICATE_TYPE_NAMESPACE_ID:
		return []*persistencespb.NamespaceIdPredicateAttributes{predicate.GetNamespaceIdPredicateAttributes()}
	case enumsspb.PREDICATE_TYPE_NOT:
		return namespaceLeaves(predicate.GetNotPredicateAttributes().Predicate)
	case enumsspb.PREDICATE_TYPE_AND:
		return namespaceLeavesOf(predicate.GetAndPredicateAttributes().Predicates)
	case enumsspb.PREDICATE_TYPE_OR:
		return namespaceLeavesOf(predicate.GetOrPredicateAttributes().Predicates)
	default:
		return nil
	}
}

func namespaceLeavesOf(children []*persistencespb.Predicate) []*persistencespb.NamespaceIdPredicateAttributes {
	var leaves []*persistencespb.NamespaceIdPredicateAttributes
	for _, child := range children {
		leaves = append(leaves, namespaceLeaves(child)...)
	}
	return leaves
}

func TestToPersistencePredicate_WritesIndexesInCompositePredicates(t *testing.T) {
	a := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	b := uuid.MustParse("00000000-0000-0000-0000-00000000000b")
	c := uuid.MustParse("00000000-0000-0000-0000-00000000000c")
	namespaces := func(ids ...uuid.UUID) tasks.Predicate {
		strs := make([]string, 0, len(ids))
		for _, id := range ids {
			strs = append(strs, id.String())
		}
		return tasks.NewNamespacePredicate(strs)
	}

	type leaf struct {
		NamespaceIDs []string
		Indexes      []int32
	}
	tests := []struct {
		name        string
		predicate   tasks.Predicate
		wantLeaves  []leaf
		wantEntries [][]byte
	}{
		{
			name:        "Not around a namespace leaf",
			predicate:   predicates.Not(namespaces(a, b)),
			wantLeaves:  []leaf{{Indexes: []int32{0, 1}}},
			wantEntries: [][]byte{a[:], b[:]},
		},
		{
			name:      "And of two leaves that share b: b has one slot, used by both",
			predicate: predicates.And(namespaces(a, b), namespaces(b, c)),
			wantLeaves: []leaf{
				{Indexes: []int32{0, 1}},
				{Indexes: []int32{1, 2}},
			},
			wantEntries: [][]byte{a[:], b[:], c[:]},
		},
		{
			name:      "Or with a nested Not: slots follow first appearance, c before a",
			predicate: predicates.Or(predicates.Not(namespaces(c)), namespaces(a)),
			wantLeaves: []leaf{
				{Indexes: []int32{0}},
				{Indexes: []int32{1}},
			},
			wantEntries: [][]byte{c[:], a[:]},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := newNamespaceIDTableBuilder()

			persisted := ToPersistencePredicate(tc.predicate, builder)

			var got []leaf
			for _, attributes := range namespaceLeaves(persisted) {
				got = append(got, leaf{NamespaceIDs: attributes.NamespaceIds, Indexes: attributes.NamespaceIdIndexes})
			}
			require.Equal(t, tc.wantLeaves, got, "every leaf holds indexes and no strings")
			require.Equal(t, tc.wantEntries, builder.entries(), "each ID appears once in the table")
		})
	}
}

func TestToPersistencePredicate_LeavesNonNamespacePredicatesUnchangedByABuilder(t *testing.T) {
	// One entry per set: these predicates keep their entries in a map, so two or more would
	// serialize in an order that varies between calls and make the comparison flaky.
	tests := []struct {
		name      string
		predicate tasks.Predicate
	}{
		{"task type", tasks.NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT})},
		{"destination", tasks.NewDestinationPredicate([]string{"d1"})},
		{"outbound task group", tasks.NewOutboundTaskGroupPredicate([]string{"g1"})},
		{"outbound task", tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
			{TaskGroup: "g1", NamespaceID: "n1", Destination: "d1"},
		})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := newNamespaceIDTableBuilder()

			withBuilder := ToPersistencePredicate(tc.predicate, builder)
			withoutBuilder := ToPersistencePredicate(tc.predicate, nil)

			require.True(t, proto.Equal(withoutBuilder, withBuilder),
				"a builder must not change a %s predicate: without=%v with=%v", tc.name, withoutBuilder, withBuilder)
			require.Empty(t, builder.entries(), "a %s predicate has no namespace IDs to put in the table", tc.name)
		})
	}
}

func TestToPersistenceQueueState_CompactWritesNamespaceIDTable(t *testing.T) {
	a := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	b := uuid.MustParse("00000000-0000-0000-0000-00000000000b")
	c := uuid.MustParse("00000000-0000-0000-0000-00000000000c")
	// Reader 1's IDs are a subset of reader 2's, so the table is a, b, c whichever reader is
	// visited first; reader order is 2.4c's concern, not this test's.
	state := &queueState{
		readerScopes: map[int64][]Scope{
			1: {NewScope(NewRandomRange(), tasks.NewNamespacePredicate([]string{a.String(), b.String()}))},
			2: {NewScope(NewRandomRange(), tasks.NewNamespacePredicate([]string{a.String(), b.String(), c.String()}))},
		},
		exclusiveReaderHighWatermark: tasks.NewImmediateKey(100),
	}

	persisted := ToPersistenceQueueState(state, true)

	require.Equal(t, [][]byte{a[:], b[:], c[:]}, persisted.NamespaceIdTable, "each namespace ID once, in slot order")

	type leaf struct {
		NamespaceIDs []string
		Indexes      []int32
	}
	leafOf := func(readerID int64) []leaf {
		var leaves []leaf
		for _, scope := range persisted.ReaderStates[readerID].Scopes {
			for _, attributes := range namespaceLeaves(scope.Predicate) {
				leaves = append(leaves, leaf{NamespaceIDs: attributes.NamespaceIds, Indexes: attributes.NamespaceIdIndexes})
			}
		}
		return leaves
	}
	require.Equal(t, []leaf{{Indexes: []int32{0, 1}}}, leafOf(1), "reader 1 holds indexes into the table, no strings")
	require.Equal(t, []leaf{{Indexes: []int32{0, 1, 2}}}, leafOf(2), "reader 2 holds indexes into the table, no strings")
}

func TestQueueStateRoundTrip_CompactForm(t *testing.T) {
	a := uuid.MustParse("00000000-0000-0000-0000-00000000000a").String()
	b := uuid.MustParse("00000000-0000-0000-0000-00000000000b").String()
	c := uuid.MustParse("00000000-0000-0000-0000-00000000000c").String()
	namespaces := func(ids ...string) tasks.Predicate { return tasks.NewNamespacePredicate(ids) }
	scopeWith := func(predicate tasks.Predicate) Scope { return NewScope(NewRandomRange(), predicate) }

	tests := []struct {
		name string
		// readers maps reader ID to the predicate of each of its scopes.
		readers          map[int64][]tasks.Predicate
		wantTableEntries int
	}{
		{
			name:    "universal only",
			readers: map[int64][]tasks.Predicate{1: {predicates.Universal[tasks.Task]()}},
		},
		{
			name:             "one namespace set",
			readers:          map[int64][]tasks.Predicate{1: {namespaces(a, b)}},
			wantTableEntries: 2,
		},
		{
			name:             "Not of a namespace set",
			readers:          map[int64][]tasks.Predicate{1: {predicates.Not(namespaces(a, b))}},
			wantTableEntries: 2,
		},
		{
			name: "And and Or mixing namespace leaves",
			readers: map[int64][]tasks.Predicate{1: {
				predicates.And(predicates.Or(namespaces(a, b), predicates.Not(namespaces(c))), namespaces(b)),
			}},
			wantTableEntries: 3,
		},
		{
			name:             "a non-UUID ID stays a string beside UUIDs",
			readers:          map[int64][]tasks.Predicate{1: {namespaces(a, "legacy-id")}},
			wantTableEntries: 1,
		},
		{
			name: "two readers sharing IDs",
			readers: map[int64][]tasks.Predicate{
				1: {namespaces(a, b)},
				2: {namespaces(b, c), predicates.Universal[tasks.Task]()},
			},
			wantTableEntries: 3,
		},
		{
			name: "outbound predicate",
			readers: map[int64][]tasks.Predicate{1: {
				tasks.NewOutboundTaskPredicate([]tasks.TaskGroupNamespaceIDAndDestination{
					{TaskGroup: "g1", NamespaceID: "n1", Destination: "d1"},
				}),
			}},
		},
		{
			name:    "a reader with no scopes",
			readers: map[int64][]tasks.Predicate{1: {}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := &queueState{
				readerScopes:                 map[int64][]Scope{},
				exclusiveReaderHighWatermark: tasks.NewKey(time.Unix(0, 1_000_000).UTC(), 0),
			}
			for readerID, readerPredicates := range tc.readers {
				scopes := []Scope{}
				for _, predicate := range readerPredicates {
					scopes = append(scopes, scopeWith(predicate))
				}
				state.readerScopes[readerID] = scopes
			}

			persisted := ToPersistenceQueueState(state, true)

			require.Len(t, persisted.NamespaceIdTable, tc.wantTableEntries, "the compact form was written")
			require.Equal(t, state, FromPersistenceQueueState(persisted), "reading it back gives the same state")
		})
	}
}

func TestToPersistenceQueueState_NotCompactWritesTheOldForm(t *testing.T) {
	a := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	b := uuid.MustParse("00000000-0000-0000-0000-00000000000b")
	c := uuid.MustParse("00000000-0000-0000-0000-00000000000c")
	// IDs are supplied out of order so the written strings show they are sorted.
	state := &queueState{
		readerScopes: map[int64][]Scope{
			1: {NewScope(NewRandomRange(), tasks.NewNamespacePredicate([]string{b.String(), a.String()}))},
			2: {NewScope(NewRandomRange(), tasks.NewNamespacePredicate([]string{c.String(), b.String(), a.String()}))},
		},
		exclusiveReaderHighWatermark: tasks.NewImmediateKey(100),
	}

	persisted := ToPersistenceQueueState(state, false)

	require.Empty(t, persisted.NamespaceIdTable, "no table")
	type leaf struct {
		NamespaceIDs []string
		Indexes      []int32
	}
	leafOf := func(readerID int64) []leaf {
		var leaves []leaf
		for _, scope := range persisted.ReaderStates[readerID].Scopes {
			for _, attributes := range namespaceLeaves(scope.Predicate) {
				leaves = append(leaves, leaf{NamespaceIDs: attributes.NamespaceIds, Indexes: attributes.NamespaceIdIndexes})
			}
		}
		return leaves
	}
	require.Equal(t, []leaf{{NamespaceIDs: []string{a.String(), b.String()}}}, leafOf(1), "reader 1: sorted strings, no indexes")
	require.Equal(t, []leaf{{NamespaceIDs: []string{a.String(), b.String(), c.String()}}}, leafOf(2), "reader 2: sorted strings, no indexes")
	for readerID, readerState := range persisted.ReaderStates {
		require.Empty(t, readerState.PredicateGroups, "reader %d has no predicate groups", readerID)
	}
}

func TestToPersistenceQueueState_CompactFormIsSmaller(t *testing.T) {
	const namespaceCount = 20
	var namespaces [namespaceCount]string
	for i := range namespaces {
		var id uuid.UUID
		id[15] = byte(i + 1)
		namespaces[i] = id.String()
	}
	// 100 scopes, each over 10 of the 20 namespaces, the shape that makes plain text repeat IDs.
	scopes := make([]Scope, 0, 100)
	for i := range 100 {
		ids := make([]string, 0, 10)
		for j := range 10 {
			ids = append(ids, namespaces[(i+j)%namespaceCount])
		}
		scopes = append(scopes, NewScope(NewRandomRange(), tasks.NewNamespacePredicate(ids)))
	}
	state := &queueState{
		readerScopes:                 map[int64][]Scope{1: scopes},
		exclusiveReaderHighWatermark: tasks.NewImmediateKey(100),
	}

	plain, err := proto.Marshal(ToPersistenceQueueState(state, false))
	require.NoError(t, err)
	compact, err := proto.Marshal(ToPersistenceQueueState(state, true))
	require.NoError(t, err)

	t.Logf("plain %d bytes, compact %d bytes, %.1f%% smaller",
		len(plain), len(compact), 100*float64(len(plain)-len(compact))/float64(len(plain)))
	require.Less(t, len(compact), len(plain), "compact is %d bytes, plain is %d bytes", len(compact), len(plain))
}

func TestToPersistenceQueueState_CompactTableFollowsAscendingReaderID(t *testing.T) {
	// Reader ID to namespace ID. Ascending reader ID gives d, 3, a, 1, c, 5, 8, e, which is neither
	// sorted nor reversed, so only a deterministic reader order produces this table. Eight readers
	// make a map-order pass by luck a 1 in 40,320 event.
	namespaceByReader := []struct {
		readerID  int64
		namespace uuid.UUID
	}{
		{1, uuid.MustParse("00000000-0000-0000-0000-00000000000d")},
		{2, uuid.MustParse("00000000-0000-0000-0000-000000000003")},
		{3, uuid.MustParse("00000000-0000-0000-0000-00000000000a")},
		{4, uuid.MustParse("00000000-0000-0000-0000-000000000001")},
		{5, uuid.MustParse("00000000-0000-0000-0000-00000000000c")},
		{6, uuid.MustParse("00000000-0000-0000-0000-000000000005")},
		{7, uuid.MustParse("00000000-0000-0000-0000-000000000008")},
		{8, uuid.MustParse("00000000-0000-0000-0000-00000000000e")},
	}
	state := &queueState{
		readerScopes:                 map[int64][]Scope{},
		exclusiveReaderHighWatermark: tasks.NewImmediateKey(100),
	}
	var wantTable [][]byte
	for _, entry := range namespaceByReader {
		state.readerScopes[entry.readerID] = []Scope{
			NewScope(NewRandomRange(), tasks.NewNamespacePredicate([]string{entry.namespace.String()})),
		}
		wantTable = append(wantTable, entry.namespace[:])
	}

	persisted := ToPersistenceQueueState(state, true)

	require.Equal(t, wantTable, persisted.NamespaceIdTable, "table entries follow ascending reader ID")
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
