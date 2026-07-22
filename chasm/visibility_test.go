package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/protorequire"
)

type (
	visibilitySuite struct {
		suite.Suite
		*require.Assertions

		mockContext        *MockContext
		mockMutableContext *MockMutableContext

		registry *Registry

		visibility *Visibility
	}
)

func TestVisibilitySuite(t *testing.T) {
	suite.Run(t, new(visibilitySuite))
}

func (s *visibilitySuite) SetupTest() {
	s.initAssertions()
	s.mockContext = &MockContext{}
	s.mockMutableContext = &MockMutableContext{}

	s.registry = NewRegistry(log.NewTestLogger())
	err := s.registry.Register(&CoreLibrary{})
	s.NoError(err)

	s.visibility = NewVisibility(s.mockMutableContext)
	s.Len(s.mockMutableContext.Tasks, 1)
	s.mockMutableContext.Tasks = nil // Clear tasks added during creation
}

func (s *visibilitySuite) SetupSubTest() {
	s.initAssertions()
}

func (s *visibilitySuite) initAssertions() {
	s.Assertions = require.New(s.T())
}

func (s *visibilitySuite) TestComponentFqType() {
	rc, ok := s.registry.ComponentFor(&Visibility{})
	s.True(ok)
	s.Equal(visibilityComponentType, rc.FqType())
}

func (s *visibilitySuite) TestTaskFqType() {
	rc, ok := s.registry.TaskFor(&persistencespb.ChasmVisibilityTaskData{})
	s.True(ok)
	s.Equal(visibilityTaskType, rc.FqType())
}

func (s *visibilitySuite) TestLifeCycleState() {
	s.Equal(LifecycleStateRunning, s.visibility.LifecycleState(s.mockMutableContext))
}

func (s *visibilitySuite) TestMergeCustomSearchAttributes() {
	sa := s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.Empty(sa)

	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14

	// Add SA via Visibility struct method.
	err := s.visibility.MergeCustomSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			stringKey: s.mustEncode(stringVal),
			intKey:    s.mustEncode(intVal),
			floatKey:  s.mustEncode(floatVal),
		},
	)
	s.NoError(err)
	s.Len(s.mockMutableContext.Tasks, 1)
	s.assertTaskPayload(2, s.mockMutableContext.Tasks[0].Payload)

	sa = s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.Len(sa, 3)

	var actualStringVal string
	err = payload.Decode(sa[stringKey], &actualStringVal)
	s.NoError(err)
	s.Equal(stringVal, actualStringVal)

	var actualIntVal int
	err = payload.Decode(sa[intKey], &actualIntVal)
	s.NoError(err)
	s.Equal(intVal, actualIntVal)

	var actualFloatVal float64
	err = payload.Decode(sa[floatKey], &actualFloatVal)
	s.NoError(err)
	s.Equal(floatVal, actualFloatVal)

	// Test remove search attributes by setting payload to nil.
	err = s.visibility.MergeCustomSearchAttributes(s.mockMutableContext, map[string]*commonpb.Payload{
		intKey:   s.mustEncode(intVal),
		floatKey: nil,
	})
	s.NoError(err)
	s.Len(s.mockMutableContext.Tasks, 2)
	s.assertTaskPayload(3, s.mockMutableContext.Tasks[1].Payload)

	sa = s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.NoError(err)
	s.Len(sa, 2, "intKey and stringKey should remain")

	// Test removing all search attributes also removes the node.
	err = s.visibility.MergeCustomSearchAttributes(s.mockMutableContext, map[string]*commonpb.Payload{
		stringKey: nil,
		intKey:    nil,
	})
	s.NoError(err)
	s.Len(s.mockMutableContext.Tasks, 3)
	s.assertTaskPayload(4, s.mockMutableContext.Tasks[2].Payload)
	_, ok := s.visibility.SA.TryGet(s.mockContext)
	s.False(ok)
	s.Nil(s.visibility.CustomSearchAttributes(s.mockContext))
}

func (s *visibilitySuite) TestNewVisibilityWithData_FilterNilSearchAttributes() {
	stringKey, stringVal := "stringKey", "stringValue"
	// SA with 1 valid and 2 nil values - nil values should be filtered out
	customSearchAttributes := map[string]*commonpb.Payload{
		stringKey: s.mustEncode(stringVal),
		"nilKey1": nil,
		"nilKey2": nil,
	}
	// Memo with 1 valid and 2 nil values - nil values should be filtered out
	customMemo := map[string]*commonpb.Payload{
		stringKey: s.mustEncode(stringVal),
		"nilKey1": nil,
		"nilKey2": nil,
	}
	visibility, err := NewVisibilityWithData(s.mockMutableContext, customSearchAttributes, customMemo)
	s.NoError(err)
	// SA should have only 1 field (nil values filtered out)
	s.Len(visibility.SA.Get(s.mockContext).IndexedFields, 1)
	s.NotNil(visibility.SA.Get(s.mockContext).IndexedFields[stringKey])
	// Memo should have only 1 field (nil values filtered out)
	s.Len(visibility.Memo.Get(s.mockContext).Fields, 1)
	s.NotNil(visibility.Memo.Get(s.mockContext).Fields[stringKey])
}

func (s *visibilitySuite) TestReplaceCustomSearchAttributes() {
	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14
	byteKey, byteVal := "byteKey", []byte{0x01, 0x02, 0x03}

	// Set up some initial SA.
	err := s.visibility.ReplaceCustomSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			stringKey: s.mustEncode(stringVal),
			intKey:    s.mustEncode(intVal),
			floatKey:  s.mustEncode(floatVal),
		},
	)
	s.NoError(err)
	s.Len(s.mockMutableContext.Tasks, 1)
	s.assertTaskPayload(2, s.mockMutableContext.Tasks[0].Payload)

	sa := s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.Len(sa, 3)

	// Set to a new set of SA, non-existing keys should be removed.
	err = s.visibility.ReplaceCustomSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			floatKey: s.mustEncode(floatVal),
			byteKey:  s.mustEncode(byteVal),
		},
	)
	s.NoError(err)
	s.Len(s.mockMutableContext.Tasks, 2)
	s.assertTaskPayload(3, s.mockMutableContext.Tasks[1].Payload)

	sa = s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.Len(sa, 2)

	// Setting to an empty map should remove the node.
	err = s.visibility.ReplaceCustomSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{},
	)
	s.NoError(err)
	s.Len(s.mockMutableContext.Tasks, 3)
	s.assertTaskPayload(4, s.mockMutableContext.Tasks[2].Payload)
	_, ok := s.visibility.SA.TryGet(s.mockContext)
	s.False(ok)
	s.Nil(s.visibility.CustomSearchAttributes(s.mockContext))

	// Test that nil values are filtered out during replace.
	err = s.visibility.ReplaceCustomSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			stringKey: s.mustEncode(stringVal),
			intKey:    nil, // Should be filtered out
		},
	)
	s.NoError(err)
	s.Len(s.mockMutableContext.Tasks, 4)
	s.assertTaskPayload(5, s.mockMutableContext.Tasks[3].Payload)

	sa = s.visibility.CustomSearchAttributes(s.mockMutableContext)
	s.Len(sa, 1, "nil values should be filtered out")
	s.NotNil(sa[stringKey])
	s.Nil(sa[intKey])

	// Test that replacing with all nil values removes the node.
	err = s.visibility.ReplaceCustomSearchAttributes(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			stringKey: nil,
			intKey:    nil,
		},
	)
	s.NoError(err)
	s.Len(s.mockMutableContext.Tasks, 5)
	s.assertTaskPayload(6, s.mockMutableContext.Tasks[4].Payload)
	_, ok = s.visibility.SA.TryGet(s.mockContext)
	s.False(ok)
	s.Nil(s.visibility.CustomSearchAttributes(s.mockContext))
}

func (s *visibilitySuite) TestMergeCustomSearchAttributes_RejectsReservedPrefix() {
	reservedKey := sadefs.TemporalScheduledStartTime // "TemporalScheduledStartTime"

	// A single reserved-prefix key is rejected.
	err := s.visibility.MergeCustomSearchAttributes(s.mockMutableContext, map[string]*commonpb.Payload{
		reservedKey: s.mustEncode("2020-01-01T00:00:00Z"),
	})
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)

	// Rejection is atomic: no SA is written and no visibility task is generated even when a
	// valid key is present alongside the reserved one.
	err = s.visibility.MergeCustomSearchAttributes(s.mockMutableContext, map[string]*commonpb.Payload{
		"validKey":  s.mustEncode("value"),
		reservedKey: s.mustEncode("2020-01-01T00:00:00Z"),
	})
	s.ErrorAs(err, &invalidArg)
	s.Empty(s.mockMutableContext.Tasks)
	s.Nil(s.visibility.CustomSearchAttributes(s.mockContext))
}

func (s *visibilitySuite) TestReplaceCustomSearchAttributes_RejectsReservedPrefix() {
	// Seed with a valid SA so we can verify rejection leaves existing state untouched.
	err := s.visibility.ReplaceCustomSearchAttributes(s.mockMutableContext, map[string]*commonpb.Payload{
		"validKey": s.mustEncode("value"),
	})
	s.NoError(err)
	s.Len(s.mockMutableContext.Tasks, 1)

	// A reserved-prefix key (mixed with a valid one) is rejected atomically: no new task and
	// the previously stored SA is unchanged.
	err = s.visibility.ReplaceCustomSearchAttributes(s.mockMutableContext, map[string]*commonpb.Payload{
		"anotherKey":                  s.mustEncode("value"),
		sadefs.TemporalSchedulePaused: s.mustEncode(true),
	})
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
	s.Len(s.mockMutableContext.Tasks, 1, "no new visibility task should be generated")
	sa := s.visibility.CustomSearchAttributes(s.mockContext)
	s.Len(sa, 1)
	s.NotNil(sa["validKey"])
}

func (s *visibilitySuite) TestNewVisibilityWithData_RejectsReservedPrefix() {
	visibility, err := NewVisibilityWithData(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			sadefs.TemporalScheduledById: s.mustEncode("schedule-id"),
		},
		nil,
	)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
	s.Nil(visibility)
}

func (s *visibilitySuite) TestMergeCustomSearchAttributes_AllowsNonReservedKey() {
	// A key that merely contains "Temporal" but does not start with the reserved prefix is
	// a valid custom search attribute.
	err := s.visibility.MergeCustomSearchAttributes(s.mockMutableContext, map[string]*commonpb.Payload{
		"MyTemporalField": s.mustEncode("value"),
	})
	s.NoError(err)
	sa := s.visibility.CustomSearchAttributes(s.mockContext)
	s.Len(sa, 1)
	s.NotNil(sa["MyTemporalField"])
}

func (s *visibilitySuite) TestMergeCustomMemo() {
	memo := s.visibility.CustomMemo(s.mockMutableContext)
	s.Empty(memo)

	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14

	// Add memo via Visibility struct method.
	s.visibility.MergeCustomMemo(s.mockMutableContext, map[string]*commonpb.Payload{
		stringKey: s.mustEncode(stringVal),
		intKey:    s.mustEncode(intVal),
		floatKey:  s.mustEncode(floatVal),
	})
	s.Len(s.mockMutableContext.Tasks, 1)
	s.assertTaskPayload(2, s.mockMutableContext.Tasks[0].Payload)

	memo = s.visibility.CustomMemo(s.mockMutableContext)
	s.Len(memo, 3)

	var actualStringVal string
	err := payload.Decode(memo[stringKey], &actualStringVal)
	s.NoError(err)
	s.Equal(stringVal, actualStringVal)

	var actualIntVal int
	err = payload.Decode(memo[intKey], &actualIntVal)
	s.NoError(err)
	s.Equal(intVal, actualIntVal)

	var actualFloatVal float64
	err = payload.Decode(memo[floatKey], &actualFloatVal)
	s.NoError(err)
	s.Equal(floatVal, actualFloatVal)

	// Test remove memo by setting payload to nil.
	s.visibility.MergeCustomMemo(s.mockMutableContext, map[string]*commonpb.Payload{
		intKey:   s.mustEncode(intVal),
		floatKey: nil,
	})
	s.Len(s.mockMutableContext.Tasks, 2)
	s.assertTaskPayload(3, s.mockMutableContext.Tasks[1].Payload)

	memo = s.visibility.CustomMemo(s.mockMutableContext)
	s.Len(memo, 2, "intKey and stringKey should remain")

	// Test removing all memo fields also removes the node.
	s.visibility.MergeCustomMemo(s.mockMutableContext, map[string]*commonpb.Payload{
		stringKey: nil,
		intKey:    nil,
	})
	s.Len(s.mockMutableContext.Tasks, 3)
	s.assertTaskPayload(4, s.mockMutableContext.Tasks[2].Payload)
	_, ok := s.visibility.Memo.TryGet(s.mockContext)
	s.False(ok)
	s.Nil(s.visibility.CustomMemo(s.mockContext))
}

func (s *visibilitySuite) TestReplaceCustomMemo() {
	stringKey, stringVal := "stringKey", "stringValue"
	intKey, intVal := "intKey", 42
	floatKey, floatVal := "floatKey", 3.14
	byteKey, byteVal := "byteKey", []byte{0x01, 0x02, 0x03}

	// Set up some initial memo fields.
	s.visibility.ReplaceCustomMemo(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			stringKey: s.mustEncode(stringVal),
			intKey:    s.mustEncode(intVal),
			floatKey:  s.mustEncode(floatVal),
		},
	)
	s.Len(s.mockMutableContext.Tasks, 1)
	s.assertTaskPayload(2, s.mockMutableContext.Tasks[0].Payload)

	memo := s.visibility.CustomMemo(s.mockMutableContext)
	s.Len(memo, 3)

	// Set to a new set of memo fields, non-existing keys should be removed.
	s.visibility.ReplaceCustomMemo(
		s.mockMutableContext,
		map[string]*commonpb.Payload{
			floatKey:  s.mustEncode(floatVal),
			byteKey:   s.mustEncode(byteVal),
			stringKey: nil, // nil value must be filtered out
		},
	)
	s.Len(s.mockMutableContext.Tasks, 2)
	s.assertTaskPayload(3, s.mockMutableContext.Tasks[1].Payload)

	memo = s.visibility.CustomMemo(s.mockMutableContext)
	s.Len(memo, 2)

	// Setting to an empty map should remove the node.
	s.visibility.ReplaceCustomMemo(
		s.mockMutableContext,
		map[string]*commonpb.Payload{},
	)
	s.Len(s.mockMutableContext.Tasks, 3)
	s.assertTaskPayload(4, s.mockMutableContext.Tasks[2].Payload)
	_, ok := s.visibility.Memo.TryGet(s.mockContext)
	s.False(ok)
	s.Nil(s.visibility.CustomMemo(s.mockContext))
}

func (s *visibilitySuite) assertTaskPayload(expectedCount int64, taskPayload any) {
	protorequire.ProtoEqual(
		s.T(),
		&persistencespb.ChasmVisibilityTaskData{TransitionCount: expectedCount},
		taskPayload.(*persistencespb.ChasmVisibilityTaskData),
	)
}

func (s *visibilitySuite) mustEncode(v any) *commonpb.Payload {
	p, err := payload.Encode(v)
	s.NoError(err)
	return p
}
