package dynamicconfig

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These two tests are in a separate file in the 'dynamicconfig' package to access the private
// findMatch function. Most other tests should be in 'dynamicconfig_test' to test things as a
// client.
func TestFindMatch(t *testing.T) {
	testCases := []struct {
		v       []ConstrainedValue
		filters []Constraints
		matched bool
	}{
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			matched: false,
		},
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			matched: false,
		},
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"}},
			},
			filters: []Constraints{
				{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"},
			},
			matched: true,
		},
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{TaskQueueName: "sample-task-queue"},
			},
			matched: false,
		},
	}

	for _, tc := range testCases {
		var cache sync.Map
		_, err := findMatch(&cache, tc.v, tc.filters)
		assert.Equal(t, tc.matched, err == nil)
	}
}

func TestFindMatchIndexed(t *testing.T) {
	var cvs []ConstrainedValue
	for i := range 100 {
		cvs = append(cvs, ConstrainedValue{
			Constraints: Constraints{
				Namespace: fmt.Sprintf("namespace%d", i),
			},
			Value: 1000 + i,
		})
	}

	have := []Constraints{{Namespace: "namespace75"}}
	notHave := []Constraints{{Namespace: "othernamespace"}}

	var cache sync.Map
	v, err := findMatch(&cache, cvs, have)
	require.NoError(t, err)
	require.NotNil(t, v)
	assert.EqualValues(t, 1075, v.Value)
	assert.Equal(t, &cvs[75], v)

	_, err = findMatch(&cache, cvs, notHave)
	assert.Error(t, err)
}

func TestFindMatchWithTyped(t *testing.T) {
	testCases := []struct {
		val      []ConstrainedValue
		tv       []TypedConstrainedValue[struct{}]
		filters  []Constraints
		valOrder int
		defOrder int
	}{
		{
			val: nil,
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			valOrder: 0,
			defOrder: 0,
		},
		{
			val: nil,
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			valOrder: 0,
			defOrder: 0,
		},
		{
			val: nil,
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"}},
			},
			filters: []Constraints{
				{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"},
			},
			valOrder: 0,
			defOrder: 1,
		},
		{
			val: nil,
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{TaskQueueName: "sample-task-queue"},
			},
			valOrder: 0,
			defOrder: 0,
		},
		{
			val: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "ns"}},
			},
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "ns", TaskQueueName: "othertq"}},
				{},
			},
			filters: []Constraints{
				{Namespace: "ns", TaskQueueName: "tq"},
				{Namespace: "ns"},
				{},
			},
			valOrder: 4,
			defOrder: 9,
		},
		{
			val: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "ns"}},
			},
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "ns", TaskQueueName: "tq"}},
				{},
			},
			filters: []Constraints{
				{Namespace: "ns", TaskQueueName: "tq"},
				{Namespace: "ns"},
				{},
			},
			valOrder: 4,
			defOrder: 2,
		},
	}

	for _, tc := range testCases {
		_, _, valOrder, defOrder := findMatchWithConstrainedDefaults(tc.val, tc.tv, tc.filters)
		assert.Equal(t, tc.valOrder, valOrder)
		assert.Equal(t, tc.defOrder, defOrder)
	}
}
