package dynamicconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestMemoryClient(t *testing.T) {
	c := dynamicconfig.NewMemoryClient()
	k := dynamicconfig.MakeKey("key")

	// plain override
	assert.Nil(t, c.GetValue(k))
	remove := c.OverrideValue(k, 123)
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 123}}, c.GetValue(k))
	remove()
	assert.Nil(t, c.GetValue(k))

	// two levels, pop in correct order
	remove1 := c.OverrideValue(k, 123)
	remove2 := c.OverrideValue(k, 456)
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 456}}, c.GetValue(k))
	remove2()
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 123}}, c.GetValue(k))
	remove1()
	assert.Nil(t, c.GetValue(k))

	// three levels, pop in wrong order
	remove1 = c.OverrideValue(k, 123)
	remove2 = c.OverrideValue(k, 456)
	remove3 := c.OverrideValue(k, 789)
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 789}}, c.GetValue(k))
	remove2()
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 789}}, c.GetValue(k))
	remove3()
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 123}}, c.GetValue(k))
	remove1()
	remove3() // no-op
	remove2() // no-op
	assert.Nil(t, c.GetValue(k))
}

func TestMemoryClientPartialOverride(t *testing.T) {
	c := dynamicconfig.NewMemoryClient()
	k := dynamicconfig.MakeKey("key")

	nsA := dynamicconfig.Constraints{Namespace: "ns-a"}
	nsB := dynamicconfig.Constraints{Namespace: "ns-b"}

	// Two partial overrides with different namespace constraints coexist.
	removeA := c.PartialOverrideValue(k, []dynamicconfig.ConstrainedValue{{Constraints: nsA, Value: 1}})
	removeB := c.PartialOverrideValue(k, []dynamicconfig.ConstrainedValue{{Constraints: nsB, Value: 13}})

	// Both are visible, most recent first.
	assert.Equal(t, []dynamicconfig.ConstrainedValue{
		{Constraints: nsB, Value: 13},
		{Constraints: nsA, Value: 1},
	}, c.GetValue(k))

	// Removing one leaves the other intact.
	removeB()
	assert.Equal(t, []dynamicconfig.ConstrainedValue{
		{Constraints: nsA, Value: 1},
	}, c.GetValue(k))

	removeA()
	assert.Nil(t, c.GetValue(k))

	// A non-mergeable override stops the scan — partial overrides below it are invisible.
	removePartial := c.PartialOverrideValue(k, []dynamicconfig.ConstrainedValue{{Constraints: nsA, Value: 1}})
	removeFull := c.OverrideValue(k, 99)
	removePartial2 := c.PartialOverrideValue(k, []dynamicconfig.ConstrainedValue{{Constraints: nsB, Value: 13}})

	// Scan from end: partial nsB (merge, continue) → full 99 (take, stop). Partial nsA is below the full override.
	assert.Equal(t, []dynamicconfig.ConstrainedValue{
		{Constraints: nsB, Value: 13},
		{Value: 99},
	}, c.GetValue(k))

	removePartial2()
	removeFull()
	removePartial()
	assert.Nil(t, c.GetValue(k))
}

func TestMemoryClientPartialOverrideNonStackRemoval(t *testing.T) {
	c := dynamicconfig.NewMemoryClient()
	k := dynamicconfig.MakeKey("key")

	nsA := dynamicconfig.Constraints{Namespace: "ns-a"}
	nsB := dynamicconfig.Constraints{Namespace: "ns-b"}
	nsC := dynamicconfig.Constraints{Namespace: "ns-c"}

	// Three partial overrides.
	removeA := c.PartialOverrideValue(k, []dynamicconfig.ConstrainedValue{{Constraints: nsA, Value: 1}})
	removeB := c.PartialOverrideValue(k, []dynamicconfig.ConstrainedValue{{Constraints: nsB, Value: 2}})
	removeC := c.PartialOverrideValue(k, []dynamicconfig.ConstrainedValue{{Constraints: nsC, Value: 3}})

	assert.Equal(t, []dynamicconfig.ConstrainedValue{
		{Constraints: nsC, Value: 3},
		{Constraints: nsB, Value: 2},
		{Constraints: nsA, Value: 1},
	}, c.GetValue(k))

	// Remove the middle one (non-stack order). The other two must survive.
	removeB()
	assert.Equal(t, []dynamicconfig.ConstrainedValue{
		{Constraints: nsC, Value: 3},
		{Constraints: nsA, Value: 1},
	}, c.GetValue(k))

	// Remove the first one (still non-stack). Only C remains.
	removeA()
	assert.Equal(t, []dynamicconfig.ConstrainedValue{
		{Constraints: nsC, Value: 3},
	}, c.GetValue(k))

	removeC()
	assert.Nil(t, c.GetValue(k))
}

func TestMemoryClientSubscriptions(t *testing.T) {
	c := dynamicconfig.NewMemoryClient()
	k := dynamicconfig.MakeKey("key")

	calls := 0

	c.Subscribe(func(changed map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue) {
		calls++
		assert.Contains(t, changed, k)
		switch calls {
		case 1:
			assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 123}}, changed[k])
		case 2:
			assert.Nil(t, changed[k])
		case 3:
			assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 456}}, changed[k])
		case 4:
			assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 789}}, changed[k])
		case 5:
			assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 456}}, changed[k])
		case 6:
			assert.Nil(t, changed[k])
		}
	})

	remove := c.OverrideValue(k, 123)
	assert.Equal(t, 1, calls)
	remove()
	assert.Equal(t, 2, calls)

	remove1 := c.OverrideValue(k, 456)
	assert.Equal(t, 3, calls)
	remove2 := c.OverrideValue(k, 789)
	assert.Equal(t, 4, calls)
	remove2()
	assert.Equal(t, 5, calls)
	remove1()
	assert.Equal(t, 6, calls)
}
