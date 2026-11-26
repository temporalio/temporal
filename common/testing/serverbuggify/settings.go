package serverbuggify

import (
	"maps"
	"slices"
	"sync"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/testx/failpoint/buggify"
)

// ChooseSettings picks values for the dynamic config settings of BuggifiableSettings,
// given as their alternative values by key. A setting is randomized if config activates the name
// SettingPrefix+key, and then gets one of its values. Both choices are pure functions of the seed
// and the key.
func ChooseSettings(config buggify.Config, marked map[dynamicconfig.Key][]any) map[dynamicconfig.Key]any {
	chosen := make(map[dynamicconfig.Key]any)
	for key, values := range marked {
		name := SettingPrefix + key.String()
		if len(values) == 0 || !config.Activated(name) {
			continue
		}
		i := min(int(config.Draw(name, 1)*float64(len(values))), len(values)-1)
		chosen[key] = values[i]
	}
	return chosen
}

// NewRandomizingClient returns a client that reads from inner, but returns the value in chosen, as
// a value without constraints, for a key that inner has no value without constraints for. So
// dynamic config values that are set explicitly, with or without constraints, take precedence over
// the chosen values. The client notifies about changes if inner does.
func NewRandomizingClient(inner dynamicconfig.Client, chosen map[dynamicconfig.Key]any) dynamicconfig.Client {
	c := &randomizingClient{inner: inner, chosen: make(map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue, len(chosen))}
	for key, value := range chosen {
		c.chosen[key] = []dynamicconfig.ConstrainedValue{{Value: value}}
	}
	if notifying, ok := inner.(dynamicconfig.NotifyingClient); ok {
		return &notifyingRandomizingClient{randomizingClient: c, notifying: notifying}
	}
	return c
}

type randomizingClient struct {
	inner dynamicconfig.Client
	// chosen holds the value of each randomized setting, by key.
	chosen map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue
	// patched holds a *patchedValue per key, so that the same values of inner return the same
	// slice, which the dynamic config collection caches conversions for.
	patched sync.Map
}

type patchedValue struct {
	// first is the first element of the values of inner that out extends.
	first *dynamicconfig.ConstrainedValue
	out   []dynamicconfig.ConstrainedValue
}

func (c *randomizingClient) GetValue(key dynamicconfig.Key) []dynamicconfig.ConstrainedValue {
	return c.patch(key, c.inner.GetValue(key))
}

// patch returns cvs, the values of inner for key, with the chosen value for key if it has one and
// cvs has no value without constraints.
func (c *randomizingClient) patch(key dynamicconfig.Key, cvs []dynamicconfig.ConstrainedValue) []dynamicconfig.ConstrainedValue {
	chosen, ok := c.chosen[key]
	if !ok {
		return cvs
	}
	if len(cvs) == 0 {
		return chosen
	}
	if slices.ContainsFunc(cvs, func(cv dynamicconfig.ConstrainedValue) bool {
		return cv.Constraints == dynamicconfig.Constraints{}
	}) {
		return cvs
	}
	if p, ok := c.patched.Load(key); ok {
		if p := p.(*patchedValue); p.first == &cvs[0] && len(p.out) == len(cvs)+1 {
			return p.out
		}
	}
	out := append(slices.Clone(cvs), chosen...)
	c.patched.Store(key, &patchedValue{first: &cvs[0], out: out})
	return out
}

type notifyingRandomizingClient struct {
	*randomizingClient
	notifying dynamicconfig.NotifyingClient
}

func (c *notifyingRandomizingClient) Subscribe(update dynamicconfig.ClientUpdateFunc) func() {
	return c.notifying.Subscribe(func(changed map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue) {
		// The map can be shared with other subscribers, so it is copied before it is changed.
		patched := maps.Clone(changed)
		for key, cvs := range changed {
			patched[key] = c.patch(key, cvs)
		}
		update(patched)
	})
}
