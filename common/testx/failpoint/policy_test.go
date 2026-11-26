package failpoint_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
)

type stubPolicy struct {
	errFn    func(name, scope string) error
	switchFn func(name, scope string) bool
}

func (p stubPolicy) Error(name, scope string) error {
	if p.errFn != nil {
		return p.errFn(name, scope)
	}
	return nil
}

func (p stubPolicy) Trip(name, scope string) bool {
	if p.switchFn != nil {
		return p.switchFn(name, scope)
	}
	return false
}

func TestRegistry_Policy(t *testing.T) {
	t.Parallel()
	policyErr := errors.New("policy error")
	regErr := errors.New("registered error")
	key := failpoint.NewKey[error]("policy.error")
	switchKey := failpoint.NewKey[bool]("policy.switch")
	strKey := failpoint.NewKey[string]("policy.string")

	t.Run("SetPolicy controls MayFire and Policy", func(t *testing.T) {
		t.Parallel()
		reg := failpoint.NewRegistry()
		require.False(t, reg.MayFire())
		require.Nil(t, reg.Policy())

		p := stubPolicy{errFn: func(string, string) error { return policyErr }}
		reg.SetPolicy(p)
		require.True(t, reg.MayFire())
		require.NotNil(t, reg.Policy())

		reg.SetPolicy(nil)
		require.False(t, reg.MayFire())
		require.Nil(t, reg.Policy())
	})

	t.Run("Error policy runs on Execute miss of an error key and yields to registered failpoint", func(t *testing.T) {
		t.Parallel()
		reg := failpoint.NewRegistry()
		var gotName, gotScope string
		reg.SetPolicy(stubPolicy{
			errFn: func(name, scope string) error {
				gotName, gotScope = name, scope
				return policyErr
			},
		})

		// Miss consults policy with key name and scope.
		require.ErrorIs(t, executeError(reg, key.ForScope("ns-1")), policyErr)
		require.Equal(t, key.Name(), gotName)
		require.Equal(t, "ns-1", gotScope)

		// Non-error Execute does not consult Error policy.
		_, set := strKey.Execute(reg)
		require.False(t, set)

		// Registered failpoint wins over policy.
		fp := failpoint.New[error](key.ForScope("ns-1"))
		fp.Return(regErr)
		reg.Set(fp)
		require.ErrorIs(t, executeError(reg, key.ForScope("ns-1")), regErr)

		// Disabled registered failpoint still suppresses policy.
		fp.Disable()
		require.NoError(t, executeError(reg, key.ForScope("ns-1")))

		// Other scopes still consult policy.
		require.ErrorIs(t, executeError(reg, key.ForScope("ns-2")), policyErr)
		require.Equal(t, "ns-2", gotScope)

		// Unscoped failpoint suppresses policy for every scope.
		unscoped := failpoint.New[error](key)
		reg.Set(unscoped)
		require.NoError(t, executeError(reg, key.ForScope("ns-2")))
	})

	t.Run("Trip policy runs on Trip and TripWithPolicyScope", func(t *testing.T) {
		t.Parallel()
		reg := failpoint.NewRegistry()
		var gotName, gotScope string
		reg.SetPolicy(stubPolicy{
			switchFn: func(name, scope string) bool {
				gotName, gotScope = name, scope
				return true
			},
		})

		require.True(t, switchKey.ForScope("id-1").Trip(reg))
		require.Equal(t, switchKey.Name(), gotName)
		require.Equal(t, "id-1", gotScope)

		require.True(t, switchKey.ForScope("id-1").TripWithPolicyScope(reg, "name-1"))
		require.Equal(t, switchKey.Name(), gotName)
		require.Equal(t, "name-1", gotScope)

		// Disabled failpoint on key scope suppresses policy regardless of policyScope.
		fp := failpoint.New[bool](switchKey.ForScope("id-1"))
		reg.Set(fp)
		require.False(t, switchKey.ForScope("id-1").TripWithPolicyScope(reg, "name-1"))
		fp.On()
		require.True(t, switchKey.ForScope("id-1").TripWithPolicyScope(reg, "name-1"))
	})

	t.Run("nil registry is inert", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, executeError(nil, key))
		require.False(t, switchKey.Trip(nil))
		require.False(t, switchKey.TripWithPolicyScope(nil, "ns"))
	})
}
