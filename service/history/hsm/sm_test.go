package hsm_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
)

type event struct{ fail bool }

var handlerErr = errors.New("test")

var transition = hsm.NewTransition(
	[]hsmtest.State{hsmtest.State1, hsmtest.State2},
	hsmtest.State3,
	func(d *hsmtest.Data, e event) (hsm.TransitionOutput, error) {
		if e.fail {
			return hsm.TransitionOutput{}, handlerErr
		}
		return hsm.TransitionOutput{}, nil
	},
)

func TestTransition_Possible(t *testing.T) {
	d := hsmtest.NewData(hsmtest.State4)
	require.False(t, transition.Possible(d))
	d = hsmtest.NewData(hsmtest.State3)
	require.False(t, transition.Possible(d))
	d = hsmtest.NewData(hsmtest.State1)
	require.True(t, transition.Possible(d))
	d = hsmtest.NewData(hsmtest.State2)
	require.True(t, transition.Possible(d))
}

func TestTransition_ValidTransition(t *testing.T) {
	d := hsmtest.NewData(hsmtest.State1)
	_, err := transition.Apply(d, event{})
	require.NoError(t, err)
	require.Equal(t, hsmtest.State3, d.State())
}

func TestTransition_InvalidTransition(t *testing.T) {
	d := hsmtest.NewData(hsmtest.State4)
	_, err := transition.Apply(d, event{})
	require.ErrorIs(t, err, hsm.ErrInvalidTransition)
	require.Equal(t, hsmtest.State4, d.State())
}

func TestTransition_HandlerError(t *testing.T) {
	d := hsmtest.NewData(hsmtest.State1)
	_, err := transition.Apply(d, event{fail: true})
	require.ErrorIs(t, err, handlerErr)
}
