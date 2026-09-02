package chasm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockCtxKeyType struct{}

var mockCtxKey = mockCtxKeyType{}

type mockCtxValuesComponent struct {
	UnimplementedComponent
}

func (c *mockCtxValuesComponent) LifecycleState(_ Context) LifecycleState {
	return LifecycleStateRunning
}

type mockCtxValuesLibrary struct {
	UnimplementedLibrary
}

func (l *mockCtxValuesLibrary) Name() string { return "mockCtxValues" }

func (l *mockCtxValuesLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{
		NewRegistrableComponent[*mockCtxValuesComponent](
			"component",
			WithContextValues(map[any]any{mockCtxKey: "from-library"}),
		),
	}
}

func TestMockContextValue(t *testing.T) {
	t.Run("ReturnsRegisteredComponentValues", func(t *testing.T) {
		c := &MockContext{}
		c.RegisterComponentContextValues(map[any]any{mockCtxKey: "registered"})
		require.Equal(t, "registered", c.Value(mockCtxKey))
	})

	t.Run("RegisterLibraryCopiesComponentValues", func(t *testing.T) {
		c := &MockContext{}
		c.RegisterLibrary(&mockCtxValuesLibrary{})
		require.Equal(t, "from-library", c.Value(mockCtxKey))
	})

	t.Run("GoCtxTakesPrecedence", func(t *testing.T) {
		c := &MockContext{GoCtx: context.WithValue(context.Background(), mockCtxKey, "from-goctx")}
		c.RegisterComponentContextValues(map[any]any{mockCtxKey: "registered"})
		require.Equal(t, "from-goctx", c.Value(mockCtxKey))
	})

	// withValue derives a new context; registered values must survive the copy.
	t.Run("RegisteredValuesSurviveWithValue", func(t *testing.T) {
		c := &MockContext{}
		c.RegisterComponentContextValues(map[any]any{mockCtxKey: "registered"})
		derived := c.withValue("other", "value")
		require.Equal(t, "registered", derived.Value(mockCtxKey))
	})

	t.Run("UnknownKeyIsNil", func(t *testing.T) {
		c := &MockContext{}
		require.Nil(t, c.Value(mockCtxKey))
	})
}
