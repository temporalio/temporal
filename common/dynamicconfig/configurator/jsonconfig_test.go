package configurator_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig/configurator"
)

func TestJSONConfig(t *testing.T) {
	const raw = `{
		"DefaultValue": "off",
		"Overrides": [
			{"MatchString": "\"env\" = \"prod\"", "MatchResult": "on"}
		]
	}`

	cfg, err := configurator.JSONConfig[string]([]byte(raw))
	require.NoError(t, err)
	require.Equal(t, "off", cfg.DefaultValue)
	require.Len(t, cfg.Overrides, 1)
	require.Equal(t, "on", cfg.Overrides[0].MatchResult)

	c := configurator.New[string]()
	require.NoError(t, c.Load("k", cfg))

	v, err := c.Eval(t.Context(), "k", configurator.Constraints{"env": "prod"})
	require.NoError(t, err)
	require.Equal(t, "on", v)

	v, err = c.Eval(t.Context(), "k", configurator.Constraints{"env": "dev"})
	require.NoError(t, err)
	require.Equal(t, "off", v)
}

func TestJSONConfig_Errors(t *testing.T) {
	_, err := configurator.JSONConfig[int]([]byte(`{"DefaultValue": "not an int"}`))
	require.ErrorContains(t, err, "decoding default value")

	_, err = configurator.JSONConfig[int]([]byte(`{"DefaultValue": 1, "Overrides": [
		{"MatchString": "\"a\" = \"b\"", "MatchResult": "not an int"}]}`))
	require.ErrorContains(t, err, "decoding match result")

	_, err = configurator.JSONConfig[int]([]byte(`not json`))
	require.ErrorContains(t, err, "decoding config")
}

// The reason Temporal does not use JSONConfig: encoding/json has its own opinions about
// types, and they do not match Go's. Decoding into `any` turns every number into a float64,
// which will not satisfy a caller that wanted an int. Callers with their own type system
// should decode values themselves and build a Config[V] directly.
func TestJSONConfig_NumbersBecomeFloatsUnderAny(t *testing.T) {
	cfg, err := configurator.JSONConfig[any]([]byte(`{"DefaultValue": 100}`))
	require.NoError(t, err)
	require.IsType(t, float64(0), cfg.DefaultValue)

	// Whereas a Config built directly keeps whatever the caller put in it.
	direct := configurator.Config[any]{DefaultValue: 100}
	require.IsType(t, int(0), direct.DefaultValue)
}

func TestLoadJSON(t *testing.T) {
	c := configurator.New[bool]()
	require.NoError(t, configurator.LoadJSON(c, "flag", []byte(`{
		"DefaultValue": false,
		"Overrides": [{"MatchString": "\"env\" = \"prod\"", "MatchResult": true}]
	}`)))

	v, err := c.Eval(t.Context(), "flag", configurator.Constraints{"env": "prod"})
	require.NoError(t, err)
	require.True(t, v)

	require.ErrorContains(t,
		configurator.LoadJSON(c, "bad", []byte(`{"DefaultValue": "not a bool"}`)),
		`couldn't load "bad"`)
}
