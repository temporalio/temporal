package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToString(t *testing.T) {
	cfg, err := Load(WithConfigDir("../../config"))
	require.NoError(t, err)
	require.NotEmpty(t, cfg.String())
}
