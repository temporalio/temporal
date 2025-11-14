package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToString(t *testing.T) {
	var cfg Config
	err := Load("", "../../config", "", &cfg)
	require.NoError(t, err)
	require.NotEmpty(t, cfg.String())
}
