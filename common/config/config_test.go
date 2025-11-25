package config

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToString(t *testing.T) {
	cfg, err := Load(WithConfigDir("../../config"))
	require.NoError(t, err)
	require.NotEmpty(t, cfg.String())
}

func TestEmbeddedTemplateConsistentWithDockerTemplate(t *testing.T) {
	embeddedBytes, err := os.ReadFile("config_template_embedded.yaml")
	require.NoError(t, err)

	embeddedFiltered := skipComments(embeddedBytes, 3)
	dockerBytes, err := os.ReadFile("../../docker/config_template.yaml")
	require.NoError(t, err)
	dockerFiltered := skipComments(dockerBytes, 3)
	require.Equal(t, embeddedFiltered, dockerFiltered, "embedded template does not match docker template")

}

func skipComments(fb []byte, first int) string {
	lines := strings.Split(string(fb), "\n")
	i := 0
	var filtered []string
	for {
		if i >= first || i >= len(lines) {
			break
		}
		if strings.HasPrefix(strings.TrimSpace(lines[i]), "#") {
			i++
			continue
		}
		filtered = append(filtered, strings.TrimSpace(lines[i]))
		i++
	}
	filtered = append(filtered, lines[i:]...)
	return strings.Join(filtered, "\n")
}
