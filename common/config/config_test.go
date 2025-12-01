package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToString(t *testing.T) {
	cfg, err := Load(WithConfigDir("../../config"))
	require.NoError(t, err)
	require.NotEmpty(t, cfg.String())
}

func TestEmbeddedTemplateOnlyDiffersFromDockerByComment(t *testing.T) {
	embeddedContent, err := os.ReadFile("config_template_embedded.yaml")
	require.NoError(t, err)

	dockerContent, err := os.ReadFile("../../docker/config_template.yaml")
	require.NoError(t, err)

	dockerWithComment := "# enable-template\n" + string(dockerContent)

	require.Equal(t, dockerWithComment, string(embeddedContent),
		"Embedded template (config_template_embedded.yaml) must only differ from docker template "+
			"(docker/config_template.yaml) by the '# enable-template' comment at the top. "+
			"This comment is required for the config loader to detect and process the template.")
}
