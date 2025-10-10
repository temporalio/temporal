package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/tests/testutils"
)

const fileMode = os.FileMode(0644)

type (
	LoaderSuite struct {
		suite.Suite
	}

	itemsConfig struct {
		Item1 string `yaml:"item1"`
		Item2 string `yaml:"item2"`
	}

	testConfig struct {
		Items itemsConfig `yaml:"items"`
	}
)

func TestLoaderSuite(t *testing.T) {
	suite.Run(t, new(LoaderSuite))
}

func (s *LoaderSuite) TestBaseYaml() {
	dir := testutils.MkdirTemp(s.T(), "", "loader.testBaseYaml")

	data := buildConfig(false, "", "")
	err := os.WriteFile(path(dir, "base.yaml"), []byte(data), fileMode)
	require.Nil(s.T(), err)

	envs := []string{"", "prod"}
	zones := []string{"", "us-east-1a"}

	for _, env := range envs {
		for _, zone := range zones {
			var cfg testConfig
			err = Load(env, dir, zone, &cfg)
			require.Nil(s.T(), err)
			require.Equal(s.T(), "hello__", cfg.Items.Item1)
			require.Equal(s.T(), "world__", cfg.Items.Item2)
		}
	}
}

func (s *LoaderSuite) TestHierarchy() {
	dir := testutils.MkdirTemp(s.T(), "", "loader.testHierarchy")

	s.createFile(dir, "base.yaml", false, "", "")
	s.createFile(dir, "development.yaml", false, "development", "")
	s.createFile(dir, "prod.yaml", true, "prod", "")
	s.createFile(dir, "prod_dca.yaml", true, "prod", "dca")

	testCases := []struct {
		env   string
		zone  string
		item1 string
		item2 string
	}{
		{"", "", "hello_development_", "world_development_"},
		{"", "dca", "hello_development_", "world_development_"},
		{"", "pdx", "hello_development_", "world_development_"},
		{"development", "", "hello_development_", "world_development_"},
		{"development", "dca", "hello_development_", "world_development_"},
		{"development", "pdx", "hello_development_", "world_development_"},
		{"prod", "", "HELLO_PROD_", "world_prod_"},
		{"prod", "dca", "HELLO_PROD_DCA", "world_prod_dca"},
		{"prod", "pdx", "HELLO_PROD_", "world_prod_"},
	}

	for _, tc := range testCases {
		var cfg testConfig
		err := Load(tc.env, dir, tc.zone, &cfg)
		require.Nil(s.T(), err)
		require.Equal(s.T(), tc.item1, cfg.Items.Item1)
		require.Equal(s.T(), tc.item2, cfg.Items.Item2)
	}
}

func (s *LoaderSuite) TestInvalidPath() {
	var cfg testConfig
	err := Load("prod", "", "", &cfg)
	require.NotNil(s.T(), err)
}

func (s *LoaderSuite) createFile(dir string, file string, template bool, env string, zone string) {
	err := os.WriteFile(path(dir, file), []byte(buildConfig(template, env, zone)), fileMode)
	require.Nil(s.T(), err)
}

func buildConfig(template bool, env, zone string) string {
	comment := ""
	if template {
		comment = "# enable-template\n"
	}
	item1 := concat("hello", concat(env, zone))
	if template {
		item1 = `{{ "` + item1 + `" | upper }}`
	}
	item2 := concat("world", concat(env, zone))
	return comment + `
    items:
      item1: ` + item1 + `
      item2: ` + item2
}
