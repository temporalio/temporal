package config

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	LoaderSuite struct {
		*require.Assertions
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

func (s *LoaderSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *LoaderSuite) TestBaseYaml() {

	dir, err := ioutil.TempDir("", "loader.testBaseYaml")
	s.Nil(err)
	defer os.RemoveAll(dir)

	data := buildConfig("", "")
	err = ioutil.WriteFile(path(dir, "base.yaml"), []byte(data), fileMode)
	s.Nil(err)

	envs := []string{"", "prod"}
	zones := []string{"", "us-east-1a"}

	for _, env := range envs {
		for _, zone := range zones {
			var cfg testConfig
			err = Load(env, dir, zone, &cfg)
			s.Nil(err)
			s.Equal("hello__", cfg.Items.Item1)
			s.Equal("world__", cfg.Items.Item2)
		}
	}
}

func (s *LoaderSuite) TestHierarchy() {

	dir, err := ioutil.TempDir("", "loader.testHierarchy")
	s.Nil(err)
	defer os.RemoveAll(dir)

	s.createFile(dir, "base.yaml", "", "")
	s.createFile(dir, "development.yaml", "development", "")
	s.createFile(dir, "prod.yaml", "prod", "")
	s.createFile(dir, "prod_dca.yaml", "prod", "dca")

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
		{"prod", "", "hello_prod_", "world_prod_"},
		{"prod", "dca", "hello_prod_dca", "world_prod_dca"},
		{"prod", "pdx", "hello_prod_", "world_prod_"},
	}

	for _, tc := range testCases {
		var cfg testConfig
		err = Load(tc.env, dir, tc.zone, &cfg)
		s.Nil(err)
		s.Equal(tc.item1, cfg.Items.Item1)
		s.Equal(tc.item2, cfg.Items.Item2)
	}
}

func (s *LoaderSuite) TestInvalidPath() {
	var cfg testConfig
	err := Load("prod", "", "", &cfg)
	s.NotNil(err)
}

func (s *LoaderSuite) createFile(dir string, file string, env string, zone string) {
	err := ioutil.WriteFile(path(dir, file), []byte(buildConfig(env, zone)), fileMode)
	s.Nil(err)
}

func buildConfig(env, zone string) string {
	item1 := concat("hello", concat(env, zone))
	item2 := concat("world", concat(env, zone))
	return `
    items:
      item1: ` + item1 + `
      item2: ` + item2
}
