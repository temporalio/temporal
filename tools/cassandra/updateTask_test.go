// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cassandra

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
)

type (
	UpdateSchemaTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		rand     *rand.Rand
		keyspace string
		session  *gocql.Session
		client   CQLClient
		log      bark.Logger
	}
)

func TestUpdateSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(UpdateSchemaTestSuite))
}

func (s *UpdateSchemaTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *UpdateSchemaTestSuite) SetupSuite() {

	s.log = bark.NewLoggerFromLogrus(log.New())
	s.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	s.keyspace = fmt.Sprintf("update_schema_test_%v", s.rand.Int63())

	client, err := newCQLClient("127.0.0.1", defaultCassandraPort, "", "", "system")
	if err != nil {
		s.log.Fatal("Error creating CQLClient")
	}

	err = client.CreateKeyspace(s.keyspace, 1)
	if err != nil {
		log.Fatalf("error creating keyspace, err=%v", err)
	}

	s.client = client
}

func (s *UpdateSchemaTestSuite) TearDownSuite() {
	s.client.DropKeyspace(s.keyspace)
	s.client.Close()
}

func (s *UpdateSchemaTestSuite) TestUpdateSchema() {

	client, err := newCQLClient("127.0.0.1", defaultCassandraPort, "", "", s.keyspace)
	s.Nil(err)
	defer client.Close()

	tmpDir, err := ioutil.TempDir("", "update_schema_test")
	s.Nil(err)
	defer os.RemoveAll(tmpDir)

	s.makeSchemaVersionDirs(tmpDir)

	RunTool([]string{"./tool", "-k", s.keyspace, "-q", "setup-schema", "-v", "0.0"})
	RunTool([]string{"./tool", "-k", s.keyspace, "-q", "update-schema", "-d", tmpDir, "-v", "2.0"})

	expected := getExpectedTables(true)
	expected["domains"] = struct{}{}

	ver, err := client.ReadSchemaVersion()
	s.Nil(err)
	s.Equal("2.0", ver)

	tables, err := client.ListTables()
	s.Nil(err)
	s.Equal(len(expected), len(tables))

	for _, t := range tables {
		_, ok := expected[t]
		s.True(ok)
		delete(expected, t)
	}

	s.Equal(0, len(expected))

	dropAllTablesTypes(client)
}

func (s *UpdateSchemaTestSuite) TestDryrun() {

	client, err := newCQLClient("127.0.0.1", defaultCassandraPort, "", "", s.keyspace)
	s.Nil(err)
	defer client.Close()

	dir := "../../schema/cadence/versioned"
	RunTool([]string{"./tool", "-k", s.keyspace, "-q", "setup-schema", "-v", "0.0"})
	RunTool([]string{"./tool", "-k", s.keyspace, "-q", "update-schema", "-d", dir})

	ver, err := client.ReadSchemaVersion()
	s.Nil(err)
	// update the version to the latest
	s.log.Infof("Ver: %v", ver)
	s.Equal(0, cmpVersion(ver, "0.7"))

	dropAllTablesTypes(client)
}

func (s *UpdateSchemaTestSuite) makeSchemaVersionDirs(rootDir string) {

	mData := `{
		"CurrVersion": "1.0",
		"MinCompatibleVersion": "1.0",
		"Description": "base version of schema",
		"SchemaUpdateCqlFiles": ["base.cql"]
	}`

	dir := rootDir + "/v1.0"
	os.Mkdir(rootDir+"/v1.0", os.FileMode(0700))
	err := ioutil.WriteFile(dir+"/manifest.json", []byte(mData), os.FileMode(0600))
	s.Nil(err)
	err = ioutil.WriteFile(dir+"/base.cql", []byte(createTestCQLFileContent()), os.FileMode(0600))
	s.Nil(err)

	mData = `{
		"CurrVersion": "2.0",
		"MinCompatibleVersion": "1.0",
		"Description": "v2 of schema",
		"SchemaUpdateCqlFiles": ["domain.cql"]
	}`

	domain := `CREATE TABLE domains(
	  id     uuid,
	  domain text,
	  config text,
	  PRIMARY KEY (id)
	);`

	dir = rootDir + "/v2.0"
	os.Mkdir(rootDir+"/v2.0", os.FileMode(0700))
	err = ioutil.WriteFile(dir+"/manifest.json", []byte(mData), os.FileMode(0600))
	s.Nil(err)
	err = ioutil.WriteFile(dir+"/domain.cql", []byte(domain), os.FileMode(0600))
	s.Nil(err)
}

func (s *UpdateSchemaTestSuite) TestReadManifest() {

	tmpDir, err := ioutil.TempDir("", "update_schema_test")
	s.Nil(err)
	defer os.RemoveAll(tmpDir)

	input := `{
		"CurrVersion": "0.4",
		"MinCompatibleVersion": "0.1",
		"Description": "base version of schema",
		"SchemaUpdateCqlFiles": ["base1.cql", "base2.cql", "base3.cql"]
	}`
	files := []string{"base1.cql", "base2.cql", "base3.cql"}
	s.runReadManifestTest(tmpDir, input, "0.4", "0.1", "base version of schema", files, false)

	errInputs := []string{
		`{
			"MinCompatibleVersion": "0.1",
			"Description": "base",
			"SchemaUpdateCqlFiles": ["base1.cql"]
		 }`,
		`{
			"CurrVersion": "0.4",
			"Description": "base version of schema",
			"SchemaUpdateCqlFiles": ["base1.cql", "base2.cql", "base3.cql"]
		 }`,
		`{
			"CurrVersion": "0.4",
			"MinCompatibleVersion": "0.1",
			"Description": "base version of schema",
		 }`,
		`{
			"CurrVersion": "",
			"MinCompatibleVersion": "0.1",
			"Description": "base version of schema",
			"SchemaUpdateCqlFiles": ["base1.cql", "base2.cql", "base3.cql"]
		 }`,
		`{
			"CurrVersion": "0.4",
			"MinCompatibleVersion": "",
			"Description": "base version of schema",
			"SchemaUpdateCqlFiles": ["base1.cql", "base2.cql", "base3.cql"]
		 }`,
		`{
			"CurrVersion": "",
			"MinCompatibleVersion": "0.1",
			"Description": "base version of schema",
			"SchemaUpdateCqlFiles": []
		 }`,
	}

	for _, in := range errInputs {
		s.runReadManifestTest(tmpDir, in, "", "", "", nil, true)
	}
}

func (s *UpdateSchemaTestSuite) TestReadSchemaDir() {

	tmpDir, err := ioutil.TempDir("", "update_schema_test")
	s.Nil(err)
	defer os.RemoveAll(tmpDir)

	subDirs := []string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2", "abc", "2.0", "3.0"}
	for _, d := range subDirs {
		os.Mkdir(tmpDir+"/"+d, os.FileMode(0444))
	}

	_, err = readSchemaDir(tmpDir, "11.0", "11.2")
	s.NotNil(err)
	_, err = readSchemaDir(tmpDir, "0.5", "10.3")
	s.NotNil(err)

	ans, err := readSchemaDir(tmpDir, "0.4", "10.2")
	s.Nil(err)
	s.Equal([]string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2"}, ans)

	ans, err = readSchemaDir(tmpDir, "0.5", "3.5")
	s.Nil(err)
	s.Equal([]string{"v1.5", "v2.5", "v3.5"}, ans)
}

func (s *UpdateSchemaTestSuite) runReadManifestTest(dir, input, currVer, minVer, desc string,
	files []string, isErr bool) {

	file := dir + "/manifest.json"
	err := ioutil.WriteFile(file, []byte(input), os.FileMode(0644))
	s.Nil(err)

	m, err := readManifest(dir)
	if isErr {
		s.NotNil(err)
		return
	}
	s.Nil(err)
	s.Equal(currVer, m.CurrVersion)
	s.Equal(minVer, m.MinCompatibleVersion)
	s.Equal(desc, m.Description)
	s.True(len(m.md5) > 0)
	s.Equal(files, m.SchemaUpdateCqlFiles)
}
