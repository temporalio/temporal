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
	"path"
	"runtime"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/common/service/config"
)

type (
	VersionTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestVersionTestSuite(t *testing.T) {
	suite.Run(t, new(VersionTestSuite))
}

func (s *VersionTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *VersionTestSuite) TestParseVersion() {
	s.execParseTest("", 0, 0, false)
	s.execParseTest("0", 0, 0, false)
	s.execParseTest("99", 99, 0, false)
	s.execParseTest("0.0", 0, 0, false)
	s.execParseTest("0.9", 0, 9, false)
	s.execParseTest("1.0", 1, 0, false)
	s.execParseTest("9999.0", 9999, 0, false)
	s.execParseTest("999.999", 999, 999, false)
	s.execParseTest("88.88.88", 88, 88, false)
	s.execParseTest("a.b", 0, 0, true)
	s.execParseTest("1.5a", 0, 0, true)
	s.execParseTest("5.b", 0, 0, true)
	s.execParseTest("golang", 0, 0, true)
}

func (s *VersionTestSuite) TestCmpVersion() {

	s.Equal(0, cmpVersion("0", "0"))
	s.Equal(0, cmpVersion("999", "999"))
	s.Equal(0, cmpVersion("0.0", "0.0"))
	s.Equal(0, cmpVersion("0.999", "0.999"))
	s.Equal(0, cmpVersion("99.888", "99.888"))

	s.True(cmpVersion("0.1", "0") > 0)
	s.True(cmpVersion("0.5", "0.1") > 0)
	s.True(cmpVersion("1.1", "0.1") > 0)
	s.True(cmpVersion("1.1", "0.9") > 0)
	s.True(cmpVersion("1.1", "1.0") > 0)

	s.True(cmpVersion("0", "0.1") < 0)
	s.True(cmpVersion("0.1", "0.5") < 0)
	s.True(cmpVersion("0.1", "1.1") < 0)
	s.True(cmpVersion("0.9", "1.1") < 0)
	s.True(cmpVersion("1.0", "1.1") < 0)

	s.True(cmpVersion("0.1a", "0.5") < 0)
	s.True(cmpVersion("0.1", "0.5a") > 0)
	s.True(cmpVersion("ab", "cd") == 0)
}

func (s *VersionTestSuite) TestParseValidateVersion() {

	inputs := []string{"0", "1000", "9999", "0.1", "0.9", "99.9", "100.8"}
	for _, in := range inputs {
		s.execParseValidateTest(in, in, false)
		s.execParseValidateTest("v"+in, in, false)
	}

	errInputs := []string{"1.2a", "ab", "0.88", "5.11"}
	for _, in := range errInputs {
		s.execParseValidateTest(in, "", true)
		s.execParseValidateTest("v"+in, "", true)
	}
}

func (s *VersionTestSuite) execParseValidateTest(input string, output string, isErr bool) {
	ver, err := parseValidateVersion(input)
	if isErr {
		s.NotNil(err)
		return
	}
	s.Nil(err)
	s.Equal(output, ver)
}

func (s *VersionTestSuite) execParseTest(input string, expMajor int, expMinor int, isErr bool) {
	maj, min, err := parseVersion(input)
	if isErr {
		s.NotNil(err)
		return
	}
	s.Nil(err)
	s.Equal(expMajor, maj)
	s.Equal(expMinor, min)
}

func (s *VersionTestSuite) TestGetExpectedVersion() {
	s.T().Skip()
	flags := []struct {
		dirs     []string
		expected string
		err      string
	}{
		{[]string{"1.0"}, "1.0", ""},
		{[]string{"1.0", "2.0"}, "2.0", ""},
		{[]string{"abc"}, "", "no valid schemas"},
	}
	for _, flag := range flags {
		s.expectedVersionTest(flag.expected, flag.dirs, flag.err)
	}
}

func (s *VersionTestSuite) expectedVersionTest(expected string, dirs []string, errStr string) {
	tmpDir, err := ioutil.TempDir("", "version_test")
	s.NoError(err)
	defer os.RemoveAll(tmpDir)

	for _, dir := range dirs {
		s.createSchemaForVersion(tmpDir, dir)
	}
	v, err := getExpectedVersion(tmpDir)
	if len(errStr) == 0 {
		s.Equal(expected, v)
	} else {
		s.Error(err)
		s.Contains(err.Error(), errStr)
	}
}

func (s *VersionTestSuite) TestVerifyCompatibleVersion() {
	keyspace := "cadence_test"
	visKeyspace := "cadence_visibility_test"
	_, filename, _, ok := runtime.Caller(0)
	s.True(ok)
	root := path.Dir(path.Dir(path.Dir(filename)))
	cqlFile := path.Join(root, "schema/cadence/schema.cql")
	visCqlFile := path.Join(root, "schema/visibility/schema.cql")

	defer s.createKeyspace(keyspace)()
	defer s.createKeyspace(visKeyspace)()
	RunTool([]string{
		"./tool", "-k", keyspace, "-q", "setup-schema", "-f", cqlFile, "-version", "10.0", "-o",
	})
	RunTool([]string{
		"./tool", "-k", visKeyspace, "-q", "setup-schema", "-f", visCqlFile, "-version", "10.0", "-o",
	})

	cfg := config.Cassandra{
		Hosts:              "127.0.0.1",
		Port:               defaultCassandraPort,
		User:               "",
		Password:           "",
		Keyspace:           keyspace,
		VisibilityKeyspace: visKeyspace,
	}
	s.NoError(VerifyCompatibleVersion(cfg, root))
}

func (s *VersionTestSuite) TestCheckCompatibleVersion() {
	flags := []struct {
		expectedVersion string
		actualVersion   string
		errStr          string
		expectedFail    bool
	}{
		{"2.0", "1.0", "version mismatch", false},
		{"1.0", "1.0", "", false},
		{"1.0", "2.0", "", false},
		{"1.0", "abc", "unable to read cassandra schema version", false},
		{"abc", "1.0", "unable to read expected schema version", true},
	}
	for _, flag := range flags {
		s.runCheckCompatibleVersion(flag.expectedVersion, flag.actualVersion, flag.errStr, flag.expectedFail)
	}
}

func (s *VersionTestSuite) createKeyspace(keyspace string) func() {
	client, err := newCQLClient("127.0.0.1", defaultCassandraPort, "", "", "system")
	s.NoError(err)

	err = client.CreateKeyspace(keyspace, 1)
	if err != nil {
		log.Fatalf("error creating keyspace, err=%v", err)
	}
	return func() {
		s.NoError(client.DropKeyspace(keyspace))
		client.Close()
	}
}

func (s *VersionTestSuite) runCheckCompatibleVersion(
	expected string, actual string, errStr string, expectedFail bool,
) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	keyspace := fmt.Sprintf("version_test_%v", r.Int63())
	defer s.createKeyspace(keyspace)()

	dir := "check_version"
	tmpDir, err := ioutil.TempDir("", dir)
	s.NoError(err)
	defer os.RemoveAll(tmpDir)

	subdir := tmpDir + "/" + keyspace
	s.NoError(os.Mkdir(subdir, os.FileMode(0744)))

	s.createSchemaForVersion(subdir, actual)
	if expected != actual {
		s.createSchemaForVersion(subdir, expected)
	}

	cqlFile := subdir + "/v" + actual + "/tmp.cql"
	RunTool([]string{
		"./tool", "-k", keyspace, "-q", "setup-schema", "-f", cqlFile, "-version", actual, "-o",
	})
	if expectedFail {
		os.RemoveAll(subdir + "/v" + actual)
	}

	cfg := config.Cassandra{
		Hosts:    "127.0.0.1",
		Port:     defaultCassandraPort,
		User:     "",
		Password: "",
	}
	err = checkCompatibleVersion(cfg, keyspace, subdir)
	if len(errStr) > 0 {
		s.Error(err)
		s.Contains(err.Error(), errStr)
	} else {
		s.NoError(err)
	}
}

func (s *VersionTestSuite) createSchemaForVersion(subdir string, v string) {
	vDir := subdir + "/v" + v
	s.NoError(os.Mkdir(vDir, os.FileMode(0744)))
	cqlFile := vDir + "/tmp.cql"
	s.NoError(ioutil.WriteFile(cqlFile, []byte{}, os.FileMode(0644)))
}
