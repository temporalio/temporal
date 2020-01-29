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

package schema

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type UpdateTaskTestSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestUpdateTaskTestSuite(t *testing.T) {
	suite.Run(t, new(UpdateTaskTestSuite))
}

func (s *UpdateTaskTestSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *UpdateTaskTestSuite) TestReadSchemaDir() {

	emptyDir, err := ioutil.TempDir("", "update_schema_test_empty")
	s.NoError(err)
	defer os.RemoveAll(emptyDir)

	tmpDir, err := ioutil.TempDir("", "update_schema_test")
	s.NoError(err)
	defer os.RemoveAll(tmpDir)

	subDirs := []string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2", "abc", "2.0", "3.0"}
	for _, d := range subDirs {
		s.NoError(os.Mkdir(tmpDir+"/"+d, os.FileMode(0444)))
	}

	_, err = readSchemaDir(tmpDir, "11.0", "11.2")
	s.Error(err)
	_, err = readSchemaDir(tmpDir, "0.5", "10.3")
	s.Error(err)
	_, err = readSchemaDir(tmpDir, "1.5", "1.5")
	s.Error(err)
	_, err = readSchemaDir(tmpDir, "1.5", "0.5")
	s.Error(err)
	_, err = readSchemaDir(tmpDir, "10.3", "")
	s.Error(err)
	_, err = readSchemaDir(emptyDir, "11.0", "")
	s.Error(err)
	_, err = readSchemaDir(emptyDir, "10.1", "")
	s.Error(err)

	ans, err := readSchemaDir(tmpDir, "0.4", "10.2")
	s.NoError(err)
	s.Equal([]string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2"}, ans)

	ans, err = readSchemaDir(tmpDir, "0.5", "3.5")
	s.NoError(err)
	s.Equal([]string{"v1.5", "v2.5", "v3.5"}, ans)

	ans, err = readSchemaDir(tmpDir, "10.2", "")
	s.NoError(err)
	s.Equal(0, len(ans))
}

func (s *UpdateTaskTestSuite) TestReadManifest() {

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

func (s *UpdateTaskTestSuite) runReadManifestTest(dir, input, currVer, minVer, desc string,
	files []string, isErr bool) {

	file := dir + "/manifest.json"
	err := ioutil.WriteFile(file, []byte(input), os.FileMode(0644))
	s.Nil(err)

	m, err := readManifest(dir)
	if isErr {
		s.Error(err)
		return
	}
	s.NoError(err)
	s.Equal(currVer, m.CurrVersion)
	s.Equal(minVer, m.MinCompatibleVersion)
	s.Equal(desc, m.Description)
	s.True(len(m.md5) > 0)
	s.Equal(files, m.SchemaUpdateCqlFiles)
}
