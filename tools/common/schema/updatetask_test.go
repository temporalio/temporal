// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap/zaptest"

	"go.temporal.io/server/common/log"
	dbschemas "go.temporal.io/server/schema"
	"go.temporal.io/server/tests/testutils"
)

type UpdateTaskTestSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(n) will stop the test, not merely log an error
	suite.Suite
	versionsDir string
	emptyDir    string
	logger      log.Logger
}

func TestUpdateTaskTestSuite(t *testing.T) {
	suite.Run(t, new(UpdateTaskTestSuite))
}

var updateTaskTestData = struct {
	versions []string
}{
	versions: []string{"v0.5", "v1.5", "v2.5", "v2.5.1", "v2.5.2", "v2.7.17", "v3.5", "v10.2", "abc", "2.0", "3.0"},
}

func (s *UpdateTaskTestSuite) SetupSuite() {
	s.Assertions = require.New(s.T())

	s.versionsDir = testutils.MkdirTemp(s.T(), "", "update_schema_test")

	for _, d := range updateTaskTestData.versions {
		s.NoError(os.Mkdir(s.versionsDir+"/"+d, os.FileMode(0444)))
	}

	s.emptyDir = testutils.MkdirTemp(s.T(), "", "update_schema_test_empty")

	s.logger = log.NewZapLogger(zaptest.NewLogger(s.T()))
}

func (s *UpdateTaskTestSuite) TestReadSchemaDir() {
	ans, err := readSchemaDir(os.DirFS(s.versionsDir), ".", "0.5", "1.5", s.logger)
	s.NoError(err)
	s.Equal([]string{"v1.5"}, ans)

	ans, err = readSchemaDir(os.DirFS(s.versionsDir), ".", "0.4", "10.2", s.logger)
	s.NoError(err)
	s.Equal([]string{"v0.5", "v1.5", "v2.5", "v2.5.1", "v2.5.2", "v2.7.17", "v3.5", "v10.2"}, ans)

	ans, err = readSchemaDir(os.DirFS(s.versionsDir), ".", "0.5", "3.5", s.logger)
	s.NoError(err)
	s.Equal([]string{"v1.5", "v2.5", "v2.5.1", "v2.5.2", "v2.7.17", "v3.5"}, ans)

	// Start version found, no later versions. Return nothing.
	ans, err = readSchemaDir(os.DirFS(s.versionsDir), ".", "10.2", "", s.logger)
	s.NoError(err)
	s.Equal(0, len(ans))

	// Start version not found, no later versions. Return nothing.
	ans, err = readSchemaDir(os.DirFS(s.versionsDir), ".", "10.3", "", s.logger)
	s.NoError(err)
	s.Equal(0, len(ans))

	ans, err = readSchemaDir(os.DirFS(s.versionsDir), ".", "2.5.2", "", s.logger)
	s.NoError(err)
	s.Equal([]string{"v2.7.17", "v3.5", "v10.2"}, ans)
}

func (s *UpdateTaskTestSuite) TestReadSchemaDirEFS() {
	fsys := dbschemas.Assets()
	versionsDir := "mysql/v8/temporal/versioned"
	versions := []string{"v1.0", "v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6", "v1.7", "v1.8", "v1.9", "v1.10", "v1.11"}

	ans, err := readSchemaDir(fsys, versionsDir, "1.0", "1.1", s.logger)
	s.NoError(err)
	s.Equal([]string{"v1.1"}, ans)

	ans, err = readSchemaDir(fsys, versionsDir, "0.4", "1.11", s.logger)
	s.NoError(err)
	s.Equal(versions, ans)

	ans, err = readSchemaDir(fsys, versionsDir, "0.5", "1.9", s.logger)
	s.NoError(err)
	s.Equal([]string{"v1.0", "v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6", "v1.7", "v1.8", "v1.9"}, ans)

	// Not running the tests with "no later versions" because they will break when we add more versions to the schema dir
}

func (s *UpdateTaskTestSuite) TestSortAndFilterVersionsWithEndLessThanStart_ReturnsError() {
	_, err := sortAndFilterVersions(updateTaskTestData.versions, "1.5", "0.5", s.logger)
	s.Error(err)
	assert.Containsf(s.T(), err.Error(), "less than end version", "Unexpected error message")
}

func (s *UpdateTaskTestSuite) TestReadSchemaDirWithEndVersion_ReturnsErrorWhenNotFound() {
	// No versions in range
	_, err := readSchemaDir(os.DirFS(s.versionsDir), ".", "11.0", "11.2", s.logger)
	s.Error(err)
	assert.Containsf(s.T(), err.Error(), "specified but not found", "Unexpected error message")

	// Versions in range, but nothing for v10.3
	_, err = readSchemaDir(os.DirFS(s.versionsDir), ".", "0.5", "10.3", s.logger)
	s.Error(err)
	assert.Containsf(s.T(), err.Error(), "specified but not found", "Unexpected error message")
}

func (s *UpdateTaskTestSuite) TestReadSchemaDirWithSameStartAndEnd_ReturnsEmptyList() {
	ans, err := readSchemaDir(os.DirFS(s.versionsDir), ".", "1.7", "1.7", s.logger)
	s.NoError(err)
	assert.Equal(s.T(), 0, len(ans))
}

func (s *UpdateTaskTestSuite) TestReadSchemaDirWithEmptyDir_ReturnsError() {
	_, err := readSchemaDir(os.DirFS(s.emptyDir), ".", "11.0", "", s.logger)
	s.Error(err)
	assert.Containsf(s.T(), err.Error(), "contains no subDirs", "Unexpected error message")

	_, err = readSchemaDir(os.DirFS(s.emptyDir), ".", "10.1", "", s.logger)
	s.Error(err)
	assert.Containsf(s.T(), err.Error(), "contains no subDirs", "Unexpected error message")
}

func (s *UpdateTaskTestSuite) TestReadManifest() {
	tmpDir := testutils.MkdirTemp(s.T(), "", "update_schema_test")

	validCases := []struct {
		schema string
		files  []string
	}{
		{
			schema: `{
				"CurrVersion": "0.4",
				"MinCompatibleVersion": "0.1",
				"Description": "upgrade",
				"SchemaUpdateCqlFiles": ["base1.cql", "base2.cql", "base3.cql"]
			}`,

			files: []string{"base1.cql", "base2.cql", "base3.cql"},
		},
		{
			schema: `{
				"CurrVersion": "0.4",
				"MinCompatibleVersion": "0.1",
				"Description": "upgrade",
				"AllowNoCqlFiles": true
			}`,
			files: nil,
		},
	}
	for _, c := range validCases {
		s.runReadManifestTest(tmpDir, c.schema, "0.4", "0.1", "upgrade", c.files, false)
	}

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

func (s *UpdateTaskTestSuite) runReadManifestTest(
	dir, input, currVer, minVer, desc string,
	files []string, isErr bool,
) {

	file := dir + "/manifest.json"
	err := os.WriteFile(file, []byte(input), os.FileMode(0644))
	s.Nil(err)

	m, err := readManifest(os.DirFS(dir), ".")
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
