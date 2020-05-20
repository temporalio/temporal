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

	squashDir, err := ioutil.TempDir("", "update_schema_test_squash")
	s.NoError(err)
	defer os.RemoveAll(squashDir)

	subDirs := []string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2", "abc", "2.0", "3.0"}
	for _, d := range subDirs {
		s.NoError(os.Mkdir(tmpDir+"/"+d, os.FileMode(0444)))
	}

	squashDirs := []string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2", "abc", "2.0", "3.0", "s0.5-10.2", "s1.5-3.5"}
	for _, d := range squashDirs {
		s.NoError(os.Mkdir(squashDir+"/"+d, os.FileMode(0444)))
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

	ans, err = readSchemaDir(squashDir, "0.4", "10.2")
	s.NoError(err)
	s.Equal([]string{"v0.5", "s0.5-10.2"}, ans)

	ans, err = readSchemaDir(squashDir, "0.5", "3.5")
	s.NoError(err)
	s.Equal([]string{"v1.5", "s1.5-3.5"}, ans)

	ans, err = readSchemaDir(squashDir, "10.2", "")
	s.NoError(err)
	s.Empty(ans)
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

func (s *UpdateTaskTestSuite) TestFilterDirectories() {
	tests := []struct {
		name       string
		startVer   string
		endVer     string
		wantErr    string
		emptyDir   bool
		wantResult []string
	}{
		{name: "endVer highter", startVer: "11.0", endVer: "11.2", wantErr: "version dir not found for target version 11.2"},
		{name: "both outside", startVer: "0.5", endVer: "10.3", wantErr: "version dir not found for target version 10.3"},
		{name: "versions equal", startVer: "1.5", endVer: "1.5", wantErr: "startVer (1.5) must be less than endVer (1.5)"},
		{name: "endVer < startVer", startVer: "1.5", endVer: "0.5", wantErr: "startVer (1.5) must be less than endVer (0.5)"},
		{name: "startVer highter", startVer: "10.3", wantErr: "no subdirs found with version >= 10.3"},
		{name: "empty set 1", startVer: "11.0", wantErr: "no subdirs found with version >= 11.0", emptyDir: true},
		{name: "empty set 2", startVer: "10.1", wantErr: "no subdirs found with version >= 10.1", emptyDir: true},
		{name: "all versions", startVer: "0.4", endVer: "10.2", wantResult: []string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2"}},
		{name: "subset", startVer: "0.5", endVer: "3.5", wantResult: []string{"v1.5", "v2.5", "v3.5"}},
		{name: "already at last version", startVer: "10.2"},
	}
	subDirs := []string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2", "abc", "2.0", "3.0"}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			var dirs []string
			if !tt.emptyDir {
				dirs = subDirs
			}
			r, sq, err := filterDirectories(dirs, tt.startVer, tt.endVer)
			s.Empty(sq)
			if tt.wantErr != "" {
				s.Empty(r)
				s.EqualError(err, tt.wantErr)
			} else {
				s.NoError(err)
				s.Equal(tt.wantResult, r)
			}
		})
	}
}

func (s *UpdateTaskTestSuite) TestFilterSquashDirectories() {
	tests := []struct {
		name       string
		startVer   string
		endVer     string
		wantErr    string
		wantResult []string
		wantSq     []squashVersion
		customDirs []string
	}{
		{name: "endVer highter", startVer: "11.0", endVer: "11.2", wantErr: "version dir not found for target version 11.2"},
		{name: "both outside", startVer: "0.5", endVer: "10.3", wantErr: "version dir not found for target version 10.3"},
		{name: "versions equal", startVer: "1.5", endVer: "1.5", wantErr: "startVer (1.5) must be less than endVer (1.5)"},
		{name: "endVer < startVer", startVer: "1.5", endVer: "0.5", wantErr: "startVer (1.5) must be less than endVer (0.5)"},
		{name: "startVer highter", startVer: "10.3", wantErr: "no subdirs found with version >= 10.3"},
		{
			name:       "backward version squash",
			startVer:   "1.5",
			wantErr:    "invalid squashed version \"s3.0-2.0\", 3.0 >= 2.0",
			customDirs: []string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2", "abc", "2.0", "3.0", "s0.5-10.2", "s1.5-3.5", "s3.0-2.0"},
		},
		{
			name:       "equal version squash",
			startVer:   "1.5",
			wantErr:    "invalid squashed version \"s2.0-2.0\", 2.0 >= 2.0",
			customDirs: []string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2", "abc", "2.0", "3.0", "s0.5-10.2", "s1.5-3.5", "s2.0-2.0"},
		},
		{
			name:       "all versions",
			startVer:   "0.4",
			endVer:     "10.2",
			wantResult: []string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2"},
			wantSq: []squashVersion{
				{prev: "1.5", ver: "3.5", dirName: "s1.5-3.5"},
				{prev: "0.5", ver: "10.2", dirName: "s0.5-10.2"},
			},
		},
		{
			name:       "subset",
			startVer:   "0.5",
			endVer:     "3.5",
			wantResult: []string{"v1.5", "v2.5", "v3.5"},
			wantSq: []squashVersion{
				{prev: "1.5", ver: "3.5", dirName: "s1.5-3.5"},
			},
		},
		{name: "already at last version", startVer: "10.2"},
	}
	subDirs := []string{"v0.5", "v1.5", "v2.5", "v3.5", "v10.2", "abc", "2.0", "3.0", "s0.5-10.2", "s1.5-3.5"}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			dirs := subDirs
			if len(tt.customDirs) > 0 {
				dirs = tt.customDirs
			}
			r, sq, err := filterDirectories(dirs, tt.startVer, tt.endVer)
			if tt.wantErr != "" {
				s.Empty(r)
				s.Empty(sq)
				s.EqualError(err, tt.wantErr)
			} else {
				s.NoError(err)
				s.Equal(tt.wantResult, r)
				s.ElementsMatch(tt.wantSq, sq)
			}
		})
	}
}

func (s *UpdateTaskTestSuite) TestSquashDirToVersion() {
	tests := []struct {
		source string
		prev   string
		ver    string
	}{
		{source: "s0.0-0.0", prev: "0.0", ver: "0.0"},
		{source: "s1.0-0.0", prev: "1.0", ver: "0.0"},
		{source: "s1.0-2", prev: "1.0", ver: "2"},
		{source: "s1-2.1", prev: "1", ver: "2.1"},
		{source: "s1-2", prev: "1", ver: "2"},
	}
	for _, tt := range tests {
		s.Run(tt.source, func() {
			prev, ver := squashDirToVersion(tt.source)
			s.Equal(tt.prev, prev)
			s.Equal(tt.ver, ver)
		})
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
