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

package filestore

import (
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/common/blobstore/blob"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

type UtilSuite struct {
	*require.Assertions
	suite.Suite
}

func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(UtilSuite))
}

func (s *UtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *UtilSuite) TestFileExists() {
	dir, err := ioutil.TempDir("", "TestFileExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)

	exists, err := fileExists(dir)
	s.Error(err)
	s.False(exists)

	filename := "test-file-name"
	exists, err = fileExists(filepath.Join(dir, filename))
	s.NoError(err)
	s.False(exists)

	s.createFile(dir, filename)
	exists, err = fileExists(filepath.Join(dir, filename))
	s.NoError(err)
	s.True(exists)
}

func (s *UtilSuite) TestDirectoryExists() {
	dir, err := ioutil.TempDir("", "TestDirectoryExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)

	subdir := "subdir"
	exists, err := directoryExists(filepath.Join(dir, subdir))
	s.NoError(err)
	s.False(exists)

	filename := "test-file-name"
	s.createFile(dir, filename)
	fpath := filepath.Join(dir, filename)
	exists, err = directoryExists(fpath)
	s.Error(err)
	s.False(exists)
}

func (s *UtilSuite) TestMkdirAll() {
	dir, err := ioutil.TempDir("", "TestMkdirAll")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)

	s.NoError(mkdirAll(dir))
	s.assertDirectoryExists(dir)

	subDirPath := filepath.Join(dir, "subdir_1", "subdir_2", "subdir_3")
	s.assertDirectoryNotExists(subDirPath)
	s.NoError(mkdirAll(subDirPath))
	s.assertDirectoryExists(subDirPath)
	s.assertCorrectFileMode(subDirPath)

	filename := "test-file-name"
	s.createFile(dir, filename)
	fpath := filepath.Join(dir, filename)
	s.Error(mkdirAll(fpath))
}

func (s *UtilSuite) TestWriteFile() {
	dir, err := ioutil.TempDir("", "TestWriteFile")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)

	filename := "test-file-name"
	fpath := filepath.Join(dir, filename)
	s.NoError(writeFile(fpath, []byte("file body 1")))
	s.assertFileExists(fpath)
	s.assertCorrectFileMode(fpath)

	s.NoError(writeFile(fpath, []byte("file body 2")))
	s.assertFileExists(fpath)
	s.assertCorrectFileMode(fpath)

	s.Error(writeFile(dir, []byte("")))
	s.assertFileExists(fpath)
}

func (s *UtilSuite) TestReadFile() {
	dir, err := ioutil.TempDir("", "TestReadFile")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)

	filename := "test-file-name"
	fpath := filepath.Join(dir, filename)
	data, err := readFile(fpath)
	s.Error(err)
	s.Empty(data)

	err = writeFile(fpath, []byte("file contents"))
	s.NoError(err)
	data, err = readFile(fpath)
	s.NoError(err)
	s.Equal("file contents", string(data))
}

func (s *UtilSuite) TestDeleteFile() {
	dir, err := ioutil.TempDir("", "TestDeleteFile")
	s.NoError(err)
	defer os.Remove(dir)
	s.assertDirectoryExists(dir)

	filename := "test-file-name"
	fpath := filepath.Join(dir, filename)
	deleted, err := deleteFile(fpath)
	s.NoError(err)
	s.False(deleted)

	err = writeFile(fpath, []byte("file contents"))
	s.NoError(err)
	deleted, err = deleteFile(fpath)
	s.NoError(err)
	s.True(deleted)
	exists, err := fileExists(fpath)
	s.NoError(err)
	s.False(exists)
}

func (s *UtilSuite) TestListFiles() {
	dir, err := ioutil.TempDir("", "TestListFiles")
	s.NoError(err)
	defer os.Remove(dir)
	s.assertDirectoryExists(dir)

	filename := "test-file-name"
	fpath := filepath.Join(dir, filename)
	files, err := listFiles(fpath)
	s.Error(err)
	s.Nil(files)

	subDirPath := filepath.Join(dir, "subdir")
	s.NoError(mkdirAll(subDirPath))
	s.assertDirectoryExists(subDirPath)
	expectedFileNames := []string{"file_1", "file_2", "file_3"}
	for _, f := range expectedFileNames {
		s.createFile(dir, f)
	}
	actualFileNames, err := listFiles(dir)
	s.NoError(err)
	s.Equal(expectedFileNames, actualFileNames)
}

func (s *UtilSuite) TestSerializationBucketConfig() {
	inCfg := &BucketConfig{
		Name:          "test-custom-bucket-name",
		Owner:         "test-custom-bucket-owner",
		RetentionDays: 10,
	}
	bytes, err := serializeBucketConfig(inCfg)
	s.NoError(err)

	outCfg, err := deserializeBucketConfig(bytes)
	s.NoError(err)
	s.Equal(inCfg, outCfg)
}

func (s *UtilSuite) TestSerializationBlob() {
	inBlob := blob.NewBlob([]byte("file contents"), map[string]string{"key1": "value1", "key2": "value2"})
	data, err := serializeBlob(inBlob)
	s.NoError(err)

	outBlob, err := deserializeBlob(data)
	s.NoError(err)
	s.Equal(inBlob.Tags, outBlob.Tags)
	s.Equal("file contents", string(outBlob.Body))
}

func (s *UtilSuite) createFile(dir string, filename string) {
	err := ioutil.WriteFile(filepath.Join(dir, filename), []byte("file contents"), fileMode)
	s.Nil(err)
}

func (s *UtilSuite) assertFileExists(filepath string) {
	exists, err := fileExists(filepath)
	s.NoError(err)
	s.True(exists)
}

func (s *UtilSuite) assertDirectoryExists(path string) {
	exists, err := directoryExists(path)
	s.NoError(err)
	s.True(exists)
}

func (s *UtilSuite) assertDirectoryNotExists(path string) {
	exists, err := directoryExists(path)
	s.NoError(err)
	s.False(exists)
}

func (s *UtilSuite) assertCorrectFileMode(path string) {
	info, err := os.Stat(path)
	s.NoError(err)
	mode := fileMode
	if info.IsDir() {
		mode = dirMode | os.ModeDir
	}
	s.Equal(mode, info.Mode())
}
