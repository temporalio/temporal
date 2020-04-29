// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package util

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	testDirMode  = os.FileMode(0700)
	testFileMode = os.FileMode(0600)
)

type FileUtilSuite struct {
	*require.Assertions
	suite.Suite
}

func TestFileUtilSuite(t *testing.T) {
	suite.Run(t, new(FileUtilSuite))
}

func (s *FileUtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *FileUtilSuite) TestFileExists() {
	dir, err := ioutil.TempDir("", "TestFileExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)

	exists, err := FileExists(dir)
	s.Error(err)
	s.False(exists)

	filename := "test-file-name"
	exists, err = FileExists(filepath.Join(dir, filename))
	s.NoError(err)
	s.False(exists)

	s.createFile(dir, filename)
	exists, err = FileExists(filepath.Join(dir, filename))
	s.NoError(err)
	s.True(exists)
}

func (s *FileUtilSuite) TestDirectoryExists() {
	dir, err := ioutil.TempDir("", "TestDirectoryExists")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)

	subdir := "subdir"
	exists, err := DirectoryExists(filepath.Join(dir, subdir))
	s.NoError(err)
	s.False(exists)

	filename := "test-file-name"
	s.createFile(dir, filename)
	fpath := filepath.Join(dir, filename)
	exists, err = DirectoryExists(fpath)
	s.Error(err)
	s.False(exists)
}

func (s *FileUtilSuite) TestMkdirAll() {
	dir, err := ioutil.TempDir("", "TestMkdirAll")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)

	s.NoError(MkdirAll(dir, testDirMode))
	s.assertDirectoryExists(dir)

	subDirPath := filepath.Join(dir, "subdir_1", "subdir_2", "subdir_3")
	s.assertDirectoryNotExists(subDirPath)
	s.NoError(MkdirAll(subDirPath, testDirMode))
	s.assertDirectoryExists(subDirPath)
	s.assertCorrectFileMode(subDirPath)

	filename := "test-file-name"
	s.createFile(dir, filename)
	fpath := filepath.Join(dir, filename)
	s.Error(MkdirAll(fpath, testDirMode))
}

func (s *FileUtilSuite) TestWriteFile() {
	dir, err := ioutil.TempDir("", "TestWriteFile")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)

	filename := "test-file-name"
	fpath := filepath.Join(dir, filename)
	s.NoError(WriteFile(fpath, []byte("file body 1"), testFileMode))
	s.assertFileExists(fpath)
	s.assertCorrectFileMode(fpath)

	s.NoError(WriteFile(fpath, []byte("file body 2"), testFileMode))
	s.assertFileExists(fpath)
	s.assertCorrectFileMode(fpath)

	s.Error(WriteFile(dir, []byte(""), testFileMode))
	s.assertFileExists(fpath)
}

func (s *FileUtilSuite) TestReadFile() {
	dir, err := ioutil.TempDir("", "TestReadFile")
	s.NoError(err)
	defer os.RemoveAll(dir)
	s.assertDirectoryExists(dir)

	filename := "test-file-name"
	fpath := filepath.Join(dir, filename)
	data, err := ReadFile(fpath)
	s.Error(err)
	s.Empty(data)

	err = WriteFile(fpath, []byte("file contents"), testFileMode)
	s.NoError(err)
	data, err = ReadFile(fpath)
	s.NoError(err)
	s.Equal("file contents", string(data))
}

func (s *FileUtilSuite) TestListFilesByPrefix() {
	dir, err := ioutil.TempDir("", "TestListFiles")
	s.NoError(err)
	defer os.Remove(dir)
	s.assertDirectoryExists(dir)

	filename := "test-file-name"
	fpath := filepath.Join(dir, filename)
	files, err := ListFilesByPrefix(fpath, "test-")
	s.Error(err)
	s.Nil(files)

	subDirPath := filepath.Join(dir, "subdir")
	s.NoError(MkdirAll(subDirPath, testDirMode))
	s.assertDirectoryExists(subDirPath)
	expectedFileNames := []string{"file_1", "file_2", "file_3"}
	for _, f := range expectedFileNames {
		s.createFile(dir, f)
	}
	for _, f := range []string{"randomFile", "fileWithOtherPrefix"} {
		s.createFile(dir, f)
	}
	actualFileNames, err := ListFilesByPrefix(dir, "file_")
	s.NoError(err)
	s.Equal(len(expectedFileNames), len(actualFileNames))
}

func (s *FileUtilSuite) assertCorrectFileMode(path string) {
	info, err := os.Stat(path)
	s.NoError(err)
	mode := testFileMode
	if info.IsDir() {
		mode = testDirMode | os.ModeDir
	}
	s.Equal(mode, info.Mode())
}

func (s *FileUtilSuite) createFile(dir string, filename string) {
	err := ioutil.WriteFile(filepath.Join(dir, filename), []byte("file contents"), testFileMode)
	s.Nil(err)
}

func (s *FileUtilSuite) assertFileExists(filepath string) {
	exists, err := FileExists(filepath)
	s.NoError(err)
	s.True(exists)
}

func (s *FileUtilSuite) assertDirectoryExists(path string) {
	exists, err := DirectoryExists(path)
	s.NoError(err)
	s.True(exists)
}

func (s *FileUtilSuite) assertDirectoryNotExists(path string) {
	exists, err := DirectoryExists(path)
	s.NoError(err)
	s.False(exists)
}
