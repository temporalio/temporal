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
	"errors"
	"io/ioutil"
	"os"
	"strings"
)

var (
	// ErrDirectoryExpected indicates that a directory was expected
	ErrDirectoryExpected = errors.New("a path to a directory was expected")
	// ErrFileExpected indicates that a file was expected
	ErrFileExpected = errors.New("a path to a file was expected")
)

// FileExists returns true if file exists, false otherwise.
// Returns error if could not verify if file exists or if path specifies directory instead of file.
func FileExists(filepath string) (bool, error) {
	if info, err := os.Stat(filepath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	} else if info.IsDir() {
		return false, ErrFileExpected
	}
	return true, nil
}

// DirectoryExists returns true if directory exists, false otherwise.
// Returns error if could not verify if directory exists or if path does not point at directory.
func DirectoryExists(path string) (bool, error) {
	if info, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	} else if !info.IsDir() {
		return false, ErrDirectoryExpected
	}
	return true, nil
}

// MkdirAll creates all subdirectories in given path.
func MkdirAll(path string, dirMode os.FileMode) error {
	return os.MkdirAll(path, dirMode)
}

// WriteFile is used to write a file. If file already exists it is overwritten.
// If file does not already exist it is created.
func WriteFile(filepath string, data []byte, fileMode os.FileMode) (retErr error) {
	if err := os.Remove(filepath); err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.Create(filepath)
	defer func() {
		err := f.Close()
		if err != nil {
			retErr = err
		}
	}()
	if err != nil {
		return err
	}
	if err = f.Chmod(fileMode); err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return err
	}
	return nil
}

// ReadFile reads the contents of a file specified by filepath
// WARNING: callers of this method should be extremely careful not to use it in a context where filepath is supplied by the user.
func ReadFile(filepath string) ([]byte, error) {
	// #nosec
	return ioutil.ReadFile(filepath)
}

// ListFiles lists all files in a directory.
func ListFiles(dirPath string) ([]string, error) {
	if info, err := os.Stat(dirPath); err != nil {
		return nil, err
	} else if !info.IsDir() {
		return nil, ErrDirectoryExpected
	}

	f, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	fileNames, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	return fileNames, nil
}

// ListFilesByPrefix lists all files in directory with prefix.
func ListFilesByPrefix(dirPath string, prefix string) ([]string, error) {
	fileNames, err := ListFiles(dirPath)
	if err != nil {
		return nil, err
	}

	var filteredFileNames []string
	for _, name := range fileNames {
		if strings.HasPrefix(name, prefix) {
			filteredFileNames = append(filteredFileNames, name)
		}
	}
	return filteredFileNames, nil
}
