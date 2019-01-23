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
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/uber/cadence/common/blobstore/blob"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

const (
	dirMode  = os.FileMode(0700)
	fileMode = os.FileMode(0600)
)

var (
	errDirectoryExpected = errors.New("a path to a directory was expected")
	errFileExpected      = errors.New("a path to a file was expected")
)

func fileExists(filepath string) (bool, error) {
	if info, err := os.Stat(filepath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	} else if info.IsDir() {
		return false, errFileExpected
	}
	return true, nil
}

func directoryExists(path string) (bool, error) {
	if info, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	} else if !info.IsDir() {
		return false, errDirectoryExpected
	}
	return true, nil
}

func mkdirAll(path string) error {
	return os.MkdirAll(path, dirMode)
}

func writeFile(filepath string, data []byte) error {
	if err := os.Remove(filepath); err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.Create(filepath)
	defer f.Close()
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

func readFile(filepath string) ([]byte, error) {
	return ioutil.ReadFile(filepath)
}

func deleteFile(filepath string) (bool, error) {
	if err := os.Remove(filepath); err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

func listFiles(path string) ([]string, error) {
	if info, err := os.Stat(path); err != nil {
		return nil, err
	} else if !info.IsDir() {
		return nil, errDirectoryExpected
	}

	children, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, c := range children {
		if c.IsDir() {
			continue
		}
		files = append(files, c.Name())
	}
	return files, nil
}

func serializeBucketConfig(bucketCfg *BucketConfig) ([]byte, error) {
	return yaml.Marshal(bucketCfg)
}

func deserializeBucketConfig(data []byte) (*BucketConfig, error) {
	bucketCfg := &BucketConfig{}
	if err := yaml.Unmarshal(data, bucketCfg); err != nil {
		return nil, err
	}
	return bucketCfg, nil
}

func serializeBlob(blob *blob.Blob) ([]byte, error) {
	buf := bytes.Buffer{}
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(blob); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func deserializeBlob(data []byte) (*blob.Blob, error) {
	blob := &blob.Blob{}
	decoder := gob.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(blob); err != nil {
		return nil, err
	}
	return blob, nil
}
