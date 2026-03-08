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

package lib

import (
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/afero"

	SIM "go.temporal.io/server/tools/gomad/runtime"
)

var (
	vfsStateKey = "vfsState"
)

type File struct{ afero.File }

func Open(name string) (*File, error) {
	f, err := getOrCreateVFS().Open(name)
	if err != nil && os.IsNotExist(err) {
		// Fall back to real filesystem for read-only files not present in VFS
		// (e.g. schema SQL files loaded during test cluster initialization).
		realF, realErr := os.Open(name)
		if realErr == nil {
			return &File{realF}, nil
		}
	}
	return wrapFile(f, err)
}

func OpenFile(name string, flag int, perm os.FileMode) (*File, error) {
	f, err := getOrCreateVFS().OpenFile(name, flag, perm)
	return wrapFile(f, err)
}

func Executable() (string, error) {
	return "/executable", nil
}

func CreateTemp(dir, pattern string) (*File, error) {
	f, err := afero.TempFile(getOrCreateVFS(), dir, pattern)
	return wrapFile(f, err)
}

func Hostname() (name string, err error) {
	return "GOMAD_HOST", nil
}

func ReadFile(name string) ([]byte, error) {
	return afero.ReadFile(getOrCreateVFS(), name)
}

func Remove(name string) error {
	return getOrCreateVFS().Remove(name)
}

func RemoveAll(path string) error {
	return getOrCreateVFS().RemoveAll(path)
}

func MkdirTemp(dir, pattern string) (string, error) {
	return afero.TempDir(getOrCreateVFS(), dir, pattern)
}

func TempDir() string {
	return "/tmp"
}

func Environ() []string {
	return []string{}
}

func Getwd() (dir string, err error) {
	return "/cwd", nil
}

func Readlink(name string) (string, error) {
	panic("not implemented: os.Readlink()")
}

func Chmod(name string, mode os.FileMode) error {
	return getOrCreateVFS().Chmod(name, mode)
}

func Rename(oldpath, newpath string) error {
	return getOrCreateVFS().Rename(oldpath, newpath)
}

func Getenv(key string) string {
	return os.Getenv(key)
}

func MkdirAll(path string, perm os.FileMode) error {
	return getOrCreateVFS().MkdirAll(path, perm)
}

func Create(name string) (*File, error) {
	f, err := getOrCreateVFS().Create(name)
	return wrapFile(f, err)
}

func UserHomeDir() (string, error) {
	return "/home", nil
}

func Setenv(key, value string) error {
	return nil
}

func ExpandEnv(s string) string {
	return s // TODO
}

func ReadDir(name string) ([]os.DirEntry, error) {
	infos, err := afero.ReadDir(getOrCreateVFS(), name)
	if err != nil {
		return nil, err
	}

	dirs := make([]os.DirEntry, len(infos))
	for i, info := range infos {
		dirs[i] = fs.FileInfoToDirEntry(info)
	}
	return dirs, nil
}

func WriteFile(name string, data []byte, perm os.FileMode) error {
	return afero.WriteFile(getOrCreateVFS(), name, data, perm)
}

func LookupEnv(key string) (string, bool) {
	return "", false
}

func Getpid() int {
	return 0
}

func Getuid() int {
	return -1
}

func Lstat(name string) (os.FileInfo, error) {
	return getOrCreateVFS().Stat(name)
}

func Stat(name string) (os.FileInfo, error) {
	return getOrCreateVFS().Stat(name)
}

func (f *File) Fd() uintptr {
	// TODO: track file pointers?
	return 0
}

func (f *File) Chmod(mode os.FileMode) error {
	return getOrCreateVFS().Chmod(f.Name(), mode)
}

func LoadFileFromDisk(srcDir, dstDir, filename string) {
	// read from real file system
	srcFilepath := filepath.Join(srcDir, filename)
	srcFileData, err := os.ReadFile(srcFilepath)
	if err != nil {
		panic(err)
	}

	// write to in-memory file
	dstFilepath := filepath.Join(dstDir, filename)
	err = WriteFile(dstFilepath, srcFileData, 0644)
	if err != nil {
		panic(err)
	}
}

// ==== UTIL

func getOrCreateVFS() afero.Fs {
	sim := SIM.TryAnySimulator()
	if _, exists := sim.State[vfsStateKey]; !exists {
		sim.State[vfsStateKey] = afero.NewMemMapFs()
	}
	return sim.State[vfsStateKey].(afero.Fs)
}

func wrapFile(f afero.File, err error) (*File, error) {
	if err != nil {
		return nil, err
	}
	return &File{f}, nil
}
