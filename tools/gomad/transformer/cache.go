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

package transformer

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"

	"golang.org/x/tools/go/packages"
)

func CreateCacheWriter(outDir string) func(*packages.Package, func()) {
	if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
		panic(err)
	}

	return func(pkg *packages.Package, transformFn func()) {
		// generate hash
		hash := hashDirectory(pkg.CompiledGoFiles)

		// path to hash file
		hashFilePath := filepath.Join(outDir, simPkgPrefix, pkg.PkgPath, ".hash")
		if err := os.MkdirAll(filepath.Dir(hashFilePath), os.ModePerm); err != nil {
			panic(err)
		}

		// load current hash file
		currentHash, err := os.ReadFile(hashFilePath)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Panic(err)
			}
		}

		// compare hashes
		if string(currentHash) == hash {
			return
		}

		// wrong/missing hash - let's transform
		transformFn()

		// write new hash file
		err = os.WriteFile(hashFilePath, []byte(hash), 0644)
		if err != nil {
			log.Panic(err)
		}
	}
}

func hashDirectory(files []string) string {
	hasher := sha256.New()
	for _, f := range files {
		content, err := os.ReadFile(f)
		if err != nil {
			log.Panic(err)
		}
		hasher.Write(content)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
