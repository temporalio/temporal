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

// TODO: use hash of `transformer` package for cache
//func transformerHash() {
//	files, err := os.ReadDir(".")
//	if err != nil {
//		panic(err)
//	}
//
//	var toHash []string
//	for _, file := range files {
//		path := file.Name()
//		if filepath.Ext(path) != ".go" {
//			continue
//		}
//		if strings.Contains(filepath.Base(path), "_test") {
//			continue
//		}
//		if filepath.Base(path) == "hash.go" {
//			continue
//		}
//		toHash = append(toHash, path)
//	}
//
//	h := sha256.New()
//	for _, file := range toHash {
//		fmt.Println(file)
//		content, err2 := os.ReadFile(file)
//		if err2 != nil {
//			panic(err2)
//		}
//		h.Write(content)
//	}
//	hash := hex.EncodeToString(h.Sum(nil))
//	output := fmt.Sprintf("package transformer\n\nvar transformerHash = []byte(\"%s\")\n", hash)
//	err = os.WriteFile("hash.go", []byte(output), 0644)
//	if err != nil {
//		panic(err)
//	}
//}
