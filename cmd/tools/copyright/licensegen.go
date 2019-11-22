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

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type (
	// task that adds license header to source
	// files, if they don't already exist
	addLicenseHeaderTask struct {
		license string  // license header string to add
		config  *config // root directory of the project source
	}

	// command line config params
	config struct {
		rootDir    string
		verifyOnly bool
	}
)

// licenseFileName is the name of the license file
const licenseFileName = "LICENSE"

// unique prefix that identifies a license header
const licenseHeaderPrefixOld = "// Copyright (c)"
const licenseHeaderPrefix = "// The MIT License (MIT)"

var (
	// directories to be excluded
	dirBlacklist = []string{"vendor/"}
	// default perms for the newly created files
	defaultFilePerms = os.FileMode(0644)
)

// command line utility that adds license header
// to the source files. Usage as follows:
//
//  ./cmd/tools/copyright/licensegen.go
func main() {

	var cfg config
	flag.StringVar(&cfg.rootDir, "rootDir", ".", "project root directory")
	flag.BoolVar(&cfg.verifyOnly, "verifyOnly", false,
		"don't automatically add headers, just verify all files")
	flag.Parse()

	task := newAddLicenseHeaderTask(&cfg)
	if err := task.run(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func newAddLicenseHeaderTask(cfg *config) *addLicenseHeaderTask {
	return &addLicenseHeaderTask{
		config: cfg,
	}
}

func (task *addLicenseHeaderTask) run() error {
	data, err := ioutil.ReadFile(task.config.rootDir + "/" + licenseFileName)
	if err != nil {
		return fmt.Errorf("error reading license file, errr=%v", err.Error())
	}

	task.license, err = commentOutLines(string(data))
	if err != nil {
		return fmt.Errorf("copyright header failed to comment out lines, err=%v", err.Error())
	}

	err = filepath.Walk(task.config.rootDir, task.handleFile)
	if err != nil {
		return fmt.Errorf("copyright header check failed, err=%v", err.Error())
	}
	return nil
}

func (task *addLicenseHeaderTask) handleFile(path string, fileInfo os.FileInfo, err error) error {

	if err != nil {
		return err
	}

	if fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), "_vendor-") {
		return filepath.SkipDir
	}

	if fileInfo.IsDir() {
		return nil
	}

	if !mustProcessPath(path) {
		return nil
	}

	if !strings.HasSuffix(fileInfo.Name(), ".go") {
		return nil
	}

	// Used as part of the cli to write licence headers on files, does not use user supplied input so marked as nosec
	// #nosec
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(f)
	readLineSucc := scanner.Scan()
	if !readLineSucc {
		return fmt.Errorf("fail to read first line of file %v", path)
	}
	firstLine := strings.TrimSpace(scanner.Text())
	if err := scanner.Err(); err != nil {
		return err
	}
	f.Close()

	if strings.Contains(firstLine, licenseHeaderPrefixOld) || strings.Contains(firstLine, licenseHeaderPrefix) {
		return nil // file already has the copyright header
	}

	// at this point, src file is missing the header
	if task.config.verifyOnly {
		if !isFileAutogenerated(path) {
			return fmt.Errorf("%v missing license header", path)
		}
	}

	// Used as part of the cli to write licence headers on files, does not use user supplied input so marked as nosec
	// #nosec
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, []byte(task.license+string(data)), defaultFilePerms)
}

func isFileAutogenerated(path string) bool {
	return strings.HasPrefix(path, ".gen")
}

func mustProcessPath(path string) bool {
	for _, d := range dirBlacklist {
		if strings.HasPrefix(path, d) {
			return false
		}
	}
	return true
}

// returns true if the error type is an EOF
func isEOF(err error) bool {
	return err == io.EOF || err == io.ErrUnexpectedEOF
}

func commentOutLines(str string) (string, error) {
	var lines []string
	scanner := bufio.NewScanner(strings.NewReader(str))
	for scanner.Scan() {
		lines = append(lines, "// "+scanner.Text()+"\n")
	}
	lines = append(lines, "\n")

	if err := scanner.Err(); err != nil {
		return "", err
	}
	return strings.Join(lines, ""), nil
}
