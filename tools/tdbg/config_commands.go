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

package tdbg

import (
	"bytes"
	"fmt"
	"os"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/kr/pretty"
	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/config"
	"gopkg.in/yaml.v3"
)

func RenderConfig(c *cli.Context) error {
	f := c.String(FlagConfigTemplate)
	cfg := config.Config{}

	data, err := os.ReadFile(f)
	if err != nil {
		return err
	}

	templateFuncs := sprig.FuncMap()

	tpl, err := template.New("config").Funcs(templateFuncs).Parse(string(data))
	if err != nil {
		return fmt.Errorf("template parsing error: %w", err)
	}

	var rendered bytes.Buffer
	err = tpl.Execute(&rendered, nil)
	if err != nil {
		return fmt.Errorf("template execution error: %w", err)
	}

	err = yaml.Unmarshal(rendered.Bytes(), &cfg)
	if err != nil {
		return err
	}

	fmt.Printf("Rendered config file:\n\n%s\n\n", rendered.String())
	fmt.Printf("Config:\n\n%# v\n", pretty.Formatter(cfg))

	return nil
}
