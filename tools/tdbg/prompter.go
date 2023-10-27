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
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type (
	// Prompter is a helper for prompting the user for confirmation.
	Prompter struct {
		writer     io.Writer
		reader     io.Reader
		exiter     func(code int)
		flagLookup BoolFlagLookup
	}
	// PrompterParams is used to configure a new Prompter.
	PrompterParams struct {
		// Reader defaults to os.Stdin.
		Reader io.Reader
		// Writer defaults to os.Stdout.
		Writer io.Writer
		// Exiter defaults to [os.Exit].
		Exiter func(code int)
	}
	// PrompterOption is used to override default PrompterParams.
	PrompterOption func(*PrompterParams)
	// BoolFlagLookup can be satisfied by [github.com/urfave/cli/v2.Context].
	BoolFlagLookup interface {
		Bool(name string) bool
	}
	PrompterFactory func(c BoolFlagLookup) *Prompter
)

func NewPrompterFactory(opts ...PrompterOption) PrompterFactory {
	return func(c BoolFlagLookup) *Prompter {
		return NewPrompter(c, opts...)
	}
}

// NewPrompter creates a new Prompter. In most cases, the first argument should be [github.com/urfave/cli/v2.Context].
func NewPrompter(c BoolFlagLookup, opts ...PrompterOption) *Prompter {
	params := PrompterParams{
		Writer: os.Stdout,
		Reader: os.Stdin,
		Exiter: os.Exit,
	}
	for _, opt := range opts {
		opt(&params)
	}
	return &Prompter{
		writer:     params.Writer,
		reader:     params.Reader,
		exiter:     params.Exiter,
		flagLookup: c,
	}
}

// Prompt the user for confirmation. If the user does not respond with "y" or "yes" (case-insensitive and without
// leading or trailing space), the process will exit with code 1.
func (p *Prompter) Prompt(msg string) {
	if p.flagLookup.Bool(FlagYes) {
		return
	}
	_, err := p.writer.Write([]byte(msg + " [y/N]: "))
	if err != nil {
		panic(fmt.Errorf("failed to write prompt: %w", err))
	}
	reader := bufio.NewReader(p.reader)
	text, err := reader.ReadString('\n')
	if err != nil {
		panic(fmt.Errorf("failed to read prompt: %w", err))
	}
	textLower := strings.ToLower(strings.TrimSpace(text))
	if textLower != "y" && textLower != "yes" {
		p.exiter(1)
	}
}
