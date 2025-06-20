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
