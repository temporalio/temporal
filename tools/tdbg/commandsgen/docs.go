package commandsgen

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

func GenerateDocsFiles(commands Commands) (map[string][]byte, error) {
	optionSetMap := make(map[string]OptionSets)
	for i, optionSet := range commands.OptionSets {
		optionSetMap[optionSet.Name] = commands.OptionSets[i]
	}

	w := &docWriter{
		fileMap:      make(map[string]*bytes.Buffer),
		optionSetMap: optionSetMap,
		allCommands:  commands.CommandList,
	}

	// sorted ascending by full name of command (activity complete, batch list, etc)
	for _, cmd := range commands.CommandList {
		if err := cmd.writeDoc(w); err != nil {
			return nil, fmt.Errorf("failed writing docs for command %s: %w", cmd.FullName, err)
		}
	}

	// Format and return
	var finalMap = make(map[string][]byte)
	for key, buf := range w.fileMap {
		finalMap[key] = buf.Bytes()
	}
	return finalMap, nil
}

type docWriter struct {
	allCommands  []Command
	fileMap      map[string]*bytes.Buffer
	optionSetMap map[string]OptionSets
	optionsStack [][]Option
}

func (c *Command) writeDoc(w *docWriter) error {
	if err := w.processOptions(c); err != nil {
		return fmt.Errorf("processing options: %w", err)
	}

	// If this is a root command, write a new file
	depth := c.depth()
	if depth == 1 {
		w.writeCommand(c)
	} else if depth > 1 {
		w.writeSubcommand(c)
	}
	return nil
}

func (w *docWriter) writeCommand(c *Command) {
	fileName := c.fileName()
	w.fileMap[fileName] = &bytes.Buffer{}
	w.fileMap[fileName].WriteString("{/* NOTE: This is an auto-generated file. Any edit to this file will be overwritten.\n")
	w.fileMap[fileName].WriteString("This file is generated from https://github.com/temporalio/cli/blob/main/temporalcli/commandsgen/commands.yml */}\n")
	w.fileMap[fileName].WriteString("---\n")
	w.fileMap[fileName].WriteString("id: " + fileName + "\n")
	w.fileMap[fileName].WriteString("title: Temporal CLI " + fileName + " command reference\n")
	w.fileMap[fileName].WriteString("sidebar_label: " + fileName + "\n")
	w.fileMap[fileName].WriteString("description: " + c.Docs.DescriptionHeader + "\n")
	w.fileMap[fileName].WriteString("toc_max_heading_level: 4\n")

	w.fileMap[fileName].WriteString("keywords:\n")
	for _, keyword := range c.Docs.Keywords {
		w.fileMap[fileName].WriteString("  - " + keyword + "\n")
	}
	w.fileMap[fileName].WriteString("tags:\n")
	for _, tag := range c.Docs.Tags {
		w.fileMap[fileName].WriteString("  - " + tag + "\n")
	}
	w.fileMap[fileName].WriteString("---\n\n")
}

func (w *docWriter) writeSubcommand(c *Command) {
	fileName := c.fileName()
	prefix := strings.Repeat("#", c.depth())
	w.fileMap[fileName].WriteString(prefix + " " + c.leafName() + "\n\n")
	w.fileMap[fileName].WriteString(c.Description + "\n\n")

	if w.isLeafCommand(c) {
		w.fileMap[fileName].WriteString("Use the following options to change the behavior of this command.\n\n")

		// gather options from command and all options aviailable from parent commands
		var options = make([]Option, 0)
		var globalOptions = make([]Option, 0)
		for i, o := range w.optionsStack {
			if i == len(w.optionsStack)-1 {
				options = append(options, o...)
			} else {
				globalOptions = append(globalOptions, o...)
			}
		}

		// alphabetize options
		sort.Slice(options, func(i, j int) bool {
			return options[i].Name < options[j].Name
		})

		sort.Slice(globalOptions, func(i, j int) bool {
			return globalOptions[i].Name < globalOptions[j].Name
		})

		w.writeOptions("Flags", options, c)
		w.writeOptions("Global Flags", globalOptions, c)

	}
}

func (w *docWriter) writeOptions(prefix string, options []Option, c *Command) {
	if len(options) == 0 {
		return
	}

	fileName := c.fileName()

	fmt.Fprintf(w.fileMap[fileName], "**%s:**\n\n", prefix)

	for _, o := range options {
		// option name and alias
		fmt.Fprintf(w.fileMap[fileName], "**--%s**", o.Name)
		if len(o.Short) > 0 {
			fmt.Fprintf(w.fileMap[fileName], ", **-%s**", o.Short)
		}
		fmt.Fprintf(w.fileMap[fileName], " _%s_\n\n", o.Type)

		// description
		w.fileMap[fileName].WriteString(encodeJSONExample(o.Description))
		if o.Required {
			w.fileMap[fileName].WriteString(" Required.")
		}
		if len(o.EnumValues) > 0 {
			fmt.Fprintf(w.fileMap[fileName], " Accepted values: %s.", strings.Join(o.EnumValues, ", "))
		}
		if len(o.Default) > 0 {
			fmt.Fprintf(w.fileMap[fileName], ` (default "%s")`, o.Default)
		}
		w.fileMap[fileName].WriteString("\n\n")

		if o.Experimental {
			w.fileMap[fileName].WriteString(":::note" + "\n\n")
			w.fileMap[fileName].WriteString("Option is experimental." + "\n\n")
			w.fileMap[fileName].WriteString(":::" + "\n\n")
		}
	}
}

func (w *docWriter) processOptions(c *Command) error {
	// Pop options from stack if we are moving up a level
	if len(w.optionsStack) >= len(strings.Split(c.FullName, " ")) {
		w.optionsStack = w.optionsStack[:len(w.optionsStack)-1]
	}
	var options []Option
	options = append(options, c.Options...)

	// Maintain stack of options available from parent commands
	for _, set := range c.OptionSets {
		optionSet, ok := w.optionSetMap[set]
		if !ok {
			return fmt.Errorf("invalid option set %v used", set)
		}
		optionSetOptions := optionSet.Options
		options = append(options, optionSetOptions...)
	}

	w.optionsStack = append(w.optionsStack, options)
	return nil
}

func (w *docWriter) isLeafCommand(c *Command) bool {
	for _, maybeSubCmd := range w.allCommands {
		if maybeSubCmd.isSubCommand(c) {
			return false
		}
	}
	return true
}

func encodeJSONExample(v string) string {
	// example: 'YourKey={"your": "value"}'
	// results in an mdx acorn rendering error
	// and wrapping in backticks lets it render
	re := regexp.MustCompile(`('[a-zA-Z0-9]*={.*}')`)
	v = re.ReplaceAllString(v, "`$1`")
	return v
}
