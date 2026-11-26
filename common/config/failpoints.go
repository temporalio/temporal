package config

import (
	"fmt"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// UnmarshalYAML reads the switch and keeps the failpoints.buggify block, without decoding it:
// the config files are layered, so the final value of Enabled is only known after the last file.
// ReadBuggify decodes the blocks.
func (f *Failpoints) UnmarshalYAML(node *yaml.Node) error {
	var raw struct {
		Enabled *bool     `yaml:"enabled"`
		Buggify yaml.Node `yaml:"buggify"`
	}
	if err := node.Decode(&raw); err != nil {
		return err
	}
	if raw.Enabled != nil {
		f.Enabled = *raw.Enabled
	}
	if raw.Buggify.Kind != 0 {
		f.buggifyBlocks = append(f.buggifyBlocks, raw.Buggify)
	}
	return nil
}

// ReadBuggify returns the failpoints.buggify block: Buggify with the blocks of the YAML files
// applied on top, in order of loading. It returns an error if a block is malformed. Call it only
// if failpoints are enabled: a server without them never reads, and so never rejects, the block.
func (f Failpoints) ReadBuggify() (FailpointsBuggify, error) {
	block := f.Buggify
	for i := range f.buggifyBlocks {
		if err := f.buggifyBlocks[i].Decode(&block); err != nil {
			return FailpointsBuggify{}, fmt.Errorf("invalid failpoints.buggify: %w", err)
		}
	}
	return block, nil
}

// UnmarshalYAML decodes the block like a struct, except that the seed must be an unsigned integer:
// the YAML decoder rounds a fractional number such as 1.5 into a uint64 field, which would start
// BUGGIFY with a different seed than the one that is written. A field that the block doesn't have
// keeps its value, and keys, if the block has it, replaces the list: it must be a list, and an
// empty list `keys: []` clears the list, while `keys: null` (or `keys:` without a value) is an
// error rather than keeping the list of an earlier file. An unknown field is an error,
// so a field that BUGGIFY no longer reads, such as a top-level activationProbability, is not
// silently ignored.
func (b *FailpointsBuggify) UnmarshalYAML(node *yaml.Node) error {
	if err := rejectUnknownFields(node, "seed", "keys", "trace"); err != nil {
		return err
	}
	var raw struct {
		Seed  yaml.Node `yaml:"seed"`
		Keys  yaml.Node `yaml:"keys"`
		Trace *bool     `yaml:"trace"`
	}
	if err := node.Decode(&raw); err != nil {
		return err
	}
	if raw.Seed.Kind != 0 && raw.Seed.ShortTag() != "!!null" {
		if raw.Seed.ShortTag() != "!!int" {
			return fmt.Errorf("line %d: seed %q: want an unsigned integer", raw.Seed.Line, raw.Seed.Value)
		}
		var seed uint64
		if err := raw.Seed.Decode(&seed); err != nil {
			return fmt.Errorf("line %d: seed %q: want an unsigned integer", raw.Seed.Line, raw.Seed.Value)
		}
		b.Seed = seed
	}
	if raw.Keys.Kind != 0 {
		if raw.Keys.Kind != yaml.SequenceNode {
			return fmt.Errorf("line %d: keys: want a list", raw.Keys.Line)
		}
		keys := make([]FailpointsBuggifyKey, len(raw.Keys.Content))
		for i, entry := range raw.Keys.Content {
			if err := entry.Decode(&keys[i]); err != nil {
				return err
			}
		}
		b.Keys = keys
	}
	if raw.Trace != nil {
		b.Trace = *raw.Trace
	}
	return nil
}

// UnmarshalYAML decodes an entry of the allowlist, and rejects a field that it doesn't have. The
// check is here, not where the list is decoded, because the decoder gives this method the mapping
// that an alias such as `- *entry` refers to, while the list only sees the alias node.
func (k *FailpointsBuggifyKey) UnmarshalYAML(node *yaml.Node) error {
	if err := rejectUnknownFields(node, "match", "activationProbability", "fireProbability", "error"); err != nil {
		return err
	}
	// plain has the fields of the entry without this method, so decoding doesn't recurse.
	type plain FailpointsBuggifyKey
	return node.Decode((*plain)(k))
}

// rejectUnknownFields returns an error if the mapping node has a field other than the allowed,
// also one that a merge key (`<<: *base`) adds. It returns an error, too, if a mapping merges
// itself, directly or through other mappings, because following the merge would never end.
func rejectUnknownFields(node *yaml.Node, allowed ...string) error {
	return rejectUnknownFieldsOf(node, allowed, nil)
}

// rejectUnknownFieldsOf is rejectUnknownFields with the mappings of the merge keys that are being
// followed, to find a cycle.
func rejectUnknownFieldsOf(node *yaml.Node, allowed []string, active []*yaml.Node) error {
	if node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	if node.Kind != yaml.MappingNode {
		return nil
	}
	if slices.Contains(active, node) {
		return fmt.Errorf("line %d: a mapping merges itself", node.Line)
	}
	active = append(active, node)
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Tag == "!!merge" {
			merged := node.Content[i+1]
			if merged.Kind == yaml.SequenceNode {
				for _, m := range merged.Content {
					if err := rejectUnknownFieldsOf(m, allowed, active); err != nil {
						return err
					}
				}
			} else if err := rejectUnknownFieldsOf(merged, allowed, active); err != nil {
				return err
			}
			continue
		}
		if !slices.Contains(allowed, node.Content[i].Value) {
			return fmt.Errorf("line %d: unknown field %q: want one of %s",
				node.Content[i].Line, node.Content[i].Value, strings.Join(allowed, ", "))
		}
	}
	return nil
}
