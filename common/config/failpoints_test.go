package config

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// The failpoints block of the static config has the switch and the BUGGIFY settings, and off by
// default.
func TestFailpoints_Unmarshal(t *testing.T) {
	t.Parallel()
	activation, fire, quarter := 0.5, 0.0, 0.25

	for name, tc := range map[string]struct {
		yaml string
		want Failpoints
	}{
		"absent": {yaml: "log: {level: warn}\n"},
		"switch": {yaml: "failpoints:\n  enabled: true\n", want: Failpoints{Enabled: true}},
		"buggify": {
			yaml: `failpoints:
  enabled: true
  buggify:
    seed: 42
    keys:
      - match: "grpc:*"
        activationProbability: 0.5
        fireProbability: 0
        error: Unavailable
      - match: "matching.skipSyncMatch"
        activationProbability: 0.25
        fireProbability: 0.25
    trace: true
`,
			want: Failpoints{Enabled: true, Buggify: FailpointsBuggify{
				Seed: 42,
				Keys: []FailpointsBuggifyKey{
					{Match: "grpc:*", ActivationProbability: &activation, FireProbability: &fire, Error: "Unavailable"},
					{Match: "matching.skipSyncMatch", ActivationProbability: &quarter, FireProbability: &quarter},
				},
				Trace: true,
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var cfg Config
			require.NoError(t, yaml.Unmarshal([]byte(tc.yaml), &cfg))
			require.Equal(t, tc.want.Enabled, cfg.Failpoints.Enabled)
			block, err := cfg.Failpoints.ReadBuggify()
			require.NoError(t, err)
			require.Equal(t, tc.want.Buggify, block)
		})
	}
}

// aliasedEntry is a config file that defines an entry of the allowlist with a field that BUGGIFY
// doesn't read, to be referred to by an alias in the keys that the file appends.
const aliasedEntry = "x-entry: &entry\n  match: a\n  activationProbability: 1\n  fireProbability: 1\n  prefix: b\nfailpoints:\n  enabled: true\n  buggify:\n"

// The failpoints.buggify block is not decoded while the config is loaded, so a server without
// failpoints never rejects it, and the seed is not rounded.
func TestFailpoints_BuggifyBlockIsReadAfterLoading(t *testing.T) {
	t.Parallel()

	t.Run("a block of a disabled switch is not decoded", func(t *testing.T) {
		t.Parallel()
		for _, file := range []string{
			"failpoints:\n  buggify: false\n",
			"failpoints:\n  enabled: false\n  buggify:\n    seed: 1.5\n",
		} {
			var cfg Config
			require.NoError(t, yaml.Unmarshal([]byte(file), &cfg), file)
			require.False(t, cfg.Failpoints.Enabled)
		}
	})
	t.Run("a malformed block of an enabled switch is rejected when it is read", func(t *testing.T) {
		t.Parallel()
		for file, want := range map[string]string{
			"failpoints:\n  enabled: true\n  buggify: false\n":                                                    "invalid failpoints.buggify",
			"failpoints:\n  enabled: true\n  buggify:\n    seed: 1.5\n":                                           `seed "1.5": want an unsigned integer`,
			"failpoints:\n  enabled: true\n  buggify:\n    seed: -1\n":                                            `seed "-1": want an unsigned integer`,
			"failpoints:\n  enabled: true\n  buggify:\n    seed: \"7\"\n":                                         `seed "7": want an unsigned integer`,
			"failpoints:\n  enabled: true\n  buggify:\n    keys:\n      - match: a\n        fireProbability: x\n": "invalid failpoints.buggify",
			"failpoints:\n  enabled: true\n  buggify:\n    keys: a\n":                                             "keys: want a list",
			"failpoints:\n  enabled: true\n  buggify:\n    keys: null\n":                                          "keys: want a list",
			"failpoints:\n  enabled: true\n  buggify:\n    keys:\n":                                               "keys: want a list",
			"failpoints:\n  enabled: true\n  buggify:\n    keys: ~\n":                                             "keys: want a list",
			// Fields that BUGGIFY no longer reads are errors, not ignored.
			"failpoints:\n  enabled: true\n  buggify:\n    activationProbability: 0.5\n":                 `unknown field "activationProbability"`,
			"failpoints:\n  enabled: true\n  buggify:\n    include: [a]\n":                               `unknown field "include"`,
			"failpoints:\n  enabled: true\n  buggify:\n    keys:\n      - match: a\n        prefix: b\n": `unknown field "prefix"`,
			// An alias doesn't hide the fields of the entry that it refers to, nor does a merge key.
			aliasedEntry + "    keys:\n      - *entry\n":                                                                   `unknown field "prefix"`,
			aliasedEntry + "    keys:\n      - <<: *entry\n        match: b\n":                                             `unknown field "prefix"`,
			aliasedEntry + "    keys:\n      - {match: a, activationProbability: 1, fireProbability: 1}\n      - *entry\n": `unknown field "prefix"`,
			// The block itself can be an alias too.
			"x-block: &block\n  seed: 1\n  activationProbability: 0.5\nfailpoints:\n  enabled: true\n  buggify: *block\n": `unknown field "activationProbability"`,
		} {
			var cfg Config
			require.NoError(t, yaml.Unmarshal([]byte(file), &cfg), file)
			_, err := cfg.Failpoints.ReadBuggify()
			require.ErrorContains(t, err, want, file)
		}
	})
	t.Run("a mapping that merges itself is an error, not a stack overflow", func(t *testing.T) {
		t.Parallel()
		for file, want := range map[string]string{
			// The block merges itself.
			"failpoints:\n  enabled: true\n  buggify: &block\n    seed: 1\n    <<: *block\n": "merges itself",
			// An entry of the allowlist merges itself.
			"failpoints:\n  enabled: true\n  buggify:\n    keys:\n      - &entry\n        match: a\n        <<: *entry\n": "merges itself",
			// Two mappings merge each other, through a list of merges.
			"failpoints:\n  enabled: true\n  buggify:\n    keys:\n      - &a\n        match: a\n        <<: [&b {<<: *a}]\n": "merges itself",
		} {
			var cfg Config
			require.NoError(t, yaml.Unmarshal([]byte(file), &cfg), file)
			_, err := cfg.Failpoints.ReadBuggify()
			require.ErrorContains(t, err, want, file)
		}
	})
	t.Run("an alias of a valid entry is read", func(t *testing.T) {
		t.Parallel()
		const file = "x-entry: &entry {match: a, activationProbability: 1, fireProbability: 1}\n" +
			"failpoints:\n  enabled: true\n  buggify:\n    seed: 1\n    keys:\n      - *entry\n      - <<: *entry\n        match: b\n        error: Unavailable\n"
		var cfg Config
		require.NoError(t, yaml.Unmarshal([]byte(file), &cfg))
		block, err := cfg.Failpoints.ReadBuggify()
		require.NoError(t, err)
		require.Len(t, block.Keys, 2)
		require.Equal(t, "a", block.Keys[0].Match)
		require.Equal(t, "b", block.Keys[1].Match)
		require.Equal(t, "Unavailable", block.Keys[1].Error)
	})
	t.Run("the seed is an unsigned integer", func(t *testing.T) {
		t.Parallel()
		for file, want := range map[string]uint64{
			"failpoints:\n  enabled: true\n  buggify:\n    seed: 0\n":                    0,
			"failpoints:\n  enabled: true\n  buggify:\n    seed: 0x2A\n":                 42,
			"failpoints:\n  enabled: true\n  buggify:\n    seed: 18446744073709551615\n": math.MaxUint64,
		} {
			var cfg Config
			require.NoError(t, yaml.Unmarshal([]byte(file), &cfg), file)
			block, err := cfg.Failpoints.ReadBuggify()
			require.NoError(t, err, file)
			require.Equal(t, want, block.Seed, file)
		}
	})
	t.Run("the files are layered", func(t *testing.T) {
		t.Parallel()
		var cfg Config
		// The first file has the block and the second turns the switch on: the final value counts.
		require.NoError(t, yaml.Unmarshal([]byte("failpoints:\n  buggify:\n    seed: 7\n    trace: true\n"), &cfg))
		require.NoError(t, yaml.Unmarshal([]byte("failpoints:\n  enabled: true\n  buggify:\n    seed: 8\n"), &cfg))
		block, err := cfg.Failpoints.ReadBuggify()
		require.NoError(t, err)
		require.Equal(t, FailpointsBuggify{Seed: 8, Trace: true}, block)

		// A later file that has keys replaces the list.
		require.NoError(t, yaml.Unmarshal([]byte("failpoints:\n  buggify:\n    keys: [{match: a, activationProbability: 1, fireProbability: 1}]\n"), &cfg))
		require.NoError(t, yaml.Unmarshal([]byte("failpoints:\n  buggify:\n    keys: [{match: b, activationProbability: 1, fireProbability: 1}]\n"), &cfg))
		block, err = cfg.Failpoints.ReadBuggify()
		require.NoError(t, err)
		require.Len(t, block.Keys, 1)
		require.Equal(t, "b", block.Keys[0].Match)
	})
	t.Run("a later file can clear the keys with an empty list but not with null", func(t *testing.T) {
		t.Parallel()
		const base = "failpoints:\n  enabled: true\n  buggify:\n    seed: 7\n    keys: [{match: a, activationProbability: 1, fireProbability: 1}]\n"

		// An empty list replaces the list of the earlier file, and the seed stays.
		var cleared Config
		require.NoError(t, yaml.Unmarshal([]byte(base), &cleared))
		require.NoError(t, yaml.Unmarshal([]byte("failpoints:\n  buggify:\n    keys: []\n"), &cleared))
		block, err := cleared.Failpoints.ReadBuggify()
		require.NoError(t, err)
		require.Equal(t, uint64(7), block.Seed)
		require.NotNil(t, block.Keys)
		require.Empty(t, block.Keys)

		// null is not a list: it doesn't silently keep the keys of the earlier file.
		for _, overlay := range []string{"keys: null", "keys:", "keys: ~"} {
			var cfg Config
			require.NoError(t, yaml.Unmarshal([]byte(base), &cfg))
			require.NoError(t, yaml.Unmarshal([]byte("failpoints:\n  buggify:\n    "+overlay+"\n"), &cfg), overlay)
			_, err := cfg.Failpoints.ReadBuggify()
			require.ErrorContains(t, err, "keys: want a list", overlay)
		}
	})
}
