package set

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	setpb "go.temporal.io/server/chasm/lib/set/gen/setpb/v1"
	"google.golang.org/protobuf/proto"
)

func TestStringSetUpdate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		initial  []string
		mutation Mutation[string]
		expected []string
	}{
		{
			name:     "add to empty set",
			mutation: Mutation[string]{Add: []string{"b", "a"}},
			expected: []string{"a", "b"},
		},
		{
			name:     "add present key",
			initial:  []string{"a"},
			mutation: Mutation[string]{Add: []string{"a", "a", "b"}},
			expected: []string{"a", "b"},
		},
		{
			name:     "remove present key",
			initial:  []string{"a", "b"},
			mutation: Mutation[string]{Remove: []string{"a"}},
			expected: []string{"b"},
		},
		{
			name:     "remove absent key",
			initial:  []string{"a"},
			mutation: Mutation[string]{Remove: []string{"b"}},
			expected: []string{"a"},
		},
		{
			name:     "remove wins over add for absent key",
			mutation: Mutation[string]{Add: []string{"a", "b"}, Remove: []string{"a"}},
			expected: []string{"b"},
		},
		{
			name:     "remove wins over add for present key",
			initial:  []string{"a"},
			mutation: Mutation[string]{Add: []string{"a"}, Remove: []string{"a"}},
			expected: []string{},
		},
		{
			name:     "empty mutation",
			initial:  []string{"a"},
			expected: []string{"a"},
		},
		{
			name:     "empty mutation on empty set",
			expected: []string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := &chasm.MockMutableContext{}
			set := NewStringSet()
			require.NoError(t, set.Update(ctx, Mutation[string]{Add: tc.initial}))

			require.NoError(t, set.Update(ctx, tc.mutation))
			keys, err := set.Read(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expected, keys)

			// Applying the same mutation again changes nothing.
			require.NoError(t, set.Update(ctx, tc.mutation))
			keys, err = set.Read(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expected, keys)
		})
	}
}

// TestStringSetUnchangedUpdateKeepsState checks that a mutation which leaves the keys as they are
// does not replace the stored keys.
func TestStringSetUnchangedUpdateKeepsState(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	set := newStringSet()
	require.NoError(t, set.Update(ctx, Mutation[string]{Add: []string{"a", "b"}}))
	stored := set.Keys

	for _, mutation := range []Mutation[string]{
		{},
		{Add: []string{"b", "a"}},
		{Remove: []string{"c"}},
		{Add: []string{"c"}, Remove: []string{"c"}},
	} {
		require.NoError(t, set.Update(ctx, mutation))
		require.Same(t, &stored[0], &set.Keys[0], "mutation %+v replaced the stored keys", mutation)
		require.Len(t, set.Keys, 2)
	}

	require.NoError(t, set.Update(ctx, Mutation[string]{Add: []string{"c"}}))
	keys, err := set.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, keys)
}

// TestStringSetPersistsArbitraryBytes covers keys such as task tokens, which are not valid UTF-8
// and so could not be stored in a proto string field.
func TestStringSetPersistsArbitraryBytes(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	set := newStringSet()
	keys := []string{"\x00\x01", "\xff\xfe"}
	require.NoError(t, set.Update(ctx, Mutation[string]{Add: keys}))

	blob, err := proto.Marshal(set.StringSetState)
	require.NoError(t, err)
	decoded := &stringSet{StringSetState: &setpb.StringSetState{}}
	require.NoError(t, proto.Unmarshal(blob, decoded.StringSetState))
	read, err := decoded.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, keys, read)
}

func TestLibraryRegistersStringSet(t *testing.T) {
	t.Parallel()
	components := NewNilLibrary().Components()
	require.Len(t, components, 1)
	require.Equal(t, reflect.TypeFor[*stringSet](), components[0].GoType())
}
