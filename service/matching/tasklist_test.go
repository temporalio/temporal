package matching

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidTaskListNames(t *testing.T) {
	testCases := []struct {
		input     string
		baseName  string
		partition int
	}{
		{"0", "0", 0},
		{"list0", "list0", 0},
		{"/list0", "/list0", 0},
		{"/list0/", "/list0/", 0},
		{"__temporal_sys/list0", "__temporal_sys/list0", 0},
		{"__temporal_sys/list0/", "__temporal_sys/list0/", 0},
		{"/__temporal_sys_list0", "/__temporal_sys_list0", 0},
		{"/__temporal_sys/list0/1", "list0", 1},
		{"/__temporal_sys//list0//41", "/list0/", 41},
		{"/__temporal_sys//__temporal_sys/sys/0/41", "/__temporal_sys/sys/0", 41},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			tn, err := newTaskListName(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.partition, tn.partition)
			require.Equal(t, tc.partition == 0, tn.IsRoot())
			require.Equal(t, tc.baseName, tn.baseName)
			require.Equal(t, tc.baseName, tn.GetRoot())
			require.Equal(t, tc.input, tn.name)
		})
	}
}

func TestTaskListParentName(t *testing.T) {
	testCases := []struct {
		name   string
		degree int
		output string
	}{
		/* unexpected input */
		{"list0", 0, ""},
		/* 1-ary tree */
		{"list0", 1, ""},
		{"/__temporal_sys/list0/1", 1, "list0"},
		{"/__temporal_sys/list0/2", 1, "/__temporal_sys/list0/1"},
		/* 2-ary tree */
		{"list0", 2, ""},
		{"/__temporal_sys/list0/1", 2, "list0"},
		{"/__temporal_sys/list0/2", 2, "list0"},
		{"/__temporal_sys/list0/3", 2, "/__temporal_sys/list0/1"},
		{"/__temporal_sys/list0/4", 2, "/__temporal_sys/list0/1"},
		{"/__temporal_sys/list0/5", 2, "/__temporal_sys/list0/2"},
		{"/__temporal_sys/list0/6", 2, "/__temporal_sys/list0/2"},
		/* 3-ary tree */
		{"/__temporal_sys/list0/1", 3, "list0"},
		{"/__temporal_sys/list0/2", 3, "list0"},
		{"/__temporal_sys/list0/3", 3, "list0"},
		{"/__temporal_sys/list0/4", 3, "/__temporal_sys/list0/1"},
		{"/__temporal_sys/list0/5", 3, "/__temporal_sys/list0/1"},
		{"/__temporal_sys/list0/6", 3, "/__temporal_sys/list0/1"},
		{"/__temporal_sys/list0/7", 3, "/__temporal_sys/list0/2"},
		{"/__temporal_sys/list0/10", 3, "/__temporal_sys/list0/3"},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"#"+strconv.Itoa(tc.degree), func(t *testing.T) {
			tn, err := newTaskListName(tc.name)
			require.NoError(t, err)
			require.Equal(t, tc.output, tn.Parent(tc.degree))
		})
	}
}

func TestInvalidTasklistNames(t *testing.T) {
	inputs := []string{
		"/__temporal_sys/",
		"/__temporal_sys/0",
		"/__temporal_sys//1",
		"/__temporal_sys//0",
		"/__temporal_sys/list0",
		"/__temporal_sys/list0/0",
		"/__temporal_sys/list0/-1",
	}
	for _, name := range inputs {
		t.Run(name, func(t *testing.T) {
			_, err := newTaskListName(name)
			require.Error(t, err)
		})
	}
}
