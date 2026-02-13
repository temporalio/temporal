package tqid

import (
	"errors"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
)

func TestFromProtoPartition_Sticky(t *testing.T) {
	a := assert.New(t)

	nsid := "my-namespace"
	stickyName := "a8sdkf5ks"
	normalName := "very-normal"
	taskType := enumspb.TASK_QUEUE_TYPE_WORKFLOW
	kind := enumspb.TASK_QUEUE_KIND_STICKY
	proto := &taskqueuepb.TaskQueue{
		Name:       stickyName,
		Kind:       kind,
		NormalName: normalName,
	}

	p, err := PartitionFromProto(proto, nsid, taskType)
	a.NoError(err)
	a.Equal(nsid, p.NamespaceId())
	a.Equal(taskType, p.TaskType())
	a.Equal(kind, p.Kind())
	a.Equal(normalName, p.TaskQueue().Name())
	a.Equal(stickyName, p.(*StickyPartition).StickyName())
	a.Equal(stickyName, p.RpcName())
	a.False(p.IsRoot())
	a.Equal(PartitionKey{nsid, stickyName, 0, taskType}, p.Key())

	// should be able to parse without normal name, old clients may not send normal name.
	proto.NormalName = ""
	p, err = PartitionFromProto(proto, nsid, taskType)
	a.NoError(err)
	a.Equal(nsid, p.NamespaceId())
	a.Equal(taskType, p.TaskType())
	a.Equal(kind, p.Kind())
	a.Equal("", p.TaskQueue().Name())
	a.Equal(stickyName, p.(*StickyPartition).StickyName())
	a.Equal(stickyName, p.RpcName())
	a.False(p.IsRoot())
	a.Equal(PartitionKey{nsid, stickyName, 0, taskType}, p.Key())

	proto.Name = "/_sys/my-basic-tq-name/23"
	_, err = PartitionFromProto(proto, nsid, taskType)
	// sticky queue cannot have non-zero prtn
	a.True(errors.Is(err, ErrNonZeroSticky))
}

func TestFromProtoPartition_Normal(t *testing.T) {
	a := assert.New(t)

	nsid := "my-namespace"
	tqname := "my-basic-tq-name"
	taskType := enumspb.TASK_QUEUE_TYPE_WORKFLOW
	kind := enumspb.TASK_QUEUE_KIND_NORMAL
	proto := &taskqueuepb.TaskQueue{
		Name: tqname,
		Kind: kind,
	}

	p, err := PartitionFromProto(proto, nsid, taskType)
	a.NoError(err)
	a.Equal(nsid, p.NamespaceId())
	a.Equal(taskType, p.TaskType())
	a.Equal(kind, p.Kind())
	a.Equal(tqname, p.TaskQueue().Name())
	a.Equal(tqname, p.RpcName())
	a.True(p.IsRoot())
	a.Equal(PartitionKey{nsid, tqname, 0, taskType}, p.Key())

	proto.NormalName = "something"
	_, err = PartitionFromProto(proto, nsid, taskType)
	// normal queue cannot have normal name
	a.Error(err)

	proto.Name = "/_sys/my-basic-tq-name/23"
	proto.NormalName = ""
	p, err = PartitionFromProto(proto, nsid, taskType)
	a.NoError(err)
	a.Equal(nsid, p.NamespaceId())
	a.Equal(tqname, p.TaskQueue().Name())
	a.Equal(taskType, p.TaskType())
	a.Equal(kind, p.Kind())
	a.Equal(23, p.(*NormalPartition).PartitionId())
	a.Equal("/_sys/my-basic-tq-name/23", p.RpcName())
	a.False(p.IsRoot())
	a.Equal(PartitionKey{nsid, tqname, 23, taskType}, p.Key())
	a.Equal(4, mustParent(p, 5).PartitionId())
	a.Equal(0, mustParent(p, 32).PartitionId())

	proto.Name = "/_sys/my-basic-tq-name/verxyz:23"
	_, err = PartitionFromProto(proto, nsid, taskType)
	a.Error(err)

	proto.Name = "/_sys/my-basic-tq-name/verxyz#23"
	_, err = PartitionFromProto(proto, nsid, taskType)
	a.Error(err)
}

func TestFromBaseName(t *testing.T) {
	a := assert.New(t)

	f, err := NewTaskQueueFamily("", "my-basic-tq-name")
	a.NoError(err)
	a.Equal("my-basic-tq-name", f.Name())

	_, err = NewTaskQueueFamily("", "/_sys/my-basic-tq-name/23")
	a.Error(err)
}

func TestNormalPartition(t *testing.T) {
	a := assert.New(t)

	f, err := NewTaskQueueFamily("", "tq")
	a.NoError(err)
	p := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(23)
	a.Equal("tq", p.TaskQueue().Name())
	a.Equal(23, p.PartitionId())
	a.Equal("/_sys/tq/23", p.RpcName())
	a.False(p.IsRoot())
}

func TestValidRpcNames(t *testing.T) {
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
		{"/_sys/list0/1", "list0", 1},
		{"/_sys//list0//41", "/list0/", 41},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			p := mustParseNormalPartition(t, tc.input, 0)
			require.Equal(t, tc.partition, p.PartitionId())
			require.Equal(t, tc.partition == 0, p.IsRoot())
			require.Equal(t, tc.baseName, p.TaskQueue().Name())
			require.Equal(t, tc.input, p.RpcName())
		})
	}
}

func TestParentName(t *testing.T) {
	const invalid = "__invalid__"
	testCases := []struct {
		name   string
		degree int
		output string
	}{
		/* unexpected input */
		{"list0", 0, invalid},
		/* 1-ary tree */
		{"list0", 1, invalid},
		{"/_sys/list0/1", 1, "list0"},
		{"/_sys/list0/2", 1, "/_sys/list0/1"},
		/* 2-ary tree */
		{"list0", 2, invalid},
		{"/_sys/list0/1", 2, "list0"},
		{"/_sys/list0/2", 2, "list0"},
		{"/_sys/list0/3", 2, "/_sys/list0/1"},
		{"/_sys/list0/4", 2, "/_sys/list0/1"},
		{"/_sys/list0/5", 2, "/_sys/list0/2"},
		{"/_sys/list0/6", 2, "/_sys/list0/2"},
		/* 3-ary tree */
		{"/_sys/list0/1", 3, "list0"},
		{"/_sys/list0/2", 3, "list0"},
		{"/_sys/list0/3", 3, "list0"},
		{"/_sys/list0/4", 3, "/_sys/list0/1"},
		{"/_sys/list0/5", 3, "/_sys/list0/1"},
		{"/_sys/list0/6", 3, "/_sys/list0/1"},
		{"/_sys/list0/7", 3, "/_sys/list0/2"},
		{"/_sys/list0/10", 3, "/_sys/list0/3"},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"#"+strconv.Itoa(tc.degree), func(t *testing.T) {
			p := mustParseNormalPartition(t, tc.name, enumspb.TaskQueueType(rand.Intn(3)))
			parent, err := p.ParentPartition(tc.degree)
			if tc.output == invalid {
				require.Equal(t, ErrNoParent, err)
			} else {
				require.Equal(t, tc.output, parent.RpcName())
				require.Equal(t, p.TaskType(), parent.TaskType())
			}
		})
	}
}

func TestInvalidRpcNames(t *testing.T) {
	inputs := []string{
		"/_sys/",
		"/_sys/0",
		"/_sys//1",
		"/_sys//0",
		"/_sys/list0",
		"/_sys/list0/0",
		"/_sys/list0/-1",
		"/_sys/list0/abc",
		"/_sys//_sys/sys/0/41",
		"/_sys/list0:verxyz:23",
		"/_sys/list0/verxyz:23",
		"/_sys/list0/verxyz#23",
	}
	for _, name := range inputs {
		t.Run(name, func(t *testing.T) {
			_, err := PartitionFromProto(&taskqueuepb.TaskQueue{Name: name}, "", 0)
			require.Error(t, err)
		})
	}
}

func TestWithoutBatching(t *testing.T) {
	f := UnsafeTaskQueueFamily("asdf1234", "some-tq")
	tq := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	ps := make([]*NormalPartition, 5)
	for i := range ps {
		ps[i] = tq.NormalPartition(i)
	}

	// without batching, routing keys are different for each partition and indexes are zero
	unique := make(map[string]bool)
	for _, p := range ps {
		key, n := p.RoutingKey(0)
		assert.Zero(t, n)
		unique[key] = true
	}
	assert.Len(t, unique, len(ps))
}

func TestBatching(t *testing.T) {
	f := UnsafeTaskQueueFamily("asdf1234", "some-tq")
	tq := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	ps := make([]*NormalPartition, 5)
	for i := range ps {
		ps[i] = tq.NormalPartition(i)
	}

	// with batching by 3: 0,1,2 use one key, 3,4 use another key
	keys := make([]string, len(ps))
	ns := make([]int, len(ps))
	for i, p := range ps {
		keys[i], ns[i] = p.RoutingKey(3)
	}
	assert.Equal(t, []int{0, 1, 2, 0, 1}, ns)
	assert.Equal(t, keys[0], keys[1])
	assert.Equal(t, keys[0], keys[2])
	assert.Equal(t, keys[3], keys[4])
	assert.NotEqual(t, keys[0], keys[3])
}

func TestStableKeyForRootPartition(t *testing.T) {
	f := UnsafeTaskQueueFamily("asdf1234", "some-tq")
	tq := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	root := tq.RootPartition()

	// root does not move with vs without batching
	nbKey, nbN := root.RoutingKey(0)
	bKey, bN := root.RoutingKey(6)
	assert.Equal(t, nbKey, bKey)
	assert.Equal(t, nbN, bN)
}

func mustParseNormalPartition(t *testing.T, rpcName string, taskType enumspb.TaskQueueType) *NormalPartition {
	p, err := PartitionFromProto(&taskqueuepb.TaskQueue{Name: rpcName}, "", taskType)
	require.NoError(t, err)
	res, ok := p.(*NormalPartition)
	require.True(t, ok)
	return res
}

func mustParent(p Partition, n int) *NormalPartition {
	normalPrtn, ok := p.(*NormalPartition)
	if !ok {
		panic("not a normal partition")
	}
	parent, err := normalPrtn.ParentPartition(n)
	if err != nil {
		panic(err)
	}
	return parent
}
