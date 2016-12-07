package coroutine

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDispatcher(t *testing.T) {
	value := "foo"
	d := NewDispatcher(func(ctx Context) { value = "bar" })
	require.Equal(t, "foo", value)
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())
	require.Equal(t, "bar", value)
}

func TestNonBlockingChildren(t *testing.T) {
	var history []string
	d := NewDispatcher(func(ctx Context) {
		for i := 0; i < 10; i++ {
			ii := i
			ctx.NewCoroutine(func(ctx Context) {
				history = append(history, fmt.Sprintf("child-%v", ii))
			})
		}
		history = append(history, "root")
	})
	require.EqualValues(t, 0, len(history))
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())

	require.EqualValues(t, 11, len(history))
}

func TestNonbufferedChannel(t *testing.T) {
	var history []string
	d := NewDispatcher(func(ctx Context) {
		c1 := ctx.NewChannel()
		ctx.NewCoroutine(func(ctx Context) {
			history = append(history, "child-start")
			v, more := c1.Recv(ctx)
			assert.True(t, more)
			history = append(history, fmt.Sprintf("child-end-%v", v))
		})
		history = append(history, "root-before-channel-put")
		c1.Send(ctx, "value1")
		history = append(history, "root-after-channel-put")

	})
	require.EqualValues(t, 0, len(history))
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())

	expected := []string{
		"root-before-channel-put",
		"child-start",
		"child-end-value1",
		"root-after-channel-put",
	}
	require.EqualValues(t, expected, history)

}

func TestBufferedChannelPut(t *testing.T) {
	var history []string
	d := NewDispatcher(func(ctx Context) {
		c1 := ctx.NewBufferedChannel(1)
		ctx.NewCoroutine(func(ctx Context) {
			history = append(history, "child-start")
			v1, more := c1.Recv(ctx)
			assert.True(t, more)
			history = append(history, fmt.Sprintf("child-end-%v", v1))
			v2, more := c1.Recv(ctx)
			assert.True(t, more)
			history = append(history, fmt.Sprintf("child-end-%v", v2))

		})
		history = append(history, "root-before-channel-put")
		c1.Send(ctx, "value1")
		history = append(history, "root-after-channel-put1")
		c1.Send(ctx, "value2")
		history = append(history, "root-after-channel-put2")
	})
	require.EqualValues(t, 0, len(history))
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())

	expected := []string{
		"root-before-channel-put",
		"root-after-channel-put1",
		"child-start",
		"child-end-value1",
		"child-end-value2",
		"root-after-channel-put2",
	}
	require.EqualValues(t, expected, history)
}

func TestBufferedChannelGet(t *testing.T) {
	var history []string
	d := NewDispatcher(func(ctx Context) {
		c1 := ctx.NewChannel()
		c2 := ctx.NewBufferedChannel(2)

		ctx.NewCoroutine(func(ctx Context) {
			history = append(history, "child1-start")
			c2.Send(ctx, "bar1")
			history = append(history, "child1-get")
			v1, more := c1.Recv(ctx)
			assert.True(t, more)
			history = append(history, fmt.Sprintf("child1-end-%v", v1))

		})
		ctx.NewCoroutine(func(ctx Context) {
			history = append(history, "child2-start")
			c2.Send(ctx, "bar2")
			history = append(history, "child2-get")
			v1, more := c1.Recv(ctx)
			assert.True(t, more)
			history = append(history, fmt.Sprintf("child2-end-%v", v1))
		})
		history = append(history, "root-before-channel-get1")
		c2.Recv(ctx)
		history = append(history, "root-before-channel-get2")
		c2.Recv(ctx)
		history = append(history, "root-before-channel-put")
		c1.Send(ctx, "value1")
		history = append(history, "root-after-channel-put1")
		c1.Send(ctx, "value2")
		history = append(history, "root-after-channel-put2")
	})
	require.EqualValues(t, 0, len(history))
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())

	expected := []string{
		"root-before-channel-get1",
		"child1-start",
		"child1-get",
		"child2-start",
		"child2-get",
		"root-before-channel-get2",
		"root-before-channel-put",
		"root-after-channel-put1",
		"root-after-channel-put2",
		"child1-end-value1",
		"child2-end-value2",
	}
	require.EqualValues(t, expected, history)
}

func TestNotBlockingSelect(t *testing.T) {
	var history []string
	d := NewDispatcher(func(ctx Context) {
		c1 := ctx.NewBufferedChannel(1)
		c2 := ctx.NewBufferedChannel(1)
		s := ctx.NewSelector()
		s.
			AddRecv(c1, func(v interface{}, more bool) {
				assert.True(t, more)
				history = append(history, fmt.Sprintf("c1-%v", v))
			}).
			AddRecv(c2, func(v interface{}, more bool) {
				assert.True(t, more)
				history = append(history, fmt.Sprintf("c2-%v", v))
			}).
			AddDefault(func() { history = append(history, "default") })
		c1.Send(ctx, "one")
		s.Select(ctx)
		c2.Send(ctx, "two")
		s.Select(ctx)
		s.Select(ctx)
	})
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())

	expected := []string{
		"c1-one",
		"c2-two",
		"default",
	}
	require.EqualValues(t, expected, history)
}

func TestBlockingSelect(t *testing.T) {
	var history []string
	d := NewDispatcher(func(ctx Context) {
		c1 := ctx.NewChannel()
		c2 := ctx.NewChannel()
		ctx.NewCoroutine(func(ctx Context) {
			history = append(history, "add-one")
			c1.Send(ctx, "one")
		})
		ctx.NewCoroutine(func(ctx Context) {
			history = append(history, "add-two")
			c2.Send(ctx, "two")
		})

		s := ctx.NewSelector()
		s.
			AddRecv(c1, func(v interface{}, more bool) {
				assert.True(t, more)
				history = append(history, fmt.Sprintf("c1-%v", v))
			}).
			AddRecv(c2, func(v interface{}, more bool) {
				assert.True(t, more)
				history = append(history, fmt.Sprintf("c2-%v", v))
			})
		history = append(history, "select1")
		s.Select(ctx)
		history = append(history, "select2")
		s.Select(ctx)
		history = append(history, "done")
	})
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())

	expected := []string{
		"select1",
		"add-one",
		"add-two",
		"c1-one",
		"select2",
		"c2-two",
		"done",
	}
	require.EqualValues(t, expected, history)
}

func TestSendSelect(t *testing.T) {
	var history []string
	d := NewDispatcher(func(ctx Context) {
		c1 := ctx.NewChannel()
		c2 := ctx.NewChannel()
		ctx.NewCoroutine(func(ctx Context) {
			history = append(history, "receiver")
			v, more := c2.Recv(ctx)
			assert.True(t, more)
			history = append(history, fmt.Sprintf("c2-%v", v))
			v, more = c1.Recv(ctx)

			assert.True(t, more)
			history = append(history, fmt.Sprintf("c1-%v", v))
		})
		s := ctx.NewSelector()
		s.AddSend(c1, "one", func() { history = append(history, "send1") }).
			AddSend(c2, "two", func() { history = append(history, "send2") })
		history = append(history, "select1")
		s.Select(ctx)
		history = append(history, "select2")
		s.Select(ctx)
		history = append(history, "done")
	})
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())

	expected := []string{
		"select1",
		"receiver",
		"send2",
		"select2",
		"c2-two",
		"send1",
		"done",
		"c1-one",
	}
	require.EqualValues(t, expected, history)
}

func TestChannelClose(t *testing.T) {
	var history []string
	d := NewDispatcher(func(ctx Context) {
		jobs := ctx.NewBufferedChannel(5)
		done := ctx.NewNamedChannel("done")

		ctx.NewNamedCoroutine("receiver", func(ctx Context) {
			for {
				j, more := jobs.Recv(ctx)
				if more {
					history = append(history, fmt.Sprintf("received job %v", j))
				} else {
					history = append(history, "received all jobs")
					done.Send(ctx, true)
					return
				}
			}
		})
		for j := 1; j <= 3; j++ {
			jobs.Send(ctx, j)
			history = append(history, fmt.Sprintf("sent job %v", j))
		}
		jobs.Close()
		history = append(history, "sent all jobs")
		_, _ = done.Recv(ctx)
		history = append(history, "done")

	})
	require.EqualValues(t, 0, len(history))
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone(), d.StackTrace())

	expected := []string{
		"sent job 1",
		"sent job 2",
		"sent job 3",
		"sent all jobs",
		"received job 1",
		"received job 2",
		"received job 3",
		"received all jobs",
		"done",
	}
	require.EqualValues(t, expected, history)
}

func TestSendClosedChannel(t *testing.T) {
	d := NewDispatcher(func(ctx Context) {
		defer func() {
			assert.NotNil(t, recover(), "panic expected")
		}()
		c := ctx.NewChannel()
		ctx.NewCoroutine(func(ctx Context) {
			c.Close()
		})
		c.Send(ctx, "baz")
	})
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())
}

func TestBlockedSendClosedChannel(t *testing.T) {
	d := NewDispatcher(func(ctx Context) {
		defer func() {
			assert.NotNil(t, recover(), "panic expected")
		}()
		c := ctx.NewBufferedChannel(5)
		c.Send(ctx, "bar")
		c.Close()
		c.Send(ctx, "baz")
	})
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())
}

func TestAsyncSendClosedChannel(t *testing.T) {
	d := NewDispatcher(func(ctx Context) {
		defer func() {
			assert.NotNil(t, recover(), "panic expected")
		}()
		c := ctx.NewBufferedChannel(5)
		c.Send(ctx, "bar")
		c.Close()
		_ = c.SendAsync("baz")
	})
	d.ExecuteUntilAllBlocked()
	require.True(t, d.IsDone())
}

func TestDispatchClose(t *testing.T) {
	var history []string
	d := NewDispatcher(func(ctx Context) {
		c := ctx.NewNamedChannel("forever_blocked")
		for i := 0; i < 10; i++ {
			ii := i
			ctx.NewNamedCoroutine(fmt.Sprintf("c-%v", i), func(ctx Context) {
				_, _ = c.Recv(ctx) // blocked forever
				history = append(history, fmt.Sprintf("child-%v", ii))
			})
		}
		history = append(history, "root")
		_, _ = c.Recv(ctx) // blocked forever
	})
	require.EqualValues(t, 0, len(history))
	d.ExecuteUntilAllBlocked()
	require.False(t, d.IsDone())
	stack := d.StackTrace()
	// 11 coroutines (3 lines each) + 10 nl
	require.EqualValues(t, 11*3+10, len(strings.Split(stack, "\n")), stack)
	require.Contains(t, stack, "coroutine 1 [blocked on forever_blocked.Recv]:")
	for i := 0; i < 10; i++ {
		require.Contains(t, stack, fmt.Sprintf("coroutine c-%v [blocked on forever_blocked.Recv]:", i))
	}
	beforeClose := runtime.NumGoroutine()
	d.Close()
	time.Sleep(100 * time.Millisecond) // Let all goroutines to die
	closedCount := beforeClose - runtime.NumGoroutine()
	require.EqualValues(t, 11, closedCount)
	expected := []string{
		"root",
	}
	require.EqualValues(t, expected, history)
}

func TestPanic(t *testing.T) {
	var history []string
	d := NewDispatcher(func(ctx Context) {
		c := ctx.NewNamedChannel("forever_blocked")
		for i := 0; i < 10; i++ {
			ii := i
			ctx.NewNamedCoroutine(fmt.Sprintf("c-%v", i), func(ctx Context) {
				if ii == 9 {
					panic("simulated failure")
				}
				_, _ = c.Recv(ctx) // blocked forever
				history = append(history, fmt.Sprintf("child-%v", ii))
			})
		}
		history = append(history, "root")
		_, _ = c.Recv(ctx) // blocked forever
	})
	require.EqualValues(t, 0, len(history))
	err := d.ExecuteUntilAllBlocked()
	require.NotNil(t, err)
	require.EqualValues(t, "simulated failure", err.Value())
	require.EqualValues(t, "simulated failure", err.Error())

	require.Contains(t, err.StackTrace(), "common/coroutine.TestPanic")
}
