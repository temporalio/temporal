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

package reactive

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleTopic() {
	// hostPoolChange represents a change that adds and/or removes hosts
	type hostPoolChange struct {
		added   []string
		removed []string
	}

	// hostPool is the object we want to publish changes from
	type hostPool struct {
		// by embedding a Topic object, clients may now subscribe to the hostPool easily and the hostPool itself
		// can easily publish changes without managing subscribers itself.
		Topic[hostPoolChange]
	}

	// subscriber is the channel we'll read host pool changes from
	ch := make(chan hostPoolChange)

	// p is an example host pool. Note that we don't need to initialize the Topic. The zero-value is valid.
	p := hostPool{}

	// sub is a subscription to the host pool changes.
	// Notice that we did not have to write any code to manage the set of subscribers within the hostPool object.
	sub := p.Subscribe(ch)

	// publish publishes a change from the pool to all of its subscriptions
	publish := func(c hostPoolChange) {
		if err := p.Publish(context.Background(), c); err != nil {
			panic(err)
		}
	}

	go func() {
		// publish some changes
		publish(hostPoolChange{
			added: []string{"127.0.0.1:8000"},
		})
		publish(hostPoolChange{
			added: []string{"127.0.0.1:8000"},
		})
		publish(hostPoolChange{
			removed: []string{"127.0.0.1:8000"},
		})

		// close the channel (this is done by the subscriber's code, not managed by the topic)
		close(ch)
	}()

	for change := range ch {
		fmt.Printf("%+v\n", change)
	}

	if err := sub.Unsubscribe(); err != nil {
		panic(err)
	}
	// Output: {added:[127.0.0.1:8000] removed:[]}
	// {added:[127.0.0.1:8000] removed:[]}
	// {added:[] removed:[127.0.0.1:8000]}
}

func TestTopic_Publish_Ok(t *testing.T) {
	t.Parallel()

	tp := Topic[int]{}
	ch := make(chan int, 1)

	tp.Subscribe(ch)

	err := tp.Publish(context.Background(), 1)

	require.NoError(t, err)
	assert.Equal(t, 1, <-ch)
}

func TestTopic_Publish_ContextCanceled(t *testing.T) {
	t.Parallel()

	tp := Topic[int]{}
	ch := make(chan int)
	tp.Subscribe(ch)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	errs := make(chan error)

	go func() {
		errs <- tp.Publish(ctx, 1)
	}()

	err := <-errs
	assert.ErrorIs(t, err, ErrPublishInterrupted)
}

func TestTopic_Unsubscribe_Ok(t *testing.T) {
	t.Parallel()

	tp := Topic[int]{}
	ch := make(chan int)
	sub := tp.Subscribe(ch)

	err := sub.Unsubscribe()
	assert.NoError(t, err)

	// closing the channel here means that any subsequent send would cause the test to panic and fail, but we shouldn't
	// send because we canceled the subscription.
	close(ch)
	assert.NoError(t, tp.Publish(context.Background(), 1))
}

func TestTopic_Unsubscribe_OutOfBounds(t *testing.T) {
	t.Parallel()

	tp := Topic[int]{}
	ch := make(chan int)

	sub := tp.Subscribe(ch)
	_ = sub.Unsubscribe()

	err := sub.Unsubscribe()

	assert.ErrorIs(t, err, ErrSubscriptionAlreadyCanceled)
}

func TestTopic_Unsubscribe_AlreadyUnsubscribed(t *testing.T) {
	t.Parallel()

	tp := Topic[int]{}
	ch := make(chan int)
	sub := tp.Subscribe(ch)
	tp.Subscribe(ch)

	_ = sub.Unsubscribe()

	err := sub.Unsubscribe()

	assert.ErrorIs(t, err, ErrSubscriptionAlreadyCanceled)
}
