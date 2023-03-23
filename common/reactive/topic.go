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

// Package reactive provides a small set of reactive components inspired by ReactiveX to make PubSub easier,
// without introducing too many of the foreign concepts present in RxGo.
package reactive

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrPublishInterrupted          = errors.New("publish event to subscribers interrupted")
	ErrSubscriptionAlreadyCanceled = errors.New("subscription already canceled")
)

// Topic is an object that implements the PubSub pattern. Topic manages a set of subscriptions for you, and allows you
// to Publish events to them. See ExampleTopic for a demo. It is both safe and encouraged to embed the zero-value of a
// Topic object because there is no initialization required. Instances of Topic are thread-safe.
type Topic[T any] struct {

	// RWMutex is used to ensure thread-safety.
	sync.RWMutex

	// subscriptions is the list of active subscription objects tracking this topic. It is ok for this to be nil and
	// uninitialized.
	subscriptions []*subscription[T]

	// nextSubscriptionID is the unique id of the next subscription that will be created when Subscribe is called.
	nextSubscriptionID int
}

// Subscribe activates a subscription for the given channel. Any calls to Publish will attempt to send the event to the
// supplied channel, only failing when the context supplied to Publish is canceled. You don't have to call Unsubscribe
// on the returned Subscription because it does not spawn any goroutines.
func (t *Topic[T]) Subscribe(subscriber chan<- T) Subscription {
	t.Lock()
	defer t.Unlock()

	index := len(t.subscriptions)
	id := t.nextSubscriptionID
	s := &subscription[T]{
		topic:      t,
		subscriber: subscriber,
		index:      index,
		id:         id,
	}

	// append this subscription to the topic's set so that we can forward events to it.
	t.subscriptions = append(t.subscriptions, s)

	// increment the nextSubscriptionID so that the next subscription we create has a unique id.
	t.nextSubscriptionID++

	return s
}

// Publish attempts to send an event to the channels of all subscriptions.
// It will fail if the supplied context is canceled and return an ErrPublishInterrupted error.
func (t *Topic[T]) Publish(ctx context.Context, event T) error {
	t.RLock()
	defer t.RUnlock()

	for _, s := range t.subscriptions {
		select {
		case s.subscriber <- event:
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ErrPublishInterrupted, ctx.Err())
		}
	}

	return nil
}

// Subscription is a subscription to a Topic that you can Unsubscribe from to stop receiving updates.
type Subscription interface {
	// Unsubscribe stops this subscription. After this method returns, it is guaranteed that the
	// subject will no longer send any events to the channel supplied to Subscribe.
	Unsubscribe() error
}

// cancelSubscription removes the subscription at the index from the list of subscriptions.
// If the index is out-of-bounds, or the item at the index has a different id, then we return an
// ErrSubscriptionAlreadyCanceled error.
func (t *Topic[T]) cancelSubscription(index int, id int) error {
	t.Lock()
	defer t.Unlock()

	if index >= len(t.subscriptions) {
		return ErrSubscriptionAlreadyCanceled
	}

	if t.subscriptions[index].id != id {
		return ErrSubscriptionAlreadyCanceled
	}

	// delete the subscription by swapping it with the last element and then truncating the slice
	t.subscriptions[index] = t.subscriptions[len(t.subscriptions)-1]
	t.subscriptions[index].index = index
	t.subscriptions = t.subscriptions[:len(t.subscriptions)-1]

	return nil
}

// subscription represents a subscription to this topic's topic.
type subscription[T any] struct {
	// topic is the Topic that provides this subscription.
	topic *Topic[T]
	// id is unique amongst all of this topic's current and past subscriptions.
	id int
	// index is the index of this subscription within the topic's list of subscriptions.
	index int
	// subscriber is the channel that events are sent to.
	subscriber chan<- T
}

// Unsubscribe stops the subscription, freeing up the resources it was using in the topic object.
func (s *subscription[T]) Unsubscribe() error {
	return s.topic.cancelSubscription(s.index, s.id)
}
