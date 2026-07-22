package rpctest

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScriptMatchesByPredicateAndRecordsInvocationOrder(t *testing.T) {
	var script Script
	script.Expect("start", "second", MatchType(func(request int) bool { return request == 2 }), func(any) (any, error) {
		return "two", nil
	})
	script.Expect("start", "first", MatchType(func(request int) bool { return request == 1 }), func(any) (any, error) {
		return "one", nil
	})

	response, err := script.Invoke(context.Background(), "start", 1)
	require.NoError(t, err)
	require.Equal(t, "one", response)
	response, err = script.Invoke(context.Background(), "start", 2)
	require.NoError(t, err)
	require.Equal(t, "two", response)
	require.NoError(t, script.AssertExhausted())
	require.Equal(t, []Call{{Sequence: 1, Method: "start", Request: 1}, {Sequence: 2, Method: "start", Request: 2}}, script.Calls())
}

func TestScriptReportsUnmatchedAndAmbiguousCalls(t *testing.T) {
	t.Run("unmatched", func(t *testing.T) {
		var script Script
		_, err := script.Invoke(context.Background(), "missing", nil)
		require.ErrorContains(t, err, "unmatched call")
	})
	t.Run("ambiguous", func(t *testing.T) {
		var script Script
		script.Expect("start", "first", nil, nil)
		script.Expect("start", "second", nil, nil)
		_, err := script.Invoke(context.Background(), "start", nil)
		require.ErrorContains(t, err, "ambiguous call")
	})
}

func TestResponderDoesNotHoldScriptMutex(t *testing.T) {
	var script Script
	started := make(chan struct{})
	unblock := make(chan struct{})
	script.ExpectContext("first", "blocking", nil, func(context.Context, any) (any, error) {
		close(started)
		<-unblock
		return nil, nil
	})
	script.Expect("second", "independent", nil, nil)

	var wg sync.WaitGroup
	firstResult := make(chan error, 1)
	wg.Go(func() {
		_, err := script.Invoke(context.Background(), "first", nil)
		firstResult <- err
	})
	<-started
	_, err := script.Invoke(context.Background(), "second", nil)
	require.NoError(t, err)
	close(unblock)
	wg.Wait()
	require.NoError(t, <-firstResult)
}
