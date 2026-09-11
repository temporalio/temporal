package searchattribute

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
)

func TestManagerRefreshErrorCooldown(t *testing.T) {
	for _, test := range []struct {
		name string
		warm bool
		err  error
	}{
		{"cold resource exhausted", false, persistence.ErrPersistenceSystemLimitExceeded},
		{"warm resource exhausted", true, persistence.ErrPersistenceSystemLimitExceeded},
		{"cold unavailable", false, serviceerror.NewUnavailable("metadata unavailable")},
		{"cold timeout", false, context.DeadlineExceeded},
		{"warm timeout", true, context.DeadlineExceeded},
		{"cold unexpected error", false, errors.New("metadata failure")},
		{"warm unexpected error", true, errors.New("metadata failure")},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, metadata, timeSource, logs := newRefreshTestManager(t)
			if test.warm {
				metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(refreshTestMetadata(1, "Old"), nil)
				_, err := m.GetSearchAttributes("index", false)
				require.NoError(t, err)
				timeSource.Advance(61 * time.Second)
			}
			metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).DoAndReturn(
				func(context.Context) (*persistence.GetClusterMetadataResponse, error) {
					timeSource.Advance(10 * time.Second)
					return nil, test.err
				})
			for range 20 {
				attributes, err := m.GetSearchAttributes("index", false)
				require.Equal(t, test.err, err)
				require.Empty(t, attributes.Custom())
			}
			// Slow I/O must not consume the minimum retry interval.
			timeSource.Advance(time.Second - time.Nanosecond)
			_, err := m.GetSearchAttributes("index", false)
			require.Equal(t, test.err, err)
			require.EqualValues(t, 1, logs.MatchCount())

			timeSource.Advance(time.Second)
			metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(refreshTestMetadata(2, "New"), nil)
			attributes, err := m.GetSearchAttributes("index", false)
			require.NoError(t, err)
			require.Contains(t, attributes.Custom(), "New")
			timeSource.Advance(59 * time.Second)
			_, err = m.GetSearchAttributes("index", false)
			require.NoError(t, err)
		})
	}
}

func newRefreshTestManager(t *testing.T) (*managerImpl, *persistence.MockClusterMetadataManager, *clock.EventTimeSource, *testlogger.Expectation) {
	t.Helper()
	metadata := persistence.NewMockClusterMetadataManager(gomock.NewController(t))
	timeSource := clock.NewEventTimeSource()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	logs := logger.Expect(testlogger.Error, "failed to refresh search attributes cache")
	return NewManager(timeSource, metadata, logger, func() bool { return false }), metadata, timeSource, logs
}

func refreshTestMetadata(version int64, field string) *persistence.GetClusterMetadataResponse {
	return &persistence.GetClusterMetadataResponse{
		Version: version,
		ClusterMetadata: &persistencespb.ClusterMetadata{IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
			"index": {CustomSearchAttributes: map[string]enumspb.IndexedValueType{field: enumspb.INDEXED_VALUE_TYPE_KEYWORD}},
		}},
	}
}

func TestManagerConcurrentFailedRefresh(t *testing.T) {
	m, metadata, timeSource, logs := newRefreshTestManager(t)
	entered, release := make(chan struct{}), make(chan struct{})
	releaseRefresh := sync.OnceFunc(func() { close(release) })
	defer releaseRefresh()
	refreshErr := persistence.ErrPersistenceSystemLimitExceeded
	metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).DoAndReturn(
		func(context.Context) (*persistence.GetClusterMetadataResponse, error) {
			close(entered)
			<-release
			return nil, refreshErr
		})
	const callers = 32
	results := make(chan error, callers)
	for range callers {
		go func() {
			_, err := m.GetSearchAttributes("index", false)
			results <- err
		}()
	}
	awaitRefreshTest(t, entered)
	timeSource.Advance(10 * time.Second)
	releaseRefresh()
	for range callers {
		require.Same(t, refreshErr, awaitRefreshTest(t, results))
	}
	require.EqualValues(t, 1, logs.MatchCount())
}

func TestManagerForceBypassesRefreshError(t *testing.T) {
	m, metadata, _, logs := newRefreshTestManager(t)
	refreshErr := persistence.ErrPersistenceSystemLimitExceeded
	metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(nil, refreshErr).Times(3)
	_, err := m.GetSearchAttributes("index", false)
	require.Same(t, refreshErr, err)
	_, err = m.GetSearchAttributes("index", true)
	require.Same(t, refreshErr, err)
	m.forceRefresh = func() bool { return true }
	_, err = m.GetSearchAttributes("index", false)
	require.Same(t, refreshErr, err)
	m.forceRefresh = func() bool { return false }
	_, err = m.GetSearchAttributes("index", false)
	require.Same(t, refreshErr, err)
	require.EqualValues(t, 3, logs.MatchCount())
}

func TestManagerFailedForcePreservesFreshCache(t *testing.T) {
	m, metadata, timeSource, logs := newRefreshTestManager(t)
	metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(refreshTestMetadata(1, "Old"), nil)
	_, err := m.GetSearchAttributes("index", false)
	require.NoError(t, err)
	timeSource.Advance(59*time.Second + 500*time.Millisecond)
	refreshErr := persistence.ErrPersistenceSystemLimitExceeded
	metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(nil, refreshErr)
	_, err = m.GetSearchAttributes("index", true)
	require.Same(t, refreshErr, err)
	attributes, err := m.GetSearchAttributes("index", false)
	require.NoError(t, err)
	require.Contains(t, attributes.Custom(), "Old")
	// After the original TTL, ordinary callers share the failure until retry is due.
	timeSource.Advance(500*time.Millisecond + time.Nanosecond)
	_, err = m.GetSearchAttributes("index", false)
	require.Same(t, refreshErr, err)
	require.EqualValues(t, 1, logs.MatchCount())
}

func TestManagerRefreshClearsRetainedError(t *testing.T) {
	for _, test := range []struct {
		name string
		warm bool
		err  error
	}{
		{"not found", false, serviceerror.NewNotFound("metadata not persisted")},
		{"warm unavailable", true, serviceerror.NewUnavailable("metadata unavailable")},
		{"unchanged version", true, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, metadata, timeSource, logs := newRefreshTestManager(t)
			if test.warm {
				metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(refreshTestMetadata(1, "Old"), nil)
				_, err := m.GetSearchAttributes("index", false)
				require.NoError(t, err)
				timeSource.Advance(61 * time.Second)
			}
			metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(nil, persistence.ErrPersistenceSystemLimitExceeded)
			_, err := m.GetSearchAttributes("index", false)
			require.Error(t, err)
			metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(refreshTestMetadata(1, "Ignored"), test.err)
			attributes, err := m.GetSearchAttributes("index", true)
			require.NoError(t, err)
			if test.warm {
				require.Contains(t, attributes.Custom(), "Old")
			} else {
				require.Empty(t, attributes.Custom())
			}
			_, err = m.GetSearchAttributes("index", false)
			require.NoError(t, err)
			timeSource.Advance(19 * time.Second)
			_, err = m.GetSearchAttributes("index", false)
			require.NoError(t, err)
			require.EqualValues(t, 1, logs.MatchCount())
		})
	}
}

func TestManagerSaveInvalidatesRefreshError(t *testing.T) {
	for _, saveErr := range []error{nil, errors.New("write conflict")} {
		t.Run(fmt.Sprint(saveErr), func(t *testing.T) {
			m, metadata, _, _ := newRefreshTestManager(t)
			metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(nil, persistence.ErrPersistenceSystemLimitExceeded)
			_, err := m.GetSearchAttributes("index", false)
			require.Error(t, err)
			metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(refreshTestMetadata(1, "Old"), nil)
			metadata.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).Return(false, saveErr)
			err = m.SaveSearchAttributes(t.Context(), "index", map[string]enumspb.IndexedValueType{"New": enumspb.INDEXED_VALUE_TYPE_KEYWORD})
			require.Equal(t, saveErr, err)
			metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(refreshTestMetadata(2, "New"), nil)
			attributes, err := m.GetSearchAttributes("index", false)
			require.NoError(t, err)
			require.Contains(t, attributes.Custom(), "New")
		})
	}
}

func TestManagerSaveInvalidationFollowsRefresh(t *testing.T) {
	m, metadata, _, _ := newRefreshTestManager(t)
	entered, release, written := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseRefresh := sync.OnceFunc(func() { close(release) })
	defer releaseRefresh()
	metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).DoAndReturn(
		func(context.Context) (*persistence.GetClusterMetadataResponse, error) {
			close(entered)
			<-release
			return refreshTestMetadata(1, "Old"), nil
		})
	refreshed := make(chan error, 1)
	go func() {
		_, err := m.GetSearchAttributes("index", false)
		refreshed <- err
	}()
	awaitRefreshTest(t, entered)
	metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(refreshTestMetadata(1, "Old"), nil)
	metadata.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *persistence.SaveClusterMetadataRequest) (bool, error) {
			close(written)
			return true, nil
		})
	saved := make(chan error, 1)
	go func() {
		saved <- m.SaveSearchAttributes(t.Context(), "index", map[string]enumspb.IndexedValueType{"New": enumspb.INDEXED_VALUE_TYPE_KEYWORD})
	}()
	// Save I/O completes while the previous refresh still owns the publication lock.
	awaitRefreshTest(t, written)
	select {
	case err := <-saved:
		t.Fatalf("Save returned before the in-flight refresh released publication: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseRefresh()
	require.NoError(t, awaitRefreshTest(t, refreshed))
	require.NoError(t, awaitRefreshTest(t, saved))
	metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(refreshTestMetadata(2, "New"), nil)
	attributes, err := m.GetSearchAttributes("index", false)
	require.NoError(t, err)
	require.Contains(t, attributes.Custom(), "New")
}

func awaitRefreshTest[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cache test goroutine")
		var zero T
		return zero
	}
}

func TestManagerColdUnavailablePreservesError(t *testing.T) {
	m, metadata, _, logs := newRefreshTestManager(t)
	refreshErr := serviceerror.NewUnavailable("metadata unavailable")
	metadata.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(nil, refreshErr)
	for range 2 {
		attributes, err := m.GetSearchAttributes("index", false)
		require.Error(t, err)
		require.Same(t, refreshErr, err)
		require.Empty(t, attributes.Custom())
	}
	require.EqualValues(t, 1, logs.MatchCount())
}
