package testcore

import (
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"testing"
)

type clusterCreator func(clusterTest, clusterRequest) (*FunctionalTestBase, error)
type clusterDestroyer func(*FunctionalTestBase) error

// perTestClusterProvider bounds how many tests can own clusters concurrently.
// Each test gets one cluster, which is destroyed when that test completes.
type perTestClusterProvider struct {
	live    chan struct{}
	create  clusterCreator
	destroy clusterDestroyer
	leases  sync.Map
}

type perTestClusterEntry struct {
	ready   chan struct{}
	request clusterRequest
	lease   *perTestClusterLease
	err     error
}

type perTestClusterLease struct {
	cluster  *FunctionalTestBase
	provider *perTestClusterProvider
	once     sync.Once
	err      error
}

func newPerTestClusterProvider(
	maxLive int,
	create clusterCreator,
	destroy clusterDestroyer,
) *perTestClusterProvider {
	return &perTestClusterProvider{
		live:    make(chan struct{}, maxLive),
		create:  create,
		destroy: destroy,
	}
}

func (p *perTestClusterProvider) acquire(name string, request clusterRequest) (*perTestClusterLease, error) {
	p.live <- struct{}{}
	acquired := false
	defer func() {
		if !acquired {
			<-p.live
		}
	}()

	owner := newDetachedClusterTestOwner(name)
	cluster, err := p.createCluster(owner, request)
	if err != nil {
		return nil, err
	}
	acquired = true
	return &perTestClusterLease{cluster: cluster, provider: p}, nil
}

func (p *perTestClusterProvider) clusterForTest(
	t *testing.T,
	request clusterRequest,
) (*FunctionalTestBase, error) {
	entry := &perTestClusterEntry{ready: make(chan struct{})}
	actual, loaded := p.leases.LoadOrStore(t, entry)
	if loaded {
		entry = actual.(*perTestClusterEntry)
		<-entry.ready
		if entry.err != nil {
			return nil, entry.err
		}
		if err := entry.request.satisfies(request); err != nil {
			return nil, err
		}
		return entry.lease.cluster, nil
	}

	entry.request = request
	entry.lease, entry.err = p.acquire(t.Name(), request)
	if entry.err != nil {
		close(entry.ready)
		p.leases.CompareAndDelete(t, entry)
		return nil, entry.err
	}

	cluster := entry.lease.cluster
	cluster.SetT(t)
	t.Cleanup(func() {
		p.leases.CompareAndDelete(t, entry)
		if err := entry.lease.release(); err != nil {
			t.Logf("Failed to tear down per-test cluster: %v", err)
		}
	})
	cluster.RegisterTest(t)
	close(entry.ready)
	return cluster, nil
}

func (existing clusterRequest) satisfies(request clusterRequest) error {
	if request.needWorkerService && !existing.needWorkerService {
		return errors.New("worker service must be requested by the first NewEnv call in a test")
	}
	for key, value := range request.dynamicConfig {
		existingValue, ok := existing.dynamicConfig[key]
		if !ok || !reflect.DeepEqual(existingValue, value) {
			return fmt.Errorf("dynamic config %q must be requested with the same value by the first NewEnv call in a test", key)
		}
	}
	if len(request.clusterOpts) == 0 {
		return nil
	}

	existingParams := ApplyTestClusterOptions(existing.clusterOpts)
	requestedParams := ApplyTestClusterOptions(request.clusterOpts)
	if len(existingParams.AdditionalServerOptions) > 0 || len(requestedParams.AdditionalServerOptions) > 0 {
		return errors.New("additional server options must only be requested by the first NewEnv call in a test")
	}
	existingParams.bootPhaseObserver = nil
	requestedParams.bootPhaseObserver = nil
	if !reflect.DeepEqual(existingParams, requestedParams) {
		return errors.New("cluster options must be requested with the same values by the first NewEnv call in a test")
	}
	return nil
}

func (p *perTestClusterProvider) createCluster(
	owner *clusterTestOwner,
	request clusterRequest,
) (cluster *FunctionalTestBase, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if _, ok := recovered.(clusterBootAbort); !ok {
				err = fmt.Errorf("cluster boot panicked: %v\n%s", recovered, debug.Stack())
			}
		}
		owner.finishBoot()
		if err == nil {
			err = owner.bootError()
		}
		if err != nil && cluster != nil {
			err = errors.Join(err, p.destroy(cluster))
			cluster = nil
		}
		if err != nil {
			owner.doCleanups()
		}
	}()

	return p.create(owner, request)
}

func (p *perTestClusterProvider) testParallelism(configured int) int {
	parallelism := min(configured, cap(p.live)/2)
	if parallelism < 1 {
		panic("per-test cluster limit must be at least 2")
	}
	return parallelism
}

func (l *perTestClusterLease) release() error {
	l.once.Do(func() {
		l.err = l.provider.destroy(l.cluster)
		<-l.provider.live
	})
	return l.err
}
