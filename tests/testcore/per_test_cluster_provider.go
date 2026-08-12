package testcore

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

type clusterCreator func(clusterTest, clusterRequest) (*FunctionalTestBase, error)
type clusterDestroyer func(*FunctionalTestBase) error

// perTestClusterProvider bounds how many tests can own clusters concurrently.
// Every successful acquire creates a new cluster and every release destroys it.
type perTestClusterProvider struct {
	live    chan struct{}
	create  clusterCreator
	destroy clusterDestroyer
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
	return min(configured, cap(p.live))
}

func (l *perTestClusterLease) release() error {
	l.once.Do(func() {
		l.err = l.provider.destroy(l.cluster)
		<-l.provider.live
	})
	return l.err
}
