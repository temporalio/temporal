package testcore

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

type clusterCreator func(clusterTest, clusterRequest) (*FunctionalTestBase, error)
type clusterDestroyer func(*FunctionalTestBase) error
type clusterAcquireSource string

const (
	clusterAcquireSourcePooled    clusterAcquireSource = "pooled"
	clusterAcquireSourceWarmSpare clusterAcquireSource = "warm-spare"
	clusterAcquireSourceWarmMiss  clusterAcquireSource = "warm-miss"
	clusterAcquireSourceCustom    clusterAcquireSource = "custom"
)

// perTestClusterProvider bounds the number of tests that can own clusters and
// keeps separate inventories of pristine core and worker-enabled clusters ready for handoff.
type perTestClusterProvider struct {
	maxLiveTests int
	liveTests    chan struct{}
	spares       *warmSparePool[*FunctionalTestBase]
	workerSpares *warmSparePool[*FunctionalTestBase]
	create       clusterCreator
	createWarm   clusterCreator
	destroy      clusterDestroyer
}

type perTestClusterLease struct {
	cluster       *FunctionalTestBase
	provider      *perTestClusterProvider
	acquireSource clusterAcquireSource
	once          sync.Once
	err           error
}

func newPerTestClusterProvider(
	maxLiveTests int,
	warmSpares int,
	create clusterCreator,
	createWarm clusterCreator,
	destroy clusterDestroyer,
) *perTestClusterProvider {
	if createWarm == nil {
		createWarm = create
	}
	provider := &perTestClusterProvider{
		maxLiveTests: maxLiveTests,
		liveTests:    make(chan struct{}, maxLiveTests),
		create:       create,
		createWarm:   createWarm,
		destroy:      destroy,
	}
	workerWarmSpares := warmSpares / 2
	provider.spares = provider.newWarmSparePool(warmSpares-workerWarmSpares, clusterRequest{
		kind:            clusterKindDedicated,
		dedicated:       true,
		dedicatedReason: "warm spare",
	})
	provider.workerSpares = provider.newWarmSparePool(workerWarmSpares, clusterRequest{
		kind:              clusterKindDedicated,
		dedicated:         true,
		dedicatedReason:   "warm worker spare",
		needWorkerService: true,
	})
	return provider
}

func (p *perTestClusterProvider) newWarmSparePool(
	capacity int,
	request clusterRequest,
) *warmSparePool[*FunctionalTestBase] {
	return newWarmSparePool(capacity, func() (*FunctionalTestBase, error) {
		return p.createCluster(p.createWarm, newDetachedClusterT(request.dedicatedReason), request)
	}, p.destroy)
}

func (p *perTestClusterProvider) acquire(name string, request clusterRequest) (*perTestClusterLease, error) {
	p.liveTests <- struct{}{}
	acquired := false
	defer func() {
		if !acquired {
			<-p.liveTests
		}
	}()

	var cluster *FunctionalTestBase
	acquireSource := clusterAcquireSourceCustom
	if request.canUseWarmSpare() {
		acquireSource = clusterAcquireSourceWarmMiss
		spares := p.spares
		if request.needWorkerService {
			spares = p.workerSpares
		}
		spares.start()
		var ok bool
		var err error
		cluster, ok, err = spares.take()
		if err != nil {
			return nil, err
		}
		if ok {
			for key, value := range request.dynamicConfig {
				cluster.testCluster.host.overrideDynamicConfigForClusterLifetime(key, value)
			}
			cluster.usePreseededNamespace = true
			acquired = true
			return &perTestClusterLease{
				cluster:       cluster,
				provider:      p,
				acquireSource: clusterAcquireSourceWarmSpare,
			}, nil
		}
	}

	owner := newDetachedClusterT(name)
	var err error
	cluster, err = p.createCluster(p.create, owner, request)
	if err != nil {
		return nil, err
	}
	cluster.usePreseededNamespace = true
	acquired = true
	return &perTestClusterLease{
		cluster:       cluster,
		provider:      p,
		acquireSource: acquireSource,
	}, nil
}

func (p *perTestClusterProvider) createCluster(
	create clusterCreator,
	owner clusterTest,
	request clusterRequest,
) (cluster *FunctionalTestBase, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if _, ok := recovered.(clusterBootAbort); !ok {
				err = fmt.Errorf("cluster boot panicked: %v\n%s", recovered, debug.Stack())
			}
		}
		if bootOwner, ok := owner.(*sharedClusterT); ok {
			bootOwner.finishBoot()
			if err == nil {
				err = bootOwner.bootError()
			}
		}
		if err != nil && cluster != nil {
			err = errors.Join(err, p.destroy(cluster))
			cluster = nil
		}
	}()

	return create(owner, request)
}

func (p *perTestClusterProvider) startAndWait() error {
	p.spares.start()
	p.workerSpares.start()
	if err := p.spares.startAndWait(); err != nil {
		return err
	}
	return p.workerSpares.startAndWait()
}

func (p *perTestClusterProvider) testParallelism(configured int) int {
	return min(configured, p.maxLiveTests)
}

func (l *perTestClusterLease) release() error {
	l.once.Do(func() {
		l.err = l.provider.destroy(l.cluster)
		<-l.provider.liveTests
	})
	return l.err
}

func (p *perTestClusterProvider) close() {
	p.spares.close()
	p.workerSpares.close()
}

func (r clusterRequest) canUseWarmSpare() bool {
	return !r.requiresStartupConfig && len(r.clusterOpts) == 0
}
