package testcore

import (
	"errors"
	"fmt"
	"log"
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

	warmMu sync.Mutex
	warm   *warmClusterReserve
}

type warmClusterReserve struct {
	ready           chan *perTestClusterLease
	target          int
	maxRefills      int
	building        int
	stopping        bool
	refillsDisabled bool
	teardownErr     error
	builds          sync.WaitGroup
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
	p.warmMu.Lock()
	reserve := p.warm
	p.warmMu.Unlock()
	if reserve != nil && request.warmEligible() {
		select {
		case lease := <-reserve.ready:
			p.scheduleWarmRefill()
			return lease, nil
		default:
		}
	}

	if reserve == nil {
		p.live <- struct{}{}
	} else if request.warmEligible() {
		select {
		case lease := <-reserve.ready:
			p.scheduleWarmRefill()
			return lease, nil
		case p.live <- struct{}{}:
		}
	} else {
		select {
		case p.live <- struct{}{}:
		default:
			select {
			case lease := <-reserve.ready:
				if err := p.destroyWarmClusterForDemand(lease); err != nil {
					return nil, err
				}
			case p.live <- struct{}{}:
			}
		}
	}
	acquired := false
	defer func() {
		if !acquired {
			<-p.live
		}
	}()

	lease, err := p.createLease(name, request)
	if err != nil {
		return nil, err
	}
	acquired = true
	return lease, nil
}

func (p *perTestClusterProvider) destroyWarmClusterForDemand(lease *perTestClusterLease) error {
	// Keep the warm cluster's live slot reserved so the custom cluster can take it over.
	if err := p.destroy(lease.cluster); err != nil {
		<-p.live
		p.scheduleWarmRefill()
		return err
	}
	return nil
}

func (p *perTestClusterProvider) startWarmReserve(target int, maxRefills int) error {
	if target == 0 {
		return nil
	}
	if target < 0 || target > cap(p.live) {
		return fmt.Errorf("warm cluster target must be between 0 and %d", cap(p.live))
	}
	if maxRefills <= 0 {
		return errors.New("warm cluster refill limit must be positive")
	}

	reserve := &warmClusterReserve{
		ready:      make(chan *perTestClusterLease, target),
		target:     target,
		maxRefills: maxRefills,
	}
	p.warmMu.Lock()
	if p.warm != nil {
		p.warmMu.Unlock()
		return errors.New("warm cluster reserve already started")
	}
	p.warm = reserve
	p.warmMu.Unlock()

	type result struct {
		lease *perTestClusterLease
		err   error
	}
	results := make(chan result, target)
	for range target {
		p.live <- struct{}{}
		go func() {
			lease, err := p.createLease("warm-reserve", clusterRequest{reason: "warm reserve"})
			results <- result{lease: lease, err: err}
		}()
	}

	var err error
	for range target {
		created := <-results
		if created.err != nil {
			<-p.live
			err = errors.Join(err, created.err)
			continue
		}
		reserve.ready <- created.lease
	}
	if err != nil {
		err = errors.Join(err, p.stopWarmReserve())
	}
	return err
}

func (p *perTestClusterProvider) stopWarmReserve() error {
	p.warmMu.Lock()
	reserve := p.warm
	if reserve == nil {
		p.warmMu.Unlock()
		return nil
	}
	reserve.stopping = true
	p.warm = nil
	p.warmMu.Unlock()

	reserve.builds.Wait()
	p.warmMu.Lock()
	err := reserve.teardownErr
	p.warmMu.Unlock()
	for {
		select {
		case lease := <-reserve.ready:
			err = errors.Join(err, lease.release())
		default:
			return err
		}
	}
}

func (p *perTestClusterProvider) scheduleWarmRefill() {
	p.warmMu.Lock()
	defer p.warmMu.Unlock()
	reserve := p.warm
	if reserve == nil || reserve.stopping || reserve.refillsDisabled {
		return
	}
	for len(reserve.ready)+reserve.building < reserve.target && reserve.building < reserve.maxRefills {
		select {
		case p.live <- struct{}{}:
			reserve.building++
			reserve.builds.Add(1)
			go p.buildWarmCluster(reserve)
		default:
			return
		}
	}
}

func (p *perTestClusterProvider) buildWarmCluster(reserve *warmClusterReserve) {
	defer reserve.builds.Done()
	lease, err := p.createLease("warm-reserve", clusterRequest{reason: "warm reserve"})

	p.warmMu.Lock()
	reserve.building--
	stopping := reserve.stopping
	logRefillFailure := err != nil && !stopping && !reserve.refillsDisabled
	if err != nil {
		reserve.refillsDisabled = true
	}
	if err == nil && !stopping {
		reserve.ready <- lease
	}
	p.warmMu.Unlock()

	if err != nil {
		<-p.live
		if logRefillFailure {
			log.Printf("test cluster warm refills disabled: %v", err)
		}
		return
	}
	if stopping {
		if err := lease.release(); err != nil {
			p.warmMu.Lock()
			reserve.teardownErr = errors.Join(reserve.teardownErr, err)
			p.warmMu.Unlock()
		}
		return
	}
	p.scheduleWarmRefill()
}

func (p *perTestClusterProvider) runWithWarmReserve(run func() int, target int, maxRefills int) int {
	if err := p.startWarmReserve(target, maxRefills); err != nil {
		log.Printf("test cluster warm reserve disabled: %v", err)
		return run()
	}
	exitCode := run()
	if err := p.stopWarmReserve(); err != nil {
		log.Printf("test cluster warm reserve teardown failed: %v", err)
		return 1
	}
	return exitCode
}

func (p *perTestClusterProvider) createLease(name string, request clusterRequest) (*perTestClusterLease, error) {
	owner := newDetachedClusterTestOwner(name)
	cluster, err := p.createCluster(owner, request)
	if err != nil {
		return nil, err
	}
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

func (existing clusterRequest) warmEligible() bool {
	return !existing.needWorkerService && len(existing.dynamicConfig) == 0 && len(existing.clusterOpts) == 0
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
		l.provider.scheduleWarmRefill()
	})
	return l.err
}
