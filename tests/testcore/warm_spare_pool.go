package testcore

import "sync"

// warmSparePool owns a bounded inventory of resources created by one background
// goroutine. Taking a resource wakes the filler so inventory is replenished.
type warmSparePool[T any] struct {
	capacity int
	create   func() (T, error)
	destroy  func(T) error

	spares         chan T
	wake           chan struct{}
	stop           chan struct{}
	done           chan struct{}
	inventoryReady chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
	readyOnce sync.Once
	errMu     sync.Mutex
	err       error
}

func newWarmSparePool[T any](capacity int, create func() (T, error), destroy func(T) error) *warmSparePool[T] {
	return &warmSparePool[T]{
		capacity:       capacity,
		create:         create,
		destroy:        destroy,
		spares:         make(chan T, capacity),
		wake:           make(chan struct{}, 1),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
		inventoryReady: make(chan struct{}),
	}
}

func (p *warmSparePool[T]) start() {
	p.startOnce.Do(func() {
		go p.run()
	})
}

func (p *warmSparePool[T]) run() {
	defer close(p.done)
	if p.capacity == 0 {
		p.markReady()
	}
	for {
		for len(p.spares) < p.capacity {
			select {
			case <-p.stop:
				return
			default:
			}
			spare, err := p.create()
			if err != nil {
				p.errMu.Lock()
				p.err = err
				p.errMu.Unlock()
				p.markReady()
				return
			}
			select {
			case p.spares <- spare:
				if len(p.spares) == p.capacity {
					p.markReady()
				}
			case <-p.stop:
				_ = p.destroy(spare)
				return
			}
		}

		select {
		case <-p.wake:
		case <-p.stop:
			return
		}
	}
}

func (p *warmSparePool[T]) startAndWait() error {
	p.start()
	<-p.inventoryReady
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.err
}

func (p *warmSparePool[T]) markReady() {
	p.readyOnce.Do(func() { close(p.inventoryReady) })
}

func (p *warmSparePool[T]) take() (T, bool, error) {
	p.errMu.Lock()
	err := p.err
	p.errMu.Unlock()
	if err != nil {
		var zero T
		return zero, false, err
	}

	select {
	case spare := <-p.spares:
		select {
		case p.wake <- struct{}{}:
		default:
		}
		return spare, true, nil
	default:
		var zero T
		return zero, false, nil
	}
}

func (p *warmSparePool[T]) ready() int {
	return len(p.spares)
}

func (p *warmSparePool[T]) close() {
	p.closeOnce.Do(func() {
		close(p.stop)
		p.start()
		<-p.done
		for {
			select {
			case spare := <-p.spares:
				_ = p.destroy(spare)
			default:
				return
			}
		}
	})
}
