package aggregate

var NoopMovingWindowAverage MovingWindowAverage = newNoopMovingWindowAverage()

type (
	noopMovingWindowAverage struct{}
)

func newNoopMovingWindowAverage() *noopMovingWindowAverage { return &noopMovingWindowAverage{} }

func (a *noopMovingWindowAverage) Record(_ int64) {}

func (a *noopMovingWindowAverage) Average() float64 { return 0 }
