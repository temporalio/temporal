package fc

import (
	"context"

	"go.temporal.io/server/service/matching/simplelimiter"
)

type localLimiterTx struct {
	lls    *localLimiterState
	config simplelimiter.Params
	pri    wakePriority
}

func newLocalLimiterTx(lls *localLimiterState, config simplelimiter.Params, pri wakePriority) *localLimiterTx {
	return &localLimiterTx{lls: lls, config: config, pri: pri}
}

func (llt *localLimiterTx) check(cb ReadinessCallback) error {
	return llt.lls.check(llt.config, cb, llt.pri)
}

func (llt *localLimiterTx) cancelCheck() {
	llt.lls.cancelCheck()
}

func (llt *localLimiterTx) reserve(context.Context) error {
	return nil // check/cancelCheck is all we need here
}

func (llt *localLimiterTx) commit(context.Context) error {
	return nil // check/cancelCheck is all we need here
}

func (llt *localLimiterTx) cancelReserve(context.Context) {
}

func (llt *localLimiterTx) close() {
}
