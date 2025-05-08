package effect

import "context"

type Controller interface {
	OnAfterCommit(func(context.Context))
	OnAfterRollback(func(context.Context))
}
