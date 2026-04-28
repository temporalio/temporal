package umpire

type Transition[S comparable] struct {
	EntityID string
	From     S
	To       S
}

func (e *Transition[S]) Key() string {
	return e.EntityID
}

func RecordTransition[S comparable](u *Umpire, entityID string, state *S, to S) bool {
	from := *state
	if from == to {
		return false
	}
	*state = to
	u.Record(&Transition[S]{
		EntityID: entityID,
		From:     from,
		To:       to,
	})
	return true
}

func LastTransitionSeqTo[S comparable](history []*Record, match func(S) bool) map[string]int64 {
	out := make(map[string]int64)
	for _, rec := range history {
		tr, ok := rec.Fact.(*Transition[S])
		if !ok || !match(tr.To) {
			continue
		}
		if rec.Seq > out[tr.EntityID] {
			out[tr.EntityID] = rec.Seq
		}
	}
	return out
}
