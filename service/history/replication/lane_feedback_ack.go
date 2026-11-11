package replication

import (
	"time"

	"go.temporal.io/server/common/clock"
)

const laneFeedbackACKMinInterval = 100 * time.Millisecond

type laneFeedbackACKSchedule struct {
	period   time.Duration
	periodic time.Time
	lastSent time.Time
	pending  bool
	waiting  bool
}

func (s *laneFeedbackACKSchedule) deadline(now time.Time) time.Time {
	due := s.periodic
	if s.pending && !s.waiting && now.Before(due) {
		due = now
	}
	if earliest := s.lastSent.Add(laneFeedbackACKMinInterval); due.Before(earliest) {
		due = earliest
	}
	return due
}

func (s *laneFeedbackACKSchedule) complete(now time.Time, sent bool) {
	s.waiting = !sent
	if sent {
		s.lastSent, s.pending = now, false
	}
	coalesce := now
	if sent {
		coalesce = coalesce.Add(laneFeedbackACKMinInterval)
	}
	if !s.periodic.After(coalesce) {
		s.periodic = s.periodic.Add((coalesce.Sub(s.periodic)/s.period + 1) * s.period)
	}
}

func runLaneFeedbackACKLoop(source clock.TimeSource, period time.Duration, wake <-chan struct{}, shutdown <-chan struct{}, send func() (bool, error)) error {
	period = max(period, laneFeedbackACKMinInterval)
	schedule := laneFeedbackACKSchedule{period: period, periodic: source.Now().Add(period)}
	ticks, timer := source.NewTimer(period)
	defer timer.Stop()
	for {
		select {
		case <-shutdown:
			return nil
		default:
		}
		due := schedule.deadline(source.Now())
		timer.Stop()
		select {
		case <-ticks:
		default:
		}
		timer.Reset(max(0, due.Sub(source.Now())))
		select {
		case _, open := <-wake:
			if !open {
				wake = nil
				continue
			}
			schedule.pending = true
		case <-ticks:
			if source.Now().Before(due) {
				continue
			}
			sent, err := send()
			if err != nil {
				return err
			}
			schedule.complete(source.Now(), sent)
		case <-shutdown:
			return nil
		}
	}
}
