Fixes #10058

## What

- `service/worker/scheduler/civil_interval.go`: civil Julian day + scan for the next local tick.
- `service/worker/scheduler/spec.go`: `nextIntervalTime` takes `time.Time` and uses `nextCivilIntervalTick` when the interval is a whole multiple of 86400s and the schedule has a **non-UTC** `*time.Location`; otherwise the existing UTC-math is unchanged.
- `TestSpecIntervalCivilDayUSPacificDST`: 2018-11-04 vs 2018-11-05 local midnights in `America/Los_Angeles` (07:00Z then 08:00Z after fall back).

## Backward compatibility

- Schedules that resolve to **UTC** (including explicit `UTC` / empty as today): same behavior as before.
- Schedules with a **named** non-UTC IANA zone and a **day-sized** (N×24h) interval: **intentional behavior change** so steps follow **civil** calendar days at a fixed local time, observing DST.

## Release note (suggested)

> **Possibly breaking:** `ScheduleSpec.Interval` with a duration of N×24h and a **non-UTC** `ScheduleSpec` timezone now advances on **civil** calendar days in that zone. Previously, such intervals were aligned only to **UTC** 86400s boundaries. Clients that relied on the old UTC grid can set the schedule timezone to `UTC` or use calendar/cron for explicit UTC-day scheduling.

## Motivation

Typical need: `IntervalSpec` for recurring “every N local days or weeks” when the schedule is tied to a user- or org-selected IANA time zone, so local wall time should stay stable across DST changes.
