# IntervalSpec + IANA timezone / DST (reference)

**GitHub issue:** https://github.com/temporalio/temporal/issues/10058 (opened with this content).

# GitHub title (used)

`Schedule: IntervalSpec should advance by calendar days in ScheduleSpec timezone (DST)`


# Issue body (copy into github.com/temporalio/temporal/issues/new)

## Summary

`IntervalSpec` next-run calculation uses **fixed UTC second alignment** (`((ts-phase)/interval+1)*interval+phase`) and **ignores** `ScheduleSpec.timezone` for day-sized intervals. Schedules that mean “every N **local** days at a fixed local time” therefore **do not track DST** (e.g. US/Pacific: local midnight stays 07:00 UTC in PDT but should move to 08:00 UTC in PST after the fall transition).

`StructuredCalendar` / cron paths already use the loaded `*time.Location`; interval-only schedules do not.

## Reproduction (behavior before fix)

- `ScheduleSpec.timezone` = `America/Los_Angeles` (or any IANA zone with DST).
- `Interval` = `24h`, `Phase` = `8h` (first tick aligned to `1970-01-01 00:00:00` in that zone, i.e. `time.Unix(8*3600,0)` local representation).
- Observe `rawNextTime` / `GetNextTime` just after 2018-11-04 local midnight and 2018-11-05 local midnight:
  - **Before fix:** every tick is `k * 86400` seconds from the same UTC phase (e.g. always `07:00Z` in this setup).
  - **After fix:** 2018-11-04 `00:00` local is still PDT → `07:00Z`; 2018-11-05 `00:00` local is PST → `08:00Z`.

## Proposed semantics

When `interval` is a positive multiple of `86400` **and** the schedule has a **non-UTC** location, interpret the step as **N whole calendar days** in that location at the same **local** time-of-day as the grid reference `time.Unix(phase mod interval, 0).In(loc)`.

When the location is `UTC` (or unset/empty in a way that resolves to UTC), keep the **existing** fixed-UTC-second behavior for compatibility.

## Non-goals

- `43h` or other non–whole-day intervals: unchanged.
- This does not try to turn “24h” into “civil 24h” in zone for non-86400-multiple durations.

## Motivation

Common use case: `IntervalSpec` for “every N days or weeks” with an end-user or workspace-scoped IANA time zone, so local wall time should remain stable when DST changes.

---

# PR description (for the implementing PR)

## What

- `service/worker/scheduler/civil_interval.go`: civil Julian day + scan for next local tick.
- `service/worker/scheduler/spec.go`: `nextIntervalTime` takes `time.Time` and calls `nextCivilIntervalTick` when applicable; otherwise same UTC math as before.
- `TestSpecIntervalCivilDayUSPacificDST`: 2018-11-04 vs 2018-11-05 local midnights.

## Backward compatibility

- Schedules with **no** IANA offset (UTC-only behavior): **unchanged**.
- Schedules with a **named** non-UTC zone and **day/week** `Interval` aligned to 86400s: **intentional behavior change** to match wall-clock + DST (calendar semantics).

## Release note

> **Possibly breaking:** `ScheduleSpec.Interval` with a duration of N×24h and a **non-UTC** `ScheduleSpec` timezone now advances on **civil** calendar days in that zone. Previously, such intervals were aligned only to **UTC** 86400s boundaries. Clients that relied on the old UTC grid should set timezone to `UTC` or use calendar/cron for explicit UTC-day scheduling.

---

After merging, you can `gh issue create` / open the PR on https://github.com/temporalio/temporal with the same text.
