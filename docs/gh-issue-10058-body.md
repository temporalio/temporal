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

Common use case: `IntervalSpec` for “every N days or weeks” with an end-user or workspace-scoped IANA time zone, where local wall time should remain stable when DST changes.
