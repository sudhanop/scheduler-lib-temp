# rlib-scheduler: In-Memory Go Scheduling Library

A lightweight, zero-dependency, concurrency-safe recurring task scheduler for Go applications, built exclusively with the Go standard library.

---

## Table of Contents

- [8.1 Project Overview](#81-project-overview)
- [8.2 Problem Before and After the Library](#82-problem-before-and-after-the-library)
- [8.3 Architecture](#83-architecture)
- [8.4 Project Structure](#84-project-structure)
- [8.5 Prerequisites and Installation](#85-prerequisites-and-installation)
- [8.6 Quick Start](#86-quick-start)
- [8.7 Every Scheduling Method](#87-every-scheduling-method)
  - [Every (Fixed Interval)](#every-fixed-interval)
  - [Hourly & HourlyAt (Top of Clock Hour)](#hourly--hourlyat-top-of-clock-hour)
  - [Daily (Fixed Time of Day)](#daily-fixed-time-of-day)
  - [Monthly (Day of Month with Clamping)](#monthly-day-of-month-with-clamping)
  - [Yearly (Calendar Date with Leap Year Handling)](#yearly-calendar-date-with-leap-year-handling)
- [8.8 Message Examples and Output Walkthrough](#88-message-examples-and-output-walkthrough)
- [8.9 Error Handling and Diagnostics](#89-error-handling-and-diagnostics)
- [8.10 Time Zones and Calendar Rules](#810-time-zones-and-calendar-rules)
- [8.11 Concurrency and Missed Executions](#811-concurrency-and-missed-executions)
- [8.12 Testing Strategy and Execution](#812-testing-strategy-and-execution)
- [8.13 Integration into Another Go Project](#813-integration-into-another-go-project)
- [8.14 Common Errors and Troubleshooting](#814-common-errors-and-troubleshooting)
- [8.15 Limitations and Future Improvements](#815-limitations-and-future-improvements)
- [8.16 Learning and Explanation Guide (Junior-to-Senior Q&A)](#816-learning-and-explanation-guide-junior-to-senior-qa)

---

## 8.1 Project Overview

`rlib-scheduler` is a standalone Go library designed to schedule and execute background tasks repeatedly within a running application process.

### Core Philosophy
- **Zero Third-Party Dependencies**: Written entirely using the Go standard library (`time`, `sync`, `sync/atomic`, `context`).
- **Separation of Concerns**: The scheduler controls *when* a task executes; your application logic controls *what* the task executes.
- **In-Memory & Embedded**: Operates inside your application without requiring external daemons, Linux `cron`, Redis, databases, or message brokers.
- **Concurrency Protection**: By default, prevents slow jobs from overlapping with themselves, and ensures no long-running job blocks other scheduled jobs.
- **Deterministic & Clock-Aligned**: Supports both duration intervals (e.g., every 30 seconds) and clock-aligned calendar events (e.g., at the top of every clock hour, 9:00 AM daily, 1st of every month, Jan 1st yearly).

---

## 8.2 Problem Before and After the Library

### The Problem Before
Without a centralized scheduler library, every Go microservice or background worker has to hand-roll its own scheduling mechanisms:
1. **Ad-hoc `time.Sleep` or `time.Ticker` loops**: Scattered throughout multiple goroutines with inconsistent error handling and no coordinated shutdown.
2. **Clock Drift and Missed Seconds**: Checking `if time.Now().Second() == 0` can fail under CPU contention or scheduler delays, causing a once-per-hour job to miss an entire hour.
3. **Leaked Goroutines & Resource Contention**: A slow network call inside a 1-minute ticker can trigger multiple concurrent instances of the same job, exhausting database connections and memory.
4. **Calendar Calculation Bugs**: Naively adding `24 * time.Hour` breaks across Daylight Saving Time (DST) changes, and scheduling a job on the "31st" crashes or silently skips February and 30-day months.
5. **No Clean Shutdown**: When `SIGTERM` arrives, background loops are abruptly severed mid-execution without waiting for running jobs to finish.

### The Solution After
With `rlib-scheduler`:
- **Unified Registration API**: Register intervals, daily, monthly, or yearly schedules through a single, consistent API.
- **Event-Driven Sleeping**: The scheduler sleeps using `time.NewTimer` until the exact nanosecond the next job is due, using zero CPU when idle.
- **Safe Concurrency Defaults**: Automatically serializes runs of the same job (non-overlapping) while running distinct jobs concurrently in parallel.
- **Graceful Shutdown**: Intercepts shutdown contexts, cancels task contexts, and waits cleanly for in-flight tasks to terminate.
- **Standardized Error Handling**: Task errors are captured, recorded on the job status, and routed to your custom error handler.

---

## 8.3 Architecture

The library is organized around five primary components:

```
+-------------------------------------------------------------------------+
|                              Application                                |
|  - Defines Tasks: func(ctx context.Context) error                      |
|  - Registers with Scheduler (Every, Hourly, Daily, Monthly, Yearly)     |
+------------------------------------+------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                              Scheduler                                  |
|  - Thread-safe registry: map[string]*Job                                |
|  - Deterministic priority ordering: []string                            |
|  - Event-driven run loop: Timer-based sleeping & instant wakeups        |
+------------------+-----------------------------------+------------------+
                   |                                   |
                   v                                   v
+--------------------------------------+   +------------------------------+
|               Job                    |   |           Schedule           |
|  - Metadata, run counts, timestamps  |   |  - Next(from time.Time)      |
|  - Concurrency lock (atomic CAS)     |   |  - Description() string      |
|  - Timeout configuration             |   +------------------------------+
+------------------+-------------------+
                   |
                   v
+-------------------------------------------------------------------------+
|                        Worker Execution Pool                            |
|  - Dispatched in dedicated goroutines: go dispatchJob(job, rootCtx)    |
|  - Non-blocking: Slow tasks never delay other scheduled jobs            |
|  - Graceful tracking: sync.WaitGroup & root context propagation         |
+-------------------------------------------------------------------------+
```

### Component Responsibilities

1. **`Scheduler`**: The coordinator. Holds registered jobs, computes which job is due next, sleeps until that moment, and dispatches due jobs into separate goroutines.
2. **`Schedule`**: An interface with a single calculation method: `Next(from time.Time) time.Time`. It encapsulates all calendar arithmetic, timezones, and duration offsets.
3. **`Job`**: Represents an individual registered task. Tracks next run time, last execution time, last error, total run count, and active execution state.
4. **`Task`**: A function signature: `type Task func(ctx context.Context) error`. Enables tasks to listen for cancellation via `ctx.Done()`.
5. **`Worker Goroutines`**: Every due task is launched in its own goroutine with atomic concurrency gates and `sync.WaitGroup` tracking.

---

## 8.4 Project Structure

```
rlib-scheduler/
├── go.mod                  # Module definition (github.com/sudhanop/scheduler-lib-temp)
├── README.md               # Complete user manual, API documentation, and architecture guide
├── HANDOVER.md             # In-depth technical handover and mentor discussion guide
├── errors.go               # Sentinel errors (ErrEmptyJobName, ErrDuplicateJob, etc.)
├── clock.go                # Clock interface and realClock for deterministic testing
├── schedule.go             # Schedule interface & Interval/Hourly/Daily/Monthly/Yearly engines
├── job.go                  # Job definition, atomic state trackers, and JobInfo snapshots
├── options.go              # Functional options for Scheduler and Job configuration
├── scheduler.go            # Scheduler coordinator, event loop, and lifecycle management
├── schedule_test.go        # Unit tests for interval, clock alignment, and calendar edge cases
├── scheduler_test.go       # Integration tests for concurrency, errors, and graceful shutdown
└── examples/
    └── basic/
        └── main.go         # Runnable demonstration registering all 7 required schedules
```

---

## 8.5 Prerequisites and Installation

### Prerequisites
- **Go**: Version 1.22 or newer (tested with Go 1.26.5).
- Standard operating systems: Linux, macOS, Windows.

### Using Locally in Development
To use the library locally within a multi-module workspace or via Go's `replace` directive:

```bash
# In your consuming application's go.mod:
module my-app

go 1.22

require github.com/sudhanop/scheduler-lib-temp v0.0.0

replace github.com/sudhanop/scheduler-lib-temp => /path/to/rlib-scheduler
```

### Importing from a Remote Git Repository
Once pushed to GitHub:

```bash
go get github.com/sudhanop/scheduler-lib-temp@v1.0.0
```

---

## 8.6 Quick Start

Here is a complete, copyable, self-contained application:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	scheduler "github.com/sudhanop/scheduler-lib-temp"
)

func main() {
	// 1. Create a new scheduler instance
	s := scheduler.New(
		scheduler.WithErrorHandler(func(jobName string, err error) {
			log.Printf("Job %s failed: %v", jobName, err)
		}),
	)

	// 2. Define a cancellation-aware task
	cleanupTask := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			fmt.Printf("[%s] Executing database cleanup\n", time.Now().Format("15:04:05"))
			return nil
		}
	}

	// 3. Register the task to run every 10 seconds
	err := s.RegisterInterval("db-cleanup", 10*time.Second, cleanupTask, scheduler.WithImmediateFirstRun())
	if err != nil {
		log.Fatalf("Registration failed: %v", err)
	}

	// 4. Start the scheduler in the background
	if err := s.Start(); err != nil {
		log.Fatalf("Start failed: %v", err)
	}
	fmt.Println("Scheduler running. Press Ctrl+C to stop.")

	// 5. Wait for termination signal and shut down gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nStopping scheduler...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Stop(shutdownCtx); err != nil {
		log.Fatalf("Forced shutdown error: %v", err)
	}
	fmt.Println("Scheduler stopped cleanly.")
}
```

---

## 8.7 Every Scheduling Method

### Every (Fixed Interval)
- **Purpose**: Runs a task repeatedly after a fixed duration.
- **Syntax**: `scheduler.Every(interval time.Duration) (Schedule, error)` or `s.RegisterInterval(name, interval, task, opts...)`
- **Parameters**: `interval time.Duration` (must be `> 0`).
- **Return Value**: `(Schedule, error)`. Returns `ErrInvalidInterval` if `interval <= 0`.
- **First Run Behavior**:
  - By default, the first execution occurs **after** the first interval duration has elapsed.
  - If registered with `WithImmediateFirstRun()`, the first execution occurs immediately upon start, followed by normal intervals.
- **Example**:
  ```go
  err := s.RegisterInterval("heartbeat", 30*time.Second, task)
  ```

---

### Hourly & HourlyAt (Top of Clock Hour)
- **Purpose**: Runs at the beginning of every clock hour (e.g., 09:00:00, 10:00:00, 11:00:00).
- **Difference from Every(60 * time.Minute)**:
  - If an application starts at `09:17:42`:
    - `Every(60 * time.Minute)` runs at `10:17:42`, `11:17:42`, `12:17:42`.
    - `Hourly()` aligns to the clock: it runs at `10:00:00`, `11:00:00`, `12:00:00`.
- **Syntax**:
  - `scheduler.Hourly(loc ...*time.Location) (Schedule, error)` (triggers at minute 0, second 0)
  - `scheduler.HourlyAt(minute, second int, loc ...*time.Location) (Schedule, error)`
- **Parameters**: `minute` (0-59), `second` (0-59), optional `*time.Location` (defaults to `time.Local`).
- **Return Value**: `(Schedule, error)`. Returns `ErrInvalidSchedule` if minutes or seconds are out of bounds.
- **Example**:
  ```go
  err := s.RegisterHourly("hourly-sync", 0, 0, task)
  ```

---

### Daily (Fixed Time of Day)
- **Purpose**: Runs every day at a specific local clock time (e.g. 09:00:00 AM).
- **Syntax**: `scheduler.Daily(hour, minute, second int, loc ...*time.Location) (Schedule, error)` or `s.RegisterDaily(name, hour, minute, second, task, opts...)`
- **Parameters**: `hour` (0-23), `minute` (0-59), `second` (0-59), optional `*time.Location`.
- **Return Value**: `(Schedule, error)`.
- **Behavior**:
  - If the scheduled time today has not yet passed, it executes today at that time.
  - If the scheduled time today has already passed, it executes tomorrow at that time.
- **Example**:
  ```go
  // Daily at 9:00:00 AM in the server's local time zone
  err := s.RegisterDaily("daily-report", 9, 0, 0, task)
  ```

---

### Monthly (Day of Month with Clamping)
- **Purpose**: Runs on a specified day of each month at a specified time.
- **Syntax**: `scheduler.Monthly(day, hour, minute, second int, loc ...*time.Location) (Schedule, error)` or `s.RegisterMonthly(name, day, hour, minute, second, task, opts...)`
- **Parameters**: `day` (1-31), `hour` (0-23), `minute` (0-59), `second` (0-59).
- **Calendar Edge Cases & Clamping**:
  - What happens when a month has fewer days than `day`? (e.g. day 31 requested, but February has 28/29 days; April, June, September, November have 30 days).
  - **Clamping Policy**: The scheduler automatically clamps to the **last valid calendar day of that month** (e.g., Feb 28 in non-leap years, Feb 29 in leap years, April 30).
  - This ensures that monthly tasks run **exactly once every single month** without silently skipping months!
- **Example**:
  ```go
  // Runs on the 1st of every month at midnight
  err := s.RegisterMonthly("monthly-billing", 1, 0, 0, 0, task)
  ```

---

### Yearly (Calendar Date with Leap Year Handling)
- **Purpose**: Runs on a specified month and day once a year at a specified time.
- **Syntax**: `scheduler.Yearly(month time.Month, day, hour, minute, second int, loc ...*time.Location) (Schedule, error)` or `s.RegisterYearly(name, month, day, hour, minute, second, task, opts...)`
- **Parameters**: `month` (1-12), `day` (1-31), `hour` (0-23), `minute` (0-59), `second` (0-59).
- **Leap Year Edge Case (February 29)**:
  - If scheduled on `February 29`:
    - In a leap year (e.g., 2028), it triggers on `February 29`.
    - In a non-leap year (e.g., 2026, 2027), it automatically clamps to `February 28`.
  - The job never skips years.
- **Invalid Date Validation**:
  - Imposssible dates like February 30 or April 31 are rejected at registration with `ErrInvalidSchedule`.
- **Example**:
  ```go
  // January 1st at midnight
  err := s.RegisterYearly("new-year-archive", time.January, 1, 0, 0, 0, task)
  ```

---

## 8.8 Message Examples and Output Walkthrough

The demonstration application (`examples/basic/main.go`) registers all 7 requested schedules. When run, it outputs a formatted roster of next execution timestamps:

```text
================================================================================
              rlib-scheduler: Production Go Scheduler Demonstration             
================================================================================
[STATUS] Scheduler started successfully. Running background event loop.

JOB NAME               | SCHEDULE DEFINITION              | NEXT SCHEDULED EXECUTION  | TIME UNTIL  
---------------------------------------------------------------------------------------------------------------
heartbeat-30s          | Every 30s                        | 2026-10-19 12:09:21       | in 30s       
metrics-1m             | Every 1m0s                       | 2026-10-19 12:09:51       | in 1m0s      
market-scan-10m        | Every 10m0s                      | 2026-10-19 12:18:51       | in 10m0s     
clock-hourly           | Hourly at 00:00 (Local)          | 2026-10-19 13:00:00       | in 51m9s     
daily-digest-9am       | Daily at 09:00:00 (Local)        | 2026-10-20 09:00:00       | in 20h51m9s  
monthly-report-1st     | Monthly on day 1 at 00:00:00 (Local) | 2026-11-01 00:00:00       | in 299h51m9s 
yearly-newyear-jan1    | Yearly on January 1 at 00:00:00 (Local) | 2027-01-01 00:00:00       | in 1763h51m9s
---------------------------------------------------------------------------------------------------------------
Press Ctrl+C to initiate graceful shutdown.
```

When running with the `-immediate` flag (`go run ./examples/basic -immediate`), all 7 jobs run once immediately upon start:

```text
[12:08:45.134] [Yearly-Audit]   Annual regulatory compliance archive generated
[12:08:45.134] [1m-Metrics]     Aggregated CPU and memory usage metrics
[12:08:45.134] [10m-MarketScan] Market scan completed. 248 tickers processed
[12:08:45.134] [30s-Heartbeat]  Heartbeat check OK. System health: GREEN
[12:08:45.134] [Daily-Digest]   Daily briefing report compiled and published
[12:08:45.134] [Hourly-Sync]    Top-of-the-hour synchronization dispatched
[12:08:45.134] [Monthly-Bill]   Monthly billing cycle statements generated
```

---

## 8.9 Error Handling and Diagnostics

### Registration Validation
Registration methods return descriptive errors immediately upon bad input:
- `ErrEmptyJobName`: An empty string `""` was provided as the job name.
- `ErrDuplicateJob`: A job with that name is already registered.
- `ErrNilSchedule`: A nil schedule pointer was passed.
- `ErrNilTask`: A nil task function was passed.
- `ErrInvalidInterval`: Interval duration was `<= 0`.
- `ErrInvalidSchedule`: Calendar hour, minute, second, day, or month was out of range.

### Task Failure Handling
Tasks can return errors (`func(ctx context.Context) error`). When a task returns an error:
1. The error is saved in `JobInfo.LastError`.
2. The scheduler invokes the configured `ErrorHandler` callback in a non-blocking goroutine.
3. The scheduler **never crashes or stops** due to a task error. Subsequent ticks for that job and other jobs proceed as scheduled.

---

## 8.10 Time Zones and Calendar Rules

1. **Default Time Zone**: All calendar schedules default to `time.Local` (the machine's local time zone).
2. **Explicit Time Zone**: You can pass any `*time.Location` to `Hourly`, `Daily`, `Monthly`, or `Yearly`:
   ```go
   nyLoc, _ := time.LoadLocation("America/New_York")
   sched, _ := scheduler.Daily(9, 30, 0, nyLoc)
   ```
3. **Daylight Saving Time (DST)**: Because calendar calculations use Go's `time.Date(...)` with the explicit location, Go automatically resolves 23-hour or 25-hour days during DST transitions without drift.
4. **Month-End Clamping**: Days 29, 30, and 31 are automatically clamped to the last day of shorter months, preventing skipped months.

---

## 8.11 Concurrency and Missed Executions

### Concurrency Policy
- **Different Jobs**: Always execute concurrently in parallel without blocking each other.
- **Same Job (Default: Non-Concurrent)**: By default, overlapping runs of the *same* job are disabled. If job $A$ takes 45 seconds to finish, but is scheduled every 30 seconds, the 30-second tick detects that job $A$ is still running using an atomic compare-and-swap operation. The tick is skipped, and `ErrJobAlreadyRunning` is passed to the `ErrorHandler`.
- **Opt-in Concurrent Runs**: To allow multiple overlapping instances of the same job, register with `WithAllowConcurrent(true)`.

### Process Restarts and Missed Executions
- `rlib-scheduler` is an **in-memory** scheduler. It does not store job state to disk or a database.
- If the application process is stopped or restarts, unexecuted historical events are **not** played back. Upon startup, jobs evaluate their next occurrence from the current system time.
- If the machine enters sleep mode or CPU contention delays a tick, the scheduler executes the due job once immediately upon wakeup and recalculates future runs without burst catch-up loops.

---

## 8.12 Testing Strategy and Execution

The test suite tests scheduling logic deterministically without depending on long sleeps or wall-clock passage:

```bash
# Run all unit tests
go test -v ./...

# Run all tests with Go race detection enabled
go test -race ./...

# Run tests without cache
go test -count=1 -race ./...
```

### Test Coverage Highlights
- **`schedule_test.go`**:
  - `TestIntervalSchedule_Validation`: Verifies rejection of 0 and negative durations.
  - `TestHourlySchedule_ClockAlignment`: Verifies clock-hour alignment across mid-hour and midnight boundaries.
  - `TestDailySchedule_Calculation`: Verifies today vs. tomorrow transitions and New Year's Eve rollover.
  - `TestMonthlySchedule_CalendarClamping`: Verifies Feb 28, Feb 29 (leap year), and April 30 clamping.
  - `TestYearlySchedule_LeapYearAndValidation`: Verifies Feb 30 rejection and Feb 29 leap/non-leap calculations.
- **`scheduler_test.go`**:
  - `TestScheduler_Registration`: Checks all validation errors and job removal.
  - `TestScheduler_IntervalExecution`: Verifies live dispatch and run counts.
  - `TestScheduler_ConcurrencyPrevention`: Confirms slow tasks do not spawn overlapping instances.
  - `TestScheduler_TaskErrorHandling`: Verifies error propagation to custom handler.
  - `TestScheduler_GracefulShutdown`: Confirms in-flight jobs finish cleanly before `Stop()` returns.
  - `TestScheduler_ShutdownContextTimeout`: Confirms cancellation when shutdown deadline is exceeded.
  - `TestScheduler_DynamicRegistrationWhileRunning`: Verifies loop waking and immediate rescheduling.

---

## 8.13 Integration into Another Go Project

To use this library in an external Go project:

```bash
# 1. In your project directory:
go get github.com/sudhanop/scheduler-lib-temp
```

```go
// 2. In your service code:
package worker

import (
	"context"
	"time"

	scheduler "github.com/sudhanop/scheduler-lib-temp"
)

type WorkerService struct {
	sched *scheduler.Scheduler
}

func NewWorkerService() *WorkerService {
	s := scheduler.New()

	s.RegisterInterval("cache-purge", 5*time.Minute, func(ctx context.Context) error {
		// Your business logic
		return nil
	})

	return &WorkerService{sched: s}
}

func (w *WorkerService) Start() error {
	return w.sched.Start()
}

func (w *WorkerService) Stop(ctx context.Context) error {
	return w.sched.Stop(ctx)
}
```

---

## 8.14 Common Errors and Troubleshooting

| Error | Common Cause | Resolution |
| :--- | :--- | :--- |
| `ErrDuplicateJob` | Two jobs registered with the same name string. | Use unique, descriptive names for each job. |
| `ErrInvalidInterval` | Passing `<= 0` to `Every(...)`. | Ensure duration is positive (e.g. `10 * time.Second`). |
| `ErrInvalidSchedule` | Passing out-of-range values (e.g., hour 25 or minute 61). | Verify 24-hour clock ranges (0-23) and minutes (0-59). |
| `ErrJobAlreadyRunning` | A previous execution is still running and `allowConcurrent` is false. | Optimize task execution time or increase interval. |
| `ErrSchedulerRunning` | Calling `Start()` twice on the same scheduler. | Call `Start()` only once during service initialization. |
| `ErrSchedulerStopped` | Calling `Stop()` when the scheduler was never started or already stopped. | Ensure lifecycle calls are paired. |

---

## 8.15 Limitations and Future Improvements

### Current Limitations
- **In-Memory Only**: Schedule states and execution histories are not persisted across process restarts.
- **Single Process**: Does not coordinate distributed locking across multiple instances of an auto-scaled microservice.
- **No Cron String Parsing**: Configured via structured Go code rather than crontab string expressions (`"* * * * *"`).

### Possible Future Improvements
- Pluggable distributed leader election / locks (e.g., Redis or database-backed).
- Standard crontab syntax parser in a separate optional subpackage.
- Prometheus metrics collector for execution duration histograms and failure counters.

---

## 8.16 Learning and Explanation Guide (Junior-to-Senior Q&A)

### Q: What problem does this project solve?
**A**: It gives any Go service an in-process, reliable mechanism to execute background tasks on recurring intervals or clock-aligned calendar schedules without pulling in heavy third-party dependencies or running external cron services.

### Q: Why does this library use Go's standard library only?
**A**: Go's standard library provides world-class primitives (`time.Timer`, `context.Context`, `sync/atomic`, `sync.WaitGroup`, channels). Writing the scheduler without external dependencies keeps the binary small, avoids supply chain vulnerabilities, guarantees long-term API stability, and ensures zero version conflicts.

### Q: How does the scheduler sleep and wake up without wasting CPU?
**A**: The scheduler calculates the earliest `NextRun` time across all registered jobs. It then sleeps using a single `time.NewTimer(duration)` inside a `select` statement. If a new job is registered while sleeping, a signal sent over a wakeup channel interrupts the sleep immediately and recalculates. This uses virtually **0% CPU** while waiting.

### Q: How does the scheduler handle slow tasks?
**A**: Tasks are dispatched in their own dedicated goroutines (`go dispatchJob(job, rootCtx)`). A task taking 10 minutes will **never** stall or delay other scheduled tasks. Furthermore, atomic compare-and-swap locks prevent duplicate runs of the same slow task unless explicitly permitted.

### Q: How does graceful shutdown work?
**A**: When `Stop(ctx)` is called, the scheduler signals its event loop to terminate, preventing any new jobs from starting. It then waits on a `sync.WaitGroup` for currently executing jobs to finish. If the caller's context expires before in-flight tasks finish, the scheduler cancels the running jobs' contexts and returns `ctx.Err()`.
