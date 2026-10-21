# Project Handover & Technical Discussion Guide

**Package**: `github.com/sudhanop/scheduler-lib-temp`  
**Purpose**: Production-grade, zero-dependency, in-memory recurring task scheduler for Go applications.  
**Audience**: Junior / Intermediate Go Engineer preparing for technical discussions with a Senior Backend Engineer / Architect.

---

## Table of Contents

1. [A. What Was Built](#a-what-was-built)
2. [B. Architecture Walkthrough](#b-architecture-walkthrough)
3. [C. File-by-File Explanation](#c-file-by-file-explanation)
4. [D. Execution Walkthrough](#d-execution-walkthrough)
5. [E. Important Go Concepts Used](#e-important-go-concepts-used)
6. [F. Demonstration Guide](#f-demonstration-guide)
7. [G. Explain It to My Senior Developer](#g-explain-it-to-my-senior-developer)
8. [H. Questions My Senior Might Ask & Answers](#h-questions-my-senior-might-ask--answers)
9. [I. What I Should Learn Next](#i-what-i-should-learn-next)

---

## A. What Was Built

We built a reusable Go scheduling package (`scheduler`) that allows any Go application to register background functions and execute them automatically on recurring schedules.

### Key Capabilities Delivered:
1. **Interval-Based Scheduling**:
   - Supports arbitrary duration intervals (e.g., 30s, 1m, 10m).
   - Configurable first-run behavior: defaults to waiting for the first interval, with opt-in immediate execution via `WithImmediateFirstRun()`.
   - Drift-free timing calculation under steady-state execution.
2. **Clock-Aligned Hourly Scheduling**:
   - Executes at the beginning of each clock hour (e.g., 09:00:00, 10:00:00, 11:00:00) rather than rolling 60 minutes from process boot.
3. **Daily Scheduling**:
   - Executes once per day at an exact local clock time (e.g., 09:00:00 AM).
4. **Monthly Scheduling with Calendar Clamping**:
   - Executes on a specified day of each month at a specific time.
   - **Clamping policy**: If a month has fewer days than requested (e.g., February 31st), it clamps execution to the final valid day of that month (Feb 28/29, April 30), ensuring no month is skipped.
5. **Yearly Scheduling with Leap-Year Handling**:
   - Executes on a specific month and day each year.
   - Feb 29 schedules clamp to Feb 28 on non-leap years so execution is never skipped for 3 consecutive years.
6. **Simultaneous Multi-Job Execution**:
   - Predictable, non-blocking dispatch: When multiple jobs are due at the exact same instant, each runs in its own goroutine concurrently.
7. **Concurrency Safety & Overlap Prevention**:
   - Distinct jobs always run in parallel.
   - For any single job, overlapping executions are disabled by default using an atomic compare-and-swap (`atomic.Int32`), preventing server exhaustion when a job runs slow.
8. **Context Cancellation & Graceful Shutdown**:
   - Propagates cancellation contexts to running tasks.
   - Shuts down cleanly via `Stop(ctx)` or `Shutdown(ctx)`, waiting on `sync.WaitGroup` for in-flight tasks.
9. **Zero Dependencies**:
   - Implemented strictly with the Go standard library (`time`, `sync`, `sync/atomic`, `context`, `errors`, `fmt`).

---

## B. Architecture Walkthrough

### How a Task Travels from Registration to Execution

```
[1. Application Setup]
       |
       v
s := scheduler.New(WithErrorHandler(...))
s.Register("daily-sync", Daily(9, 0, 0), myTask)
       |
       +--> Stored in s.jobs["daily-sync"] and s.jobOrder
       +--> Computes initial nextRun via schedule.Next(now)
       +--> If scheduler is already running, sends wake signal to s.wakeChan
       |
[2. Scheduler Coordinator Loop (s.loop)]
       |
       v
Evaluates all registered jobs -> identifies earliest nextRun
       |
Computes duration until earliest: dur = earliest.Sub(now)
       |
Arm event-driven timer: timer = time.NewTimer(dur)
       |
Sleeps in select statement on 3 possible events:
  ├── Case 1: <-s.stopChan -> Exit loop, clean up timer
  ├── Case 2: <-s.wakeChan -> Cancel timer, re-evaluate nextRun (new job arrived)
  └── Case 3: <-timer.C   -> Timer fired! Call dispatchDueJobs(triggerTime)
       |
[3. Dispatching (s.dispatchDueJobs)]
       |
Locks s.mu -> scans s.jobOrder for jobs where job.nextRun <= triggerTime
       |
For each due job:
  ├── Advances nextRun to future occurrence (prevents duplicate triggers)
  ├── Tests concurrency lock: activeRuns.CompareAndSwap(0, 1)
  │     ├── If FALSE (job already running): skips tick, reports ErrJobAlreadyRunning
  │     └── If TRUE: acquires lock, proceeds to dispatch
  └── Spawns worker goroutine: go dispatchJob(job, rootCtx)
       |
[4. Worker Execution (s.dispatchJob)]
       |
s.wg.Add(1)
       |
Creates job context with optional timeout: WithTimeout(rootCtx, timeout)
Executes task: err := job.task(jobCtx)
Records runCount and lastError under job.mu
Calls s.errorHandler(job.Name, err) if error occurred
Releases concurrency lock: defer activeRuns.Add(-1)
Notifies completion: defer s.wg.Done()
```

---

## C. File-by-File Explanation

| File | Purpose | Why It Exists |
| :--- | :--- | :--- |
| `go.mod` | Declares the module `github.com/sudhanop/scheduler-lib-temp` and Go toolchain version. | Allows any external Go project to import the library cleanly via standard `go get`. |
| `errors.go` | Defines all public sentinel errors (`ErrEmptyJobName`, `ErrDuplicateJob`, `ErrInvalidInterval`, etc.). | Enables callers to use idiomatic `errors.Is(err, ...)` checks to inspect and handle error conditions. |
| `clock.go` | Defines the `Clock` interface (`Now() time.Time`) and `realClock`. | Allows the library to run in real time while providing a clean seam for deterministic, sleep-free unit testing. |
| `schedule.go` | Defines the `Schedule` interface (`Next` and `Description`) and concrete implementations: `intervalSchedule`, `hourlySchedule`, `dailySchedule`, `monthlySchedule`, `yearlySchedule`. | Encapsulates all calendar rules, time zones, leap years, and month clamping away from the scheduler engine. |
| `job.go` | Defines the `Job` struct, `Task` function type, atomic concurrency trackers, and the immutable `JobInfo` snapshot. | Isolates task-level metadata, statistics, and lock state. Protects callers from race conditions through `JobInfo`. |
| `options.go` | Provides functional options for configuring `Scheduler` (`WithErrorHandler`, `WithLogger`, `WithClock`) and `Job` (`WithImmediateFirstRun`, `WithAllowConcurrent`, `WithJobTimeout`). | Idiomatic Go pattern providing safe defaults while allowing flexible customization without breaking constructor signatures. |
| `scheduler.go` | Implements the `Scheduler` struct, registration methods, `Start()`, `Stop()`, event loop (`loop`), and goroutine dispatchers. | The central coordinator that controls scheduling, timer sleeping, concurrency enforcement, and graceful shutdown. |
| `schedule_test.go` | Unit tests for calendar edge cases: leap years, month-end clamping (Feb 28, Feb 29, Apr 30), and hourly clock alignment. | Validates pure calendar calculation logic quickly and deterministically without waiting on timers. |
| `scheduler_test.go` | Integration tests for registration validation, interval execution, multiple simultaneous jobs, overlap prevention, task errors, and shutdown timeout. | Verifies runtime concurrency, race freedom, and lifecycle behaviors under actual multithreaded conditions. |
| `examples/basic/main.go` | Complete demonstration app registering all 7 required schedules with terminal logging, signal trapping (`SIGINT`/`SIGTERM`), and graceful exit. | Demonstrates how a real-world application consumes the library, starts it, handles OS signals, and shuts down cleanly. |
| `README.md` | Comprehensive user manual, architecture guide, API reference, and troubleshooting guide. | Serves as the developer documentation for onboarding new team members and consumers of the package. |
| `HANDOVER.md` | This document. Technical walkthrough and deep-dive question-and-answer preparation for senior review. | Prepares you to explain every design decision, code path, and tradeoff confidently in an engineering review. |

---

## D. Execution Walkthrough

Let us trace the lifecycle of the demonstration application step by step:

### Step 1: Bootstrapping & Registration
1. `main()` calls `scheduler.New(scheduler.WithErrorHandler(...))`.
2. Seven jobs are registered via `sched.Register(...)`:
   - `heartbeat-30s` (Interval 30s)
   - `metrics-1m` (Interval 1m)
   - `market-scan-10m` (Interval 10m)
   - `clock-hourly` (Hourly at minute 0, second 0)
   - `daily-digest-9am` (Daily at 09:00:00)
   - `monthly-report-1st` (Monthly on day 1 at 00:00:00)
   - `yearly-newyear-jan1` (Yearly on January 1 at 00:00:00)
3. For each registration:
   - Validates non-empty name, non-nil schedule, and non-nil task.
   - Computes `job.nextRun = sched.Next(now)`.
   - Stores the job in `s.jobs` and records the name in `s.jobOrder`.

### Step 2: Starting the Scheduler
1. `sched.Start()` acquires `s.mu`.
2. Checks that `s.running` is false, sets `s.running = true`.
3. Creates a root cancellation context: `s.rootCtx, s.rootCancel = context.WithCancel(context.Background())`.
4. Spawns `go s.loop(s.rootCtx)`.
5. Prints the job roster table showing exact timestamps for all 7 upcoming runs.

### Step 3: Waiting for Scheduled Time (Zero-CPU Sleeping)
1. Inside `s.loop`, `s.findEarliestDueTimeLocked()` scans all jobs.
2. The earliest job is `heartbeat-30s`, due in 30 seconds.
3. The scheduler creates a timer: `timer = time.NewTimer(30 * time.Second)`.
4. The goroutine enters `select`:
   - It is not busy-looping.
   - It is parked by the Go runtime scheduler, consuming **zero CPU cycles**.

### Step 4: Timer Fires & Dispatch
1. When 30 seconds elapse, `timer.C` delivers a timestamp.
2. `s.dispatchDueJobs(now, rootCtx)` executes:
   - Finds `heartbeat-30s` is due (`nextRun <= now`).
   - Advances its next run time: `job.nextRun = job.schedule.Next(job.nextRun)` (now 60s from start).
   - Atomically attempts to acquire the job lock: `job.activeRuns.CompareAndSwap(0, 1)`.
   - Increments the scheduler's `sync.WaitGroup`: `s.wg.Add(1)`.
   - Launches a worker goroutine: `go s.dispatchJob(job, rootCtx)`.
3. The event loop immediately proceeds to recalculate the next earliest job (the next 30s heartbeat or 1m metrics) without waiting for the task to finish!

### Step 5: Handling Errors
1. If a task returns an error, the worker goroutine captures it.
2. It locks `job.mu` to update `job.runCount++` and `job.lastError = err`.
3. It calls the user's `ErrorHandler` callback in a separate goroutine so slow error handling never stalls the worker.
4. The scheduler continues running unaffected.

### Step 6: Graceful Shutdown
1. The user presses `Ctrl+C` (or the container orchestrator sends `SIGTERM`).
2. The signal channel triggers in `main.go`.
3. `main.go` creates a shutdown context with a 5-second deadline: `context.WithTimeout(context.Background(), 5*time.Second)`.
4. Calls `s.Stop(shutdownCtx)`:
   - Sets `s.running = false`.
   - Closes `s.stopChan`, terminating the background event loop immediately.
   - Waits on `s.wg.Wait()` in a goroutine.
   - If all running tasks complete within 5 seconds, it returns `nil`.
   - If a stuck task exceeds 5 seconds, `s.rootCancel()` cancels the task contexts and `Stop` returns `context.DeadlineExceeded`.

---

## E. Important Go Concepts Used

| Concept | Where It Is Used | Why It Was Chosen |
| :--- | :--- | :--- |
| **Interfaces** (`Schedule`, `Clock`, `Logger`) | `schedule.go`, `clock.go`, `options.go` | Decouples schedule calculation from the execution loop. Allows adding new schedule types or mocking time without touching scheduler internals. |
| **Goroutines** (`go ...`) | `s.loop()`, `s.dispatchJob()`, error reporting | Ensures non-blocking execution. A slow or hung task never blocks the scheduling loop or prevents other jobs from triggering. |
| **Channels** (`stopChan`, `wakeChan`, `timer.C`) | `scheduler.go` event loop | Idiomatic Go communication. Coordinates event-driven timer waiting, cancellation signals, and dynamic job wakeups without polling. |
| **`time.Timer` vs `time.Ticker`** | `s.loop()` | A ticker fires at fixed intervals, which cannot handle variable schedules (e.g. 30s, 1m, 1h). `time.NewTimer(earliest.Sub(now))` allows dynamic, variable-length sleeping for whichever job is next. |
| **`sync.RWMutex`** | `Scheduler.mu`, `Job.mu` | Protects registry maps and metadata. Allows high-concurrency read operations (`JobInfo`, `Jobs()`) while guaranteeing safe single-writer mutations. |
| **`sync/atomic` (`atomic.Int32`)** | `Job.activeRuns` | High-performance, lock-free concurrency gate (`CompareAndSwap(0, 1)`). Guarantees that only one instance of a non-concurrent job runs at a time without mutex overhead. |
| **`sync.WaitGroup`** | `s.wg` in `Scheduler` | Tracks the exact count of active worker goroutines. Enables graceful shutdown by blocking until all in-flight jobs finish. |
| **`context.Context`** | Task signatures, `Stop(ctx)`, per-job timeouts | Standard Go cancellation mechanism. Allows long-running tasks to abort gracefully when the application terminates or when a job timeout expires. |
| **Functional Options** (`Option`, `JobOption`) | `options.go`, `New()`, `Register()` | Provides extensible configuration (timeouts, error handlers, concurrency settings) without cluttering constructors with dozens of arguments. |
| **Sentinel Errors** (`errors.New`, `errors.Is`) | `errors.go`, tests | Idiomatic error handling. Callers can test for specific error conditions (`ErrDuplicateJob`, `ErrInvalidInterval`) using `errors.Is`. |

---

## F. Demonstration Guide

### Running the Tests
To execute all unit tests and verify race-condition freedom:

```bash
# Standard test run
go test -v ./...

# Race detector verification (mandatory for multithreaded Go)
go test -count=1 -race ./...
```

**Expected Test Output**:
```text
=== RUN   TestIntervalSchedule_Validation
--- PASS: TestIntervalSchedule_Validation (0.00s)
=== RUN   TestIntervalSchedule_Calculation
--- PASS: TestIntervalSchedule_Calculation (0.00s)
=== RUN   TestHourlySchedule_Validation
--- PASS: TestHourlySchedule_Validation (0.00s)
=== RUN   TestHourlySchedule_ClockAlignment
--- PASS: TestHourlySchedule_ClockAlignment (0.00s)
=== RUN   TestDailySchedule_Calculation
--- PASS: TestDailySchedule_Calculation (0.00s)
=== RUN   TestMonthlySchedule_CalendarClamping
--- PASS: TestMonthlySchedule_CalendarClamping (0.00s)
=== RUN   TestYearlySchedule_LeapYearAndValidation
--- PASS: TestYearlySchedule_LeapYearAndValidation (0.00s)
=== RUN   TestScheduler_Registration
--- PASS: TestScheduler_Registration (0.00s)
=== RUN   TestScheduler_IntervalExecution
--- PASS: TestScheduler_IntervalExecution (0.07s)
=== RUN   TestScheduler_ImmediateFirstRun
--- PASS: TestScheduler_ImmediateFirstRun (0.00s)
=== RUN   TestScheduler_MultipleSimultaneousJobs
--- PASS: TestScheduler_MultipleSimultaneousJobs (0.00s)
=== RUN   TestScheduler_TaskErrorHandling
--- PASS: TestScheduler_TaskErrorHandling (0.05s)
=== RUN   TestScheduler_ConcurrencyPrevention
--- PASS: TestScheduler_ConcurrencyPrevention (0.12s)
=== RUN   TestScheduler_GracefulShutdown
--- PASS: TestScheduler_GracefulShutdown (0.05s)
=== RUN   TestScheduler_ShutdownContextTimeout
--- PASS: TestScheduler_ShutdownContextTimeout (0.04s)
=== RUN   TestScheduler_LifecycleErrors
--- PASS: TestScheduler_LifecycleErrors (0.00s)
=== RUN   TestScheduler_DynamicRegistrationWhileRunning
--- PASS: TestScheduler_DynamicRegistrationWhileRunning (0.00s)
PASS
ok  	github.com/sudhanop/scheduler-lib-temp	1.369s
```

### Running the Example Application
To run the demonstration registering all 7 schedules:

```bash
# Standard mode: shows future schedule roster
go run ./examples/basic

# Immediate mode: executes all 7 jobs once immediately upon startup
go run ./examples/basic -immediate
```

Press `Ctrl+C` to watch graceful shutdown complete.

---

## G. Explain It to My Senior Developer

Here is a 2-minute elevator pitch you can deliver to your senior developer:

> *"We built `rlib-scheduler`, a lightweight, concurrency-safe in-memory scheduler using Go's standard library only. 
> 
> The core problem it solves is eliminating scattered, bespoke `time.Sleep` or `time.Ticker` loops across our services. The library enforces a clean separation of concerns: the scheduler controls **when** a task runs, while the application controls **what** the task does.
> 
> Architecturally, it uses an event-driven timer queue. The coordinator loop identifies the earliest due job across all registrations and sleeps using a single `time.Timer`. It consumes zero CPU while idle, and if a new job is registered dynamically, an internal wakeup channel signals the loop to re-arm the timer immediately.
> 
> For execution, every due job is dispatched in a dedicated goroutine, so slow tasks never delay other schedules. Overlapping runs of the same job are prevented by default using atomic CAS gates.
> 
> It handles full calendar math, including clock-hour alignment, daily local times, monthly clamping (e.g. Feb 31 clamps to Feb 28/29), and yearly leap-year rules.
> 
> Finally, shutdown is completely graceful: `Stop(ctx)` stops new dispatches and waits on a `sync.WaitGroup` for in-flight tasks to finish, with context cancellation for runaway tasks. The entire package is covered by automated race-detected unit tests."*

---

## H. Questions My Senior Might Ask & Answers

### Q1: Why did you build an in-memory scheduler instead of using Linux cron or systemd timers?
**Answer**:
External schedulers like Linux `cron` or `systemd` require deploying and managing external infrastructure, separate scripts, and outside processes that invoke short-lived CLI commands. That breaks container isolation (e.g. in Kubernetes where a pod runs a single long-lived Go binary), makes local development difficult, and creates an external dependency. `rlib-scheduler` runs inside the application process, has full access to in-memory state (caches, connection pools, channels), and requires no operational setup.

### Q2: How does your scheduler loop sleep without burning CPU or drifting?
**Answer**:
We avoid busy-waiting or checking `if time.Now().Second() == 0`. Instead, the scheduler inspects the `nextRun` timestamp of all registered jobs, finds the minimum timestamp, and sets a single `time.NewTimer(dur)` for the difference between that timestamp and `time.Now()`. The goroutine yields to the Go runtime until that exact nanosecond. When the timer fires, we advance `nextRun` based on the previous scheduled time (`job.schedule.Next(job.nextRun)`), eliminating drift over recurring intervals.

### Q3: What happens if a job takes 90 seconds, but its interval is 30 seconds?
**Answer**:
By default, overlapping runs of the *same* job are disabled. Each job maintains an atomic active run counter (`atomic.Int32`). When a tick arrives, the scheduler attempts an atomic compare-and-swap: `activeRuns.CompareAndSwap(0, 1)`. If the previous execution is still running, the CAS fails, the tick is safely skipped, and `ErrJobAlreadyRunning` is passed to the configured `ErrorHandler`. Distinct jobs are unaffected because each job executes in its own goroutine. If overlapping runs are specifically desired, the user can opt in via `WithAllowConcurrent(true)`.

### Q4: How does another microservice import and use this package?
**Answer**:
The package is structured as a standard Go module at `github.com/sudhanop/scheduler-lib-temp`. Consuming projects simply run `go get github.com/sudhanop/scheduler-lib-temp`. They import `"github.com/sudhanop/scheduler-lib-temp"` and construct an instance with `scheduler.New()`. There is no need to copy code or fork the repo.

### Q5: What happens during application shutdown? Does a running job get cut off mid-database transaction?
**Answer**:
No. When `Stop(ctx)` is called, the scheduler first closes `stopChan` to prevent any new jobs from starting. It then waits on a `sync.WaitGroup` that tracks all running worker goroutines. If all workers finish before `ctx` expires, `Stop` returns `nil`. If a task is hung and the caller's shutdown context deadline is reached, the scheduler triggers `rootCancel()`, causing `ctx.Done()` to fire inside the running tasks, and returns `ctx.Err()`.

### Q6: What happens if the process crashes, server restarts, or machine sleeps?
**Answer**:
Because this is an in-memory scheduler, it does not persist run history to disk. Upon restart, it evaluates future schedules from the current time. If the system was suspended (e.g. laptop closed) and wakes up past a scheduled time, the scheduler executes the due job once immediately and schedules future runs from the current time. It deliberately does not perform a burst "catch-up storm" of 500 skipped executions. If persistent, exactly-once distributed scheduling is required, we would layer an external datastore (like Postgres or Redis) on top of this engine.

---

## I. What I Should Learn Next

To deepen your understanding and maintain this codebase independently, study these topics in order:

1. **`time.Date` normalization in Go**:
   - Inspect how Go's standard library normalizes out-of-range dates (e.g. `time.Date(2026, 3, 0, ...)` resolving to the last day of February). Understand how `schedule.go` leverages this for leap years and month clamping.
2. **Atomic Operations (`sync/atomic`)**:
   - Study `atomic.Int32` and `CompareAndSwap`. Understand why lock-free compare-and-swap is preferred over heavy mutex locking for high-frequency concurrency gating.
3. **Context Trees and Lifecycles (`context.Context`)**:
   - Trace how `context.WithCancel` and `context.WithTimeout` form parent-child trees in `scheduler.go` and how cancelling `rootCancel` cascades to all active workers.
4. **Go Race Detector (`go test -race`)**:
   - Learn how the Go race detector monitors memory accesses at runtime and why every multithreaded Go project must pass `-race` in CI/CD pipelines.
