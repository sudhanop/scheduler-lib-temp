package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Scheduler manages job registration, timing calculations, and concurrent execution.
type Scheduler struct {
	mu       sync.RWMutex
	jobs     map[string]*Job
	jobOrder []string
	running  bool

	stopChan   chan struct{}
	wakeChan   chan struct{}
	wg         sync.WaitGroup
	rootCtx    context.Context
	rootCancel context.CancelFunc

	errorHandler ErrorHandler
	logger       Logger
	clock        Clock
}

// New creates a new Scheduler instance configured with the provided options.
func New(opts ...Option) *Scheduler {
	s := &Scheduler{
		jobs:     make(map[string]*Job),
		jobOrder: make([]string, 0),
		wakeChan: make(chan struct{}, 1),
		clock:    realClock{},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}

	return s
}

// Register adds a new job with a custom Schedule to the scheduler.
// If the scheduler is already running, the timing loop is automatically recalculated.
func (s *Scheduler) Register(name string, sched Schedule, task Task, opts ...JobOption) error {
	if name == "" {
		return ErrEmptyJobName
	}
	if sched == nil {
		return ErrNilSchedule
	}
	if task == nil {
		return ErrNilTask
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[name]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateJob, name)
	}

	job := &Job{
		name:     name,
		schedule: sched,
		task:     task,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(job)
		}
	}

	now := s.clock.Now()
	if job.immediateFirstRun {
		job.nextRun = now
	} else {
		job.nextRun = sched.Next(now)
	}

	s.jobs[name] = job
	s.jobOrder = append(s.jobOrder, name)

	s.notifyWakeLocked()
	return nil
}

// RegisterInterval registers a job that executes repeatedly at fixed intervals.
func (s *Scheduler) RegisterInterval(name string, interval time.Duration, task Task, opts ...JobOption) error {
	sched, err := Every(interval)
	if err != nil {
		return err
	}
	return s.Register(name, sched, task, opts...)
}

// RegisterHourly registers a job that executes at a specific minute and second of every clock hour.
// For the beginning of every clock hour, pass minute=0, second=0.
func (s *Scheduler) RegisterHourly(name string, minute, second int, task Task, opts ...JobOption) error {
	sched, err := HourlyAt(minute, second)
	if err != nil {
		return err
	}
	return s.Register(name, sched, task, opts...)
}

// RegisterDaily registers a job that executes daily at the specified hour, minute, and second.
func (s *Scheduler) RegisterDaily(name string, hour, minute, second int, task Task, opts ...JobOption) error {
	sched, err := Daily(hour, minute, second)
	if err != nil {
		return err
	}
	return s.Register(name, sched, task, opts...)
}

// RegisterMonthly registers a job that executes monthly on the specified day and time.
// If a month has fewer days than 'day', execution is automatically clamped to the final valid day of that month.
func (s *Scheduler) RegisterMonthly(name string, day, hour, minute, second int, task Task, opts ...JobOption) error {
	sched, err := Monthly(day, hour, minute, second)
	if err != nil {
		return err
	}
	return s.Register(name, sched, task, opts...)
}

// RegisterYearly registers a job that executes once a year on the specified month, day, and time.
// For February 29 on non-leap years, execution is automatically clamped to February 28.
func (s *Scheduler) RegisterYearly(name string, month time.Month, day, hour, minute, second int, task Task, opts ...JobOption) error {
	sched, err := Yearly(month, day, hour, minute, second)
	if err != nil {
		return err
	}
	return s.Register(name, sched, task, opts...)
}

// Start begins the scheduling loop in a background goroutine.
// Returns ErrSchedulerRunning if the scheduler is already active.
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return ErrSchedulerRunning
	}

	s.running = true
	s.stopChan = make(chan struct{})
	s.rootCtx, s.rootCancel = context.WithCancel(context.Background())

	go s.loop(s.rootCtx)
	return nil
}

// Stop gracefully shuts down the scheduler. It stops scheduling new executions and waits
// for all currently executing tasks to complete or until the provided context is cancelled.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return ErrSchedulerStopped
	}

	s.running = false
	close(s.stopChan)
	s.mu.Unlock()

	tasksDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(tasksDone)
	}()

	select {
	case <-tasksDone:
		if s.rootCancel != nil {
			s.rootCancel()
		}
		return nil

	case <-ctx.Done():
		if s.rootCancel != nil {
			s.rootCancel()
		}
		return ctx.Err()
	}
}

// Shutdown is an alias for Stop to conform to standard Go service naming conventions.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	return s.Stop(ctx)
}

// Remove unregisters a job by name.
func (s *Scheduler) Remove(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[name]; !exists {
		return ErrJobNotFound
	}

	delete(s.jobs, name)
	for i, n := range s.jobOrder {
		if n == name {
			s.jobOrder = append(s.jobOrder[:i], s.jobOrder[i+1:]...)
			break
		}
	}

	s.notifyWakeLocked()
	return nil
}

// Job retrieves status information for a specific job.
func (s *Scheduler) Job(name string) (JobInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.jobs[name]
	if !exists {
		return JobInfo{}, ErrJobNotFound
	}

	return job.Info(), nil
}

// Jobs returns a list of snapshots of all currently registered jobs in registration order.
func (s *Scheduler) Jobs() []JobInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list := make([]JobInfo, 0, len(s.jobOrder))
	for _, name := range s.jobOrder {
		if job, ok := s.jobs[name]; ok {
			list = append(list, job.Info())
		}
	}
	return list
}

// IsRunning reports whether the scheduler loop is currently active.
func (s *Scheduler) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// Count returns the number of currently registered jobs.
func (s *Scheduler) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.jobs)
}

// loop runs the event-driven scheduling loop.
func (s *Scheduler) loop(tasksCtx context.Context) {
	for {
		s.mu.RLock()
		if !s.running {
			s.mu.RUnlock()
			return
		}

		now := s.clock.Now()
		earliestNext, hasJobs := s.findEarliestDueTimeLocked()
		s.mu.RUnlock()

		var timer *time.Timer
		var timerCh <-chan time.Time

		if hasJobs {
			dur := earliestNext.Sub(now)
			if dur <= 0 {
				s.dispatchDueJobs(now, tasksCtx)
				continue
			}
			timer = time.NewTimer(dur)
			timerCh = timer.C
		}

		select {
		case <-s.stopChan:
			if timer != nil {
				timer.Stop()
			}
			return

		case <-s.wakeChan:
			if timer != nil {
				timer.Stop()
			}
			continue

		case triggerTime := <-timerCh:
			s.dispatchDueJobs(triggerTime, tasksCtx)
		}
	}
}

// findEarliestDueTimeLocked identifies the earliest nextRun timestamp among all registered jobs.
func (s *Scheduler) findEarliestDueTimeLocked() (time.Time, bool) {
	var earliest time.Time
	found := false

	for _, name := range s.jobOrder {
		job := s.jobs[name]
		if job == nil {
			continue
		}

		job.mu.RLock()
		next := job.nextRun
		job.mu.RUnlock()

		if next.IsZero() {
			continue
		}

		if !found || next.Before(earliest) {
			earliest = next
			found = true
		}
	}

	return earliest, found
}

// dispatchDueJobs checks each job and launches tasks whose nextRun is due.
func (s *Scheduler) dispatchDueJobs(now time.Time, rootCtx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}

	for _, name := range s.jobOrder {
		job := s.jobs[name]
		if job == nil {
			continue
		}

		job.mu.Lock()
		if job.nextRun.IsZero() || job.nextRun.After(now) {
			job.mu.Unlock()
			continue
		}

		// Update execution metadata
		job.lastRun = now

		// Calculate the next execution time.
		// Use next occurrence after current nextRun to prevent drift under normal conditions.
		// If lagging significantly behind current time, advance from 'now' to prevent burst catch-ups.
		candidateNext := job.schedule.Next(job.nextRun)
		if candidateNext.After(now) {
			job.nextRun = candidateNext
		} else {
			job.nextRun = job.schedule.Next(now)
		}

		job.mu.Unlock()

		// Concurrency protection: skip execution if overlapping runs are disabled
		if !job.allowConcurrent {
			if !job.activeRuns.CompareAndSwap(0, 1) {
				if s.errorHandler != nil {
					go s.errorHandler(job.name, ErrJobAlreadyRunning)
				}
				continue
			}
		} else {
			job.activeRuns.Add(1)
		}

		s.dispatchJob(job, rootCtx)
	}
}

// dispatchJob runs the job task in a dedicated goroutine.
func (s *Scheduler) dispatchJob(job *Job, rootCtx context.Context) {
	s.wg.Add(1)

	go func() {
		defer s.wg.Done()
		defer job.activeRuns.Add(-1)

		var ctx context.Context
		var cancel context.CancelFunc

		if job.timeout > 0 {
			ctx, cancel = context.WithTimeout(rootCtx, job.timeout)
		} else {
			ctx, cancel = context.WithCancel(rootCtx)
		}
		defer cancel()

		err := job.task(ctx)

		job.mu.Lock()
		job.runCount++
		job.lastError = err
		job.mu.Unlock()

		if err != nil && s.errorHandler != nil {
			s.errorHandler(job.name, err)
		}
	}()
}

// notifyWakeLocked signals the event loop that the job queue has changed.
func (s *Scheduler) notifyWakeLocked() {
	if s.running {
		select {
		case s.wakeChan <- struct{}{}:
		default:
		}
	}
}
