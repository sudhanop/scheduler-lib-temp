package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Task is the function executed by the scheduler for a registered job.
// It receives a context for cancellation awareness and returns an error if execution fails.
type Task func(ctx context.Context) error

// Job represents a registered task with its scheduling definition and runtime state.
type Job struct {
	mu sync.RWMutex

	name              string
	schedule          Schedule
	task              Task
	nextRun           time.Time
	lastRun           time.Time
	lastError         error
	runCount          uint64
	allowConcurrent   bool
	immediateFirstRun bool
	timeout           time.Duration
	activeRuns        atomic.Int32
}

// JobInfo provides an immutable snapshot of a Job's current status and metadata.
type JobInfo struct {
	Name              string
	ScheduleDesc      string
	NextRun           time.Time
	LastRun           time.Time
	LastError         error
	RunCount          uint64
	IsRunning         bool
	ActiveRuns        int32
	AllowConcurrent   bool
	ImmediateFirstRun bool
	Timeout           time.Duration
}

// Name returns the job's unique identifier.
func (j *Job) Name() string {
	return j.name
}

// Schedule returns the schedule associated with the job.
func (j *Job) Schedule() Schedule {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.schedule
}

// NextRun returns the job's next planned execution time.
func (j *Job) NextRun() time.Time {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.nextRun
}

// LastRun returns the timestamp when the job was last dispatched.
func (j *Job) LastRun() time.Time {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.lastRun
}

// LastError returns the error produced during the job's most recent execution, or nil.
func (j *Job) LastError() error {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.lastError
}

// RunCount returns the total number of times the job has completed execution.
func (j *Job) RunCount() uint64 {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.runCount
}

// IsRunning reports whether one or more instances of the job are currently executing.
func (j *Job) IsRunning() bool {
	return j.activeRuns.Load() > 0
}

// Info returns a thread-safe snapshot of the job's status.
func (j *Job) Info() JobInfo {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var desc string
	if j.schedule != nil {
		desc = j.schedule.Description()
	}

	return JobInfo{
		Name:              j.name,
		ScheduleDesc:      desc,
		NextRun:           j.nextRun,
		LastRun:           j.lastRun,
		LastError:         j.lastError,
		RunCount:          j.runCount,
		IsRunning:         j.activeRuns.Load() > 0,
		ActiveRuns:        j.activeRuns.Load(),
		AllowConcurrent:   j.allowConcurrent,
		ImmediateFirstRun: j.immediateFirstRun,
		Timeout:           j.timeout,
	}
}
