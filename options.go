package scheduler

import "time"

// JobOption configures a Job during registration.
type JobOption func(*Job)

// WithAllowConcurrent configures whether overlapping executions of the same job are permitted.
// If allow is false (default), subsequent scheduled ticks are skipped while the job is still executing.
func WithAllowConcurrent(allow bool) JobOption {
	return func(j *Job) {
		j.allowConcurrent = allow
	}
}

// WithImmediateFirstRun instructs the scheduler to trigger the job immediately upon scheduler startup
// in addition to its recurring schedule.
func WithImmediateFirstRun() JobOption {
	return func(j *Job) {
		j.immediateFirstRun = true
	}
}

// WithJobTimeout sets an optional per-job execution timeout. If the task exceeds this duration,
// its context is cancelled.
func WithJobTimeout(d time.Duration) JobOption {
	return func(j *Job) {
		j.timeout = d
	}
}

// Option configures the Scheduler.
type Option func(*Scheduler)

// ErrorHandler is invoked when a job returns an error or when execution is skipped.
type ErrorHandler func(jobName string, err error)

// Logger defines a minimal logging contract compatible with the standard library log.Logger.
type Logger interface {
	Printf(format string, v ...any)
}

// WithErrorHandler registers a custom callback for reporting task errors and scheduler warnings.
func WithErrorHandler(h ErrorHandler) Option {
	return func(s *Scheduler) {
		s.errorHandler = h
	}
}

// WithLogger sets the logger used for scheduler lifecycle and diagnostic messages.
func WithLogger(l Logger) Option {
	return func(s *Scheduler) {
		s.logger = l
	}
}

// WithClock sets a custom Clock provider, enabling deterministic time testing.
func WithClock(c Clock) Option {
	return func(s *Scheduler) {
		if c != nil {
			s.clock = c
		}
	}
}
