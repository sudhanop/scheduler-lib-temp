package scheduler

import "errors"

var (
	// ErrEmptyJobName is returned when attempting to register a job with an empty name string.
	ErrEmptyJobName = errors.New("scheduler: job name cannot be empty")

	// ErrDuplicateJob is returned when attempting to register a job whose name is already registered.
	ErrDuplicateJob = errors.New("scheduler: job already registered with this name")

	// ErrNilTask is returned when attempting to register a nil task function.
	ErrNilTask = errors.New("scheduler: task cannot be nil")

	// ErrNilSchedule is returned when attempting to register a nil schedule.
	ErrNilSchedule = errors.New("scheduler: schedule cannot be nil")

	// ErrInvalidInterval is returned when an interval duration is less than or equal to zero.
	ErrInvalidInterval = errors.New("scheduler: interval duration must be greater than zero")

	// ErrInvalidSchedule is returned when schedule parameters are out of valid calendar bounds.
	ErrInvalidSchedule = errors.New("scheduler: invalid schedule parameters")

	// ErrSchedulerRunning is returned when attempting to start a scheduler that is already running.
	ErrSchedulerRunning = errors.New("scheduler: scheduler is already running")

	// ErrSchedulerStopped is returned when attempting to stop or interact with a stopped scheduler.
	ErrSchedulerStopped = errors.New("scheduler: scheduler is stopped")

	// ErrJobNotFound is returned when querying or removing a job that does not exist.
	ErrJobNotFound = errors.New("scheduler: job not found")

	// ErrJobAlreadyRunning is reported to the error handler when a scheduled job is skipped
	// because its previous execution is still running and concurrent runs are disabled.
	ErrJobAlreadyRunning = errors.New("scheduler: job execution skipped because previous run is still in progress")
)
