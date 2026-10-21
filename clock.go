package scheduler

import "time"

// Clock abstracts time measurement so schedule calculations and scheduler execution
// can be tested deterministically without real-time sleeps or race conditions.
type Clock interface {
	// Now returns the current time from the perspective of this clock.
	Now() time.Time
}

// realClock is the default Clock implementation using Go's standard library time package.
type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}
